internal import Automerge
internal import Combine
internal import Foundation

// inspired from automerge-repo:
// https://github.com/automerge/automerge-repo/blob/main/packages/automerge-repo/src/storage/StorageSubsystem.ts

/// A type that provides coordinated, concurrency safe access to persist Automerge documents.
@AutomergeRepo
final class DocumentStorage {
    let chunkNamespace = "incrChanges"
    var compacting: Bool
    /// Bumped at the start of a compaction. A save that began before it and finished after it
    /// compares this to the value it captured and, when they differ, leaves the heads memo to the
    /// compaction: its chunk is redundant but stays, so that a compaction whose snapshot write then
    /// fails has not cost the changes their only copy on disk. Upstream's `#chunkGenerations`.
    var chunkGeneration: [DocumentId: Int] = [:]
    /// Bumped at the start of a purge. A save that finds this moved takes its own write back out,
    /// since after a purge the write is an orphan, and leaves the cleared memos alone.
    var purgeGeneration: [DocumentId: Int] = [:]
    let _storage: any StorageProvider
    var latestHeads: [DocumentId: Set<ChangeHash>]

    var storedChunkSize: [DocumentId: Int]
    var memoryChunkSize: [DocumentId: Int]
    var storedDocSize: [DocumentId: Int]

    var chunks: [DocumentId: [Data]]

    var loglevel: LogVerbosity
    var documentLogLevel: LogVerbosity

    /// Creates a new concurrency safe document storage instance to manage changes to Automerge documents.
    /// - Parameter storage: The storage provider
    public nonisolated init(_ storage: some StorageProvider, verbosity: LogVerbosity, documentLogVerbosity: LogVerbosity) {
        compacting = false
        _storage = storage
        latestHeads = [:]
        chunks = [:]
        // memo-ized sizes of documents and chunks so that we don't always have to
        // iterate through the storage provider (disk accesses, or even network accesses)
        // to get a size determination to know if we should compact or not.
        // (used in function`shouldCompact(:DocumentId)`)
        storedChunkSize = [:]
        memoryChunkSize = [:]
        storedDocSize = [:]
        self.loglevel = verbosity
        self.documentLogLevel = documentLogVerbosity
    }

    func setLogVerbosity(_ level: LogVerbosity) {
        self.loglevel = level
    }

    public var id: STORAGE_ID {
        _storage.id
    }

    /// Removes a document from persistent storage: its base snapshot and every incremental chunk.
    /// - Parameter id: The id of the document to remove.
    ///
    /// Forgets what this instance remembered about the document as well, so a later load or save
    /// starts from nothing rather than from heads and chunks that no longer exist.
    public func purgeDoc(id: DocumentId) async throws {
        // Before the first suspension, so any save already in flight sees the change when it resumes.
        purgeGeneration[id, default: 0] += 1
        // Load the chunk list first: a provider's `remove` only covers the base snapshot, and a
        // document saved since its last compaction may exist only as chunks.
        let storageChunks = try await _storage.loadRange(id: id, prefix: chunkNamespace)
        try await _storage.remove(id: id)
        if !storageChunks.isEmpty {
            try await _storage.removeRange(id: id, prefix: chunkNamespace, data: storageChunks)
        }
        chunks.removeValue(forKey: id)
        latestHeads.removeValue(forKey: id)
        storedChunkSize.removeValue(forKey: id)
        memoryChunkSize.removeValue(forKey: id)
        storedDocSize.removeValue(forKey: id)
    }

    /// Returns an existing document, or nil, for the document Id you provide.
    ///
    /// The method throws errors from the underlying storage system or Document errors if the
    /// loaded data was corrupt or incorrect.
    ///
    /// - Parameter id: The document Id
    /// - Returns: An automerge document or nil if the storage provider doesn't have a record of the document Id.
    public func loadDoc(id: DocumentId) async throws -> Document? {
        var combined: Data
        let storageChunks = try await _storage.loadRange(id: id, prefix: chunkNamespace)
        if chunks[id] == nil {
            chunks[id] = []
        }
        let inMemChunks: [Data] = chunks[id] ?? []

        if let baseData = try await _storage.load(id: id) {
            // loading all the changes from the base document and any incremental saves available
            combined = baseData
            storedDocSize[id] = baseData.count
        } else {
            // no full document is available to be loaded by id, so if there were no storage chunks available
            // either, return nil to indicate we don't know about this document
            if storageChunks.isEmpty {
                return nil
            }
            // loading only incremental saves available, the base document doesn't exist in storage
            combined = Data()
            storedDocSize[id] = 0
        }

        var inMemSize = memoryChunkSize[id] ?? 0
        for chunk in inMemChunks {
            inMemSize += chunk.count
            combined.append(chunk)
        }
        memoryChunkSize[id] = inMemSize

        var storedChunks = storedChunkSize[id] ?? 0
        for chunk in storageChunks {
            storedChunks += chunk.count
            combined.append(chunk)
        }
        storedChunkSize[id] = storedChunks
        let combinedDoc = try Document(combined, logLevel: documentLogLevel)
        // `latestHeads` records what is known to be on disk. A load that races a save in flight can
        // see fewer chunks than the save has recorded, so never move the heads backwards here; the
        // save paths own that value once it exists. An extra chunk is harmless, a skipped save is not.
        if latestHeads[id] == nil {
            latestHeads[id] = combinedDoc.heads()
        }

        return combinedDoc
    }

    /// Determine if a documentId should be compacted.
    /// - Parameter key: the document Id to analyze
    /// - Returns: a Boolean value that indicates whether the document should be compacted.
    func shouldCompact(_ key: DocumentId) async throws -> Bool {
        if compacting {
            return false
        }
        let inMemSize = memoryChunkSize[key] ?? (chunks[key] ?? []).reduce(0) { incrSize, data in
            incrSize + data.count
        }

        let baseSize = if let i = storedDocSize[key] {
            i
        } else {
            try await _storage.load(id: key)?.count ?? 0
        }

        let chunkSize = if let j = storedChunkSize[key] {
            j
        } else {
            try await _storage.loadRange(id: key, prefix: chunkNamespace).reduce(0) { incrSize, data in
                incrSize + data.count
            }
        }
        return chunkSize > baseSize || inMemSize > baseSize
    }

    /// Determine if the document provided has changes not represented by the underlying storage system
    /// - Parameters:
    ///   - key: The Id of the document
    ///   - doc: The Automerge document
    /// - Returns: A Boolean value that indicates the document has changes.
    func shouldSave(for key: DocumentId, doc: Document) -> Bool {
        guard let storedHeads = latestHeads[key] else {
            return true
        }
        let newHeads = doc.heads()
        if newHeads == storedHeads {
            return false
        }
        return true
    }

    /// Saves a document to the storage backend, compacting it if needed.
    /// - Parameters:
    ///   - id: The Id of the document
    ///   - doc: The automerge document
    public func saveDoc(id: DocumentId, doc: Document) async throws {
        if shouldSave(for: id, doc: doc) {
            if try await shouldCompact(id) {
                try await compact(id: id, doc: doc)
                chunks[id] = []
            } else {
                try await saveIncremental(id: id, doc: doc)
            }
        }
    }

    /// A concurrency safe compaction routine to consolidate in-memory and stored incremental changes into a compacted
    /// Automerge document.
    /// - Parameters:
    ///   - id: The document Id to compact
    ///   - doc: The document to compact.
    public func compact(id: DocumentId, doc: Document) async throws {
        compacting = true
        // A storage error below must not leave compaction switched off for the rest of the process.
        defer { compacting = false }
        // Before the first suspension: an incremental save already in flight would otherwise set the
        // saved heads back behind the snapshot's once it lands.
        chunkGeneration[id, default: 0] += 1
        // compactions do not overlap (`compacting`), so only a purge can replace the set under this one
        let purges = purgeGeneration[id, default: 0]
        let providedData = doc.save()
        var combined: Data = if let baseData = try await _storage.load(id: id) {
            // loading all the changes from the base document and any incremental saves available
            baseData
        } else {
            // loading only incremental saves available, the base document doesn't exist in storage
            Data()
        }

        combined.append(providedData)

        let inMemChunks: [Data] = chunks[id] ?? []
        var foundChunkHashValues: [Int] = []
        for chunk in inMemChunks {
            foundChunkHashValues.append(chunk.hashValue)
            combined.append(chunk)
        }

        let storageChunks = try await _storage.loadRange(id: id, prefix: chunkNamespace)
        for chunk in storageChunks {
            combined.append(chunk)
        }

        let compactedDoc = try Document(combined, logLevel: documentLogLevel)

        let compactedData = compactedDoc.save()
        // only remove the chunks AFTER the save is complete
        try await _storage.save(id: id, data: compactedData)
        if purgeGeneration[id, default: 0] != purges {
            // The document was purged while the snapshot was being written. The purge could not have
            // seen the snapshot, so take it back out and leave the cleared memos alone.
            try await _storage.remove(id: id)
            return
        }
        storedDocSize[id] = compactedData.count
        latestHeads[id] = compactedDoc.heads()

        // refresh the inMemChunks in case its changed (possible with re-entrancy, due to
        // the possible suspension points at each of the above `await` statements since we
        // grabbed the in memeory reference and made a copy)
        var updatedMemChunks = chunks[id] ?? []
        for d in inMemChunks {
            if let indexToRemove = updatedMemChunks.firstIndex(of: d) {
                updatedMemChunks.remove(at: indexToRemove)
            }
        }
        chunks[id] = updatedMemChunks
        memoryChunkSize[id] = updatedMemChunks.reduce(0) { incrSize, data in
            incrSize + data.count
        }

        // now iterate through and remove the stored chunks we loaded earlier
        // Doing this last, intentionally - it's another suspension point, and IF someone
        // reads the base document and appends the found changes in a load, they'll still
        // end up with the same document, so these can safely be removed _after_ the new
        // compacted document has been stored away by the underlying storage provider.
        try await _storage.removeRange(id: id, prefix: chunkNamespace, data: storageChunks)
        let remainingChunkSize = try await _storage.loadRange(id: id, prefix: chunkNamespace)
            .reduce(0) { incrSize, data in
                incrSize + data.count
            }
        // a purge during the cleanup above has cleared the memos; do not set one back
        if purgeGeneration[id, default: 0] == purges {
            storedChunkSize[id] = remainingChunkSize
        }
    }

    /// Save incremental changes of the existing Automerge document.
    /// - Parameters:
    ///   - id: The Id of the document
    ///   - doc: The automerge document
    public func saveIncremental(id: DocumentId, doc: Document) async throws {
        var chunkCollection = chunks[id] ?? []
        let oldHeads = latestHeads[id] ?? Set<ChangeHash>()
        // Capture the heads the chunk covers before the write suspends. `doc` is a live, mutable
        // document: a commit that lands during the write is not in this chunk, and reading the
        // heads afterwards would record it as stored, so the next save would skip it.
        let savedHeads = doc.heads()
        let generation = chunkGeneration[id, default: 0]
        let purges = purgeGeneration[id, default: 0]
        let incrementalChanges = try doc.encodeChangesSince(heads: oldHeads)
        chunkCollection.append(incrementalChanges)
        chunks[id] = chunkCollection
        try await _storage.addToRange(id: id, prefix: chunkNamespace, data: incrementalChanges)
        if purgeGeneration[id, default: 0] != purges {
            // The document was purged while this chunk was being written. The chunk is an orphan that
            // cannot load without the purged base, and recording heads for a document that no longer
            // exists would make one restored under this id look already saved. Take it back out.
            try await _storage.removeRange(id: id, prefix: chunkNamespace, data: [incrementalChanges])
            if var remaining = chunks[id], let index = remaining.firstIndex(of: incrementalChanges) {
                remaining.remove(at: index)
                chunks[id] = remaining
            }
            return
        }
        if chunkGeneration[id, default: 0] != generation {
            // A compaction ran while this chunk was being written. Its snapshot holds the chunk's
            // changes, so the chunk is redundant until the next compaction folds it in, and the heads
            // it would record are behind the snapshot's. Leave the chunk on disk, where it is the
            // changes' only copy should the snapshot's write have failed, and leave the heads alone.
            return
        }
        latestHeads[id] = savedHeads
    }

//    public func loadSyncState(id _: DocumentId, storageId _: SyncV1.STORAGE_ID) async -> SyncState {
//        SyncState()
//    }
//
//    public func saveSyncState(id _: DocumentId, storageId _: SyncV1.STORAGE_ID, state _: SyncState) async {}
}
