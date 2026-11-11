import Automerge
@testable import AutomergeRepo
import AutomergeUtilities
import XCTest

final class StorageSubsystemTests: XCTestCase {
    var subsystem: DocumentStorage!
    var testStorageProvider: InMemoryStorage!

    override func setUp() async throws {
        let storageProvider = InMemoryStorage()
        let incrementalKeys = await storageProvider.incrementalKeys()
        let docIds = await storageProvider.storageKeys()
        XCTAssertEqual(docIds.count, 0)
        XCTAssertEqual(incrementalKeys.count, 0)

        subsystem = DocumentStorage(storageProvider, verbosity: .tracing, documentLogVerbosity: .errorOnly)
        testStorageProvider = storageProvider
    }

    func assertCounts(docIds: Int, incrementals: Int) async {
        let countOfIncrementalKeys = await testStorageProvider?.incrementalKeys().count
        let countOfDocumentIdKeys = await testStorageProvider?.storageKeys().count
        XCTAssertEqual(countOfDocumentIdKeys, docIds)
        XCTAssertEqual(countOfIncrementalKeys, incrementals)
    }

    func docDataSize(id: DocumentId) async -> Int {
        await testStorageProvider?.load(id: id)?.count ?? 0
    }

    func combinedIncData(id: DocumentId) async -> Int {
        if let inc = await testStorageProvider?.loadRange(id: id, prefix: subsystem.chunkNamespace) {
            return inc.reduce(0) { partialResult, data in
                partialResult + data.count
            }
        }
        return 0
    }

    func testSubsystemSetup() async throws {
        XCTAssertNotNil(subsystem)
        let newDoc = Document()
        let newDocId = DocumentId()

        try await subsystem.saveDoc(id: newDocId, doc: newDoc)
        await assertCounts(docIds: 0, incrementals: 1)

        let combinedKeys = await testStorageProvider?.incrementalKeys()
        XCTAssertEqual(combinedKeys?.count, 1)
        XCTAssertEqual(combinedKeys?[0].id, newDocId)
        XCTAssertEqual(combinedKeys?[0].prefix, "incrChanges")
        let incData: [Data]? = await testStorageProvider?.loadRange(id: newDocId, prefix: "incrChanges")
        let incDataUnwrapped = try XCTUnwrap(incData)
        XCTAssertEqual(incDataUnwrapped.count, 1)
        XCTAssertEqual(incDataUnwrapped[0].count, 0)

        let txt = try newDoc.putObject(obj: .ROOT, key: "words", ty: .Text)
        try await subsystem.saveDoc(id: newDocId, doc: newDoc)

        await assertCounts(docIds: 0, incrementals: 1)
        var incSize = await combinedIncData(id: newDocId)
        XCTAssertEqual(incSize, 58)

        try newDoc.updateText(obj: txt, value: "Hello World!")
        try await subsystem.saveDoc(id: newDocId, doc: newDoc)

        await assertCounts(docIds: 1, incrementals: 1)
        incSize = await combinedIncData(id: newDocId)
        var docSize = await docDataSize(id: newDocId)
        XCTAssertEqual(docSize, 176)
        XCTAssertEqual(incSize, 0)

        try await subsystem.compact(id: newDocId, doc: newDoc)

        await assertCounts(docIds: 1, incrementals: 1)
        incSize = await combinedIncData(id: newDocId)
        docSize = await docDataSize(id: newDocId)
        XCTAssertEqual(docSize, 176)
        XCTAssertEqual(incSize, 0)
//        if let incrementals = await testStorageProvider?.loadRange(id: newDocId, prefix: subsystem.chunkNamespace) {
//            print(incrementals)
//        }
    }

    func testSubsystemLoadDoc() async throws {
        let newDoc = try RepoHelpers.documentWithData()
        let newDocId = DocumentId()
        try await subsystem.saveDoc(id: newDocId, doc: newDoc)

        let loadedDoc = try await subsystem.loadDoc(id: newDocId)
        let unwrappedDoc = try XCTUnwrap(loadedDoc)
        XCTAssertTrue(newDoc.equivalentContents(unwrappedDoc))
    }

    func testCommitDuringAnIncrementalWriteIsStillSavedByTheNextSave() async throws {
        let slowProvider = ControllableStorage()
        let subsystem = DocumentStorage(slowProvider, verbosity: .errorOnly, documentLogVerbosity: .errorOnly)
        let id = DocumentId()
        let doc = Document()
        try doc.put(obj: .ROOT, key: "first", value: .String("1"))
        doc.commitWith()

        // the first save's write is slow; a local commit lands while it is in flight
        await slowProvider.queueWriteDelays([.milliseconds(300)])
        let firstSave = Task { try await subsystem.saveDoc(id: id, doc: doc) }
        try await Task.sleep(for: .milliseconds(50))
        try doc.put(obj: .ROOT, key: "second", value: .String("2"))
        doc.commitWith()
        try await firstSave.value

        // the second commit was not in the first chunk, so the next save must write it
        try await subsystem.saveDoc(id: id, doc: doc)
        let writes = await slowProvider.writeCount
        XCTAssertEqual(writes, 2, "the second save must not be skipped as already stored")

        // read back from the provider alone, bypassing this subsystem's in-memory chunks
        let freshSubsystem = DocumentStorage(slowProvider, verbosity: .errorOnly, documentLogVerbosity: .errorOnly)
        let loadedFromDisk = try await freshSubsystem.loadDoc(id: id)
        let loaded = try XCTUnwrap(loadedFromDisk)
        XCTAssertEqual(try loaded.get(obj: .ROOT, key: "first"), .Scalar(.String("1")))
        XCTAssertEqual(try loaded.get(obj: .ROOT, key: "second"), .Scalar(.String("2")))
        XCTAssertEqual(loaded.heads(), doc.heads())
    }

    func testPurgeRemovesAChunkOnlyDocument() async throws {
        let newDoc = try RepoHelpers.documentWithData()
        let newDocId = DocumentId()
        try await subsystem.saveDoc(id: newDocId, doc: newDoc)
        // the first save writes only an incremental chunk, no base snapshot
        let base = await testStorageProvider.load(id: newDocId)
        let chunks = await testStorageProvider.loadRange(id: newDocId, prefix: subsystem.chunkNamespace)
        XCTAssertNil(base)
        XCTAssertEqual(chunks.count, 1)

        try await subsystem.purgeDoc(id: newDocId)

        let chunksAfter = await testStorageProvider.loadRange(id: newDocId, prefix: subsystem.chunkNamespace)
        XCTAssertTrue(chunksAfter.isEmpty, "purge must remove incremental chunks, not just the base snapshot")
        let reloaded = try await subsystem.loadDoc(id: newDocId)
        XCTAssertNil(reloaded, "nothing should load back after a purge")
        // a fresh subsystem over the same provider agrees: the document is gone from disk
        let fresh = DocumentStorage(testStorageProvider, verbosity: .errorOnly, documentLogVerbosity: .errorOnly)
        let reloadedFresh = try await fresh.loadDoc(id: newDocId)
        XCTAssertNil(reloadedFresh)
    }

    func testPurgeForgetsHeadsSoTheSameIdCanBeSavedAgain() async throws {
        let id = DocumentId()
        let first = try RepoHelpers.documentWithData()
        try await subsystem.saveDoc(id: id, doc: first)
        try await subsystem.purgeDoc(id: id)
        let purged = try await subsystem.loadDoc(id: id)
        XCTAssertNil(purged, "precondition: the purge removed the document")

        // a new document under the same id, with the same heads as the purged one: it must still be
        // written, which it would not be if the purged heads were still recorded as saved
        let again = try Document(first.save())
        try await subsystem.saveDoc(id: id, doc: again)
        let reloaded = try await subsystem.loadDoc(id: id)
        XCTAssertNotNil(reloaded, "a document saved after a purge of the same id must reach storage")
    }

    /// Saves `doc` until it has a base snapshot and no chunks, so the next save is incremental.
    /// (With no base, any in-memory chunk makes the next save a compaction instead.)
    private func saveToBaseSnapshot(
        _ doc: Document, id: DocumentId, in subsystem: DocumentStorage, provider: ControllableStorage
    ) async throws {
        try await subsystem.saveDoc(id: id, doc: doc) // incremental, chunk only
        try doc.put(obj: .ROOT, key: "settle", value: .String("1"))
        doc.commitWith()
        try await subsystem.saveDoc(id: id, doc: doc) // compacts: chunk > absent base
        let base = try await provider.load(id: id)
        let chunks = try await provider.loadRange(id: id, prefix: subsystem.chunkNamespace)
        XCTAssertNotNil(base, "precondition: a base snapshot exists")
        XCTAssertTrue(chunks.isEmpty, "precondition: no chunks remain")
    }

    func testASaveInFlightWhenThePurgeRunsLeavesNothingBehind() async throws {
        let provider = ControllableStorage()
        let subsystem = DocumentStorage(provider, verbosity: .errorOnly, documentLogVerbosity: .errorOnly)
        let id = DocumentId()
        let doc = try RepoHelpers.documentWithData()
        try await saveToBaseSnapshot(doc, id: id, in: subsystem, provider: provider)
        try doc.put(obj: .ROOT, key: "late", value: .String("edit"))
        doc.commitWith()

        // this save is incremental; its chunk write is slow and the purge runs while it is in flight
        await provider.queueWriteDelays([.milliseconds(300)])
        let straddlingSave = Task { try await subsystem.saveDoc(id: id, doc: doc) }
        try await Task.sleep(for: .milliseconds(50))
        try await subsystem.purgeDoc(id: id)
        try await straddlingSave.value

        let chunks = try await provider.loadRange(id: id, prefix: subsystem.chunkNamespace)
        XCTAssertTrue(chunks.isEmpty, "the straddling save must take its chunk back out")
        let base = try await provider.load(id: id)
        XCTAssertNil(base)
        let heads = await subsystem.latestHeads[id]
        XCTAssertNil(heads, "no heads may be recorded for a purged document")
        let fresh = DocumentStorage(provider, verbosity: .errorOnly, documentLogVerbosity: .errorOnly)
        let reloaded = try await fresh.loadDoc(id: id)
        XCTAssertNil(reloaded)
    }

    func testACompactionInFlightWhenThePurgeRunsLeavesNothingBehind() async throws {
        let provider = ControllableStorage()
        let subsystem = DocumentStorage(provider, verbosity: .errorOnly, documentLogVerbosity: .errorOnly)
        let id = DocumentId()
        let doc = Document()
        try doc.put(obj: .ROOT, key: "a", value: .String("1"))
        doc.commitWith()
        try await subsystem.saveDoc(id: id, doc: doc)
        // large enough that the next save compacts; its snapshot write is slow
        try doc.put(obj: .ROOT, key: "b", value: .String(String(repeating: "x", count: 2000)))
        doc.commitWith()
        await provider.queueWriteDelays([.milliseconds(300)])
        let straddlingCompaction = Task { try await subsystem.saveDoc(id: id, doc: doc) }
        try await Task.sleep(for: .milliseconds(50))
        try await subsystem.purgeDoc(id: id)
        try await straddlingCompaction.value

        let base = try await provider.load(id: id)
        XCTAssertNil(base, "the straddling compaction must take its snapshot back out")
        let chunks = try await provider.loadRange(id: id, prefix: subsystem.chunkNamespace)
        XCTAssertTrue(chunks.isEmpty)
        let heads = await subsystem.latestHeads[id]
        XCTAssertNil(heads)
    }

    func testADocumentSavedUnderAPurgedIdAfterAStraddlingSaveReachesStorage() async throws {
        let provider = ControllableStorage()
        let subsystem = DocumentStorage(provider, verbosity: .errorOnly, documentLogVerbosity: .errorOnly)
        let id = DocumentId()
        let doc = try RepoHelpers.documentWithData()
        try await saveToBaseSnapshot(doc, id: id, in: subsystem, provider: provider)
        try doc.put(obj: .ROOT, key: "late", value: .String("edit"))
        doc.commitWith()
        await provider.queueWriteDelays([.milliseconds(300)])
        let straddlingSave = Task { try await subsystem.saveDoc(id: id, doc: doc) }
        try await Task.sleep(for: .milliseconds(50))
        try await subsystem.purgeDoc(id: id)
        try await straddlingSave.value

        // The same document comes back under the same id, as a restore from a peer would. Its heads
        // equal what the straddling save would have recorded, so stale heads would skip this save and
        // leave only the straddling chunk on disk, which cannot load without the purged base.
        let restored = try Document(doc.save())
        try await subsystem.saveDoc(id: id, doc: restored)
        let fresh = DocumentStorage(provider, verbosity: .errorOnly, documentLogVerbosity: .errorOnly)
        let reloaded = try await fresh.loadDoc(id: id)
        XCTAssertEqual(try XCTUnwrap(reloaded).heads(), restored.heads())
    }

    func testASaveInFlightWhenACompactionRunsKeepsItsChunkAndTheCompactedHeads() async throws {
        let provider = ControllableStorage()
        let subsystem = DocumentStorage(provider, verbosity: .errorOnly, documentLogVerbosity: .errorOnly)
        let id = DocumentId()
        let doc = try RepoHelpers.documentWithData()
        try await saveToBaseSnapshot(doc, id: id, in: subsystem, provider: provider)

        // an incremental save whose chunk write is slow...
        try doc.put(obj: .ROOT, key: "small", value: .String("edit"))
        doc.commitWith()
        await provider.queueWriteDelays([.milliseconds(300)])
        let straddlingSave = Task { try await subsystem.saveDoc(id: id, doc: doc) }
        try await Task.sleep(for: .milliseconds(50))
        // ...while a compaction of a larger document runs to completion
        try doc.put(obj: .ROOT, key: "big", value: .String(String(repeating: "x", count: 4000)))
        doc.commitWith()
        try await subsystem.compact(id: id, doc: doc)
        let compactedHeads = doc.heads()
        try await straddlingSave.value

        // the chunk is redundant (the snapshot holds its changes) but stays: were the snapshot's write to
        // fail, it would be those changes' only copy on disk. The next compaction folds it in.
        let chunks = try await provider.loadRange(id: id, prefix: subsystem.chunkNamespace)
        XCTAssertEqual(chunks.count, 1, "the straddling chunk stays on disk")
        let heads = await subsystem.latestHeads[id]
        XCTAssertEqual(heads, compactedHeads, "the straddling save must not set the saved heads back")
        let fresh = DocumentStorage(provider, verbosity: .errorOnly, documentLogVerbosity: .errorOnly)
        let reloaded = try await fresh.loadDoc(id: id)
        XCTAssertEqual(try XCTUnwrap(reloaded).heads(), compactedHeads, "snapshot plus redundant chunk loads clean")
    }

    func testASaveStraddlingACompactionWhoseWriteFailsKeepsTheChangesOnDisk() async throws {
        let provider = ControllableStorage()
        let subsystem = DocumentStorage(provider, verbosity: .errorOnly, documentLogVerbosity: .errorOnly)
        let id = DocumentId()
        let doc = try RepoHelpers.documentWithData()
        try await saveToBaseSnapshot(doc, id: id, in: subsystem, provider: provider)

        // the incremental save's write is slow; the compaction's snapshot write, which starts later,
        // is slower still and then fails
        try doc.put(obj: .ROOT, key: "small", value: .String("edit"))
        doc.commitWith()
        await provider.queueWriteDelays([.milliseconds(200), .milliseconds(400)])
        let straddlingSave = Task { try await subsystem.saveDoc(id: id, doc: doc) }
        try await Task.sleep(for: .milliseconds(50)) // the save's write has started and will succeed
        try doc.put(obj: .ROOT, key: "big", value: .String(String(repeating: "x", count: 4000)))
        doc.commitWith()
        await provider.setWritesFail(true) // the compaction's write starts after this and will fail
        let compaction = Task { try await subsystem.compact(id: id, doc: doc) }
        try await straddlingSave.value
        do {
            try await compaction.value
            XCTFail("expected the snapshot write to fail")
        } catch is ControllableStorage.WriteFailed {}
        await provider.setWritesFail(false)

        // the small edit is on disk in the straddling chunk; a fresh load has it
        let fresh = DocumentStorage(provider, verbosity: .errorOnly, documentLogVerbosity: .errorOnly)
        let reloaded = try await fresh.loadDoc(id: id)
        XCTAssertEqual(try XCTUnwrap(reloaded).get(obj: .ROOT, key: "small"), .Scalar(.String("edit")))
        // and the next save still writes the big edit, which never reached disk
        try await subsystem.saveDoc(id: id, doc: doc)
        let afterRetry = try await DocumentStorage(provider, verbosity: .errorOnly, documentLogVerbosity: .errorOnly)
            .loadDoc(id: id)
        XCTAssertEqual(try XCTUnwrap(afterRetry).heads(), doc.heads())
    }

    func testCompactionResumesAfterAFailedCompaction() async throws {
        let provider = ControllableStorage()
        let subsystem = DocumentStorage(provider, verbosity: .errorOnly, documentLogVerbosity: .errorOnly)
        let id = DocumentId()
        let doc = Document()
        try doc.put(obj: .ROOT, key: "a", value: .String("1"))
        doc.commitWith()
        try await subsystem.saveDoc(id: id, doc: doc) // incremental; nothing to compact yet

        // make the document big enough relative to its (absent) base that the next save compacts, and
        // have that compaction's write fail
        try doc.put(obj: .ROOT, key: "b", value: .String(String(repeating: "x", count: 2000)))
        doc.commitWith()
        await provider.setWritesFail(true)
        do {
            try await subsystem.saveDoc(id: id, doc: doc)
            XCTFail("expected the failing write to surface")
        } catch is ControllableStorage.WriteFailed {}
        await provider.setWritesFail(false)

        // a later save must be able to compact again
        try doc.put(obj: .ROOT, key: "c", value: .String("3"))
        doc.commitWith()
        try await subsystem.saveDoc(id: id, doc: doc)
        let base = try await provider.load(id: id)
        XCTAssertNotNil(base, "compaction stayed switched off after the failed attempt")
        let fresh = DocumentStorage(provider, verbosity: .errorOnly, documentLogVerbosity: .errorOnly)
        let reloaded = try await fresh.loadDoc(id: id)
        XCTAssertEqual(try XCTUnwrap(reloaded).heads(), doc.heads())
    }

    func testLoadDoesNotMoveRecordedHeadsBackwards() async throws {
        let id = DocumentId()
        let doc = Document()
        try doc.put(obj: .ROOT, key: "a", value: .String("1"))
        doc.commitWith()
        try await subsystem.saveDoc(id: id, doc: doc)
        let headsAfterFirstSave = doc.heads()
        // what the provider holds after the first save, to put back later
        let baseBefore = await testStorageProvider.load(id: id)
        let chunksBefore = await testStorageProvider.loadRange(id: id, prefix: subsystem.chunkNamespace)

        try doc.put(obj: .ROOT, key: "b", value: .String("2"))
        doc.commitWith()
        try await subsystem.saveDoc(id: id, doc: doc)
        let recorded = await subsystem.latestHeads[id]
        XCTAssertEqual(recorded, doc.heads())

        // A load that sees less than the save recorded, as a load racing a write in flight would. Stand in
        // for that by restoring the provider to its state after the first save, and clearing the in-memory
        // chunks that would otherwise fill the gap.
        await testStorageProvider.remove(id: id)
        let chunksNow = await testStorageProvider.loadRange(id: id, prefix: subsystem.chunkNamespace)
        await testStorageProvider.removeRange(id: id, prefix: subsystem.chunkNamespace, data: chunksNow)
        if let baseBefore {
            await testStorageProvider.save(id: id, data: baseBefore)
        }
        for chunk in chunksBefore {
            await testStorageProvider.addToRange(id: id, prefix: subsystem.chunkNamespace, data: chunk)
        }
        await { @AutomergeRepo in subsystem.chunks[id] = [] }()

        let loaded = try await subsystem.loadDoc(id: id)
        XCTAssertEqual(try XCTUnwrap(loaded).heads(), headsAfterFirstSave, "sanity: the load is behind")
        let afterLoad = await subsystem.latestHeads[id]
        XCTAssertEqual(afterLoad, doc.heads(), "a load behind the recorded heads must leave them alone")

        // a subsystem with nothing recorded still learns the heads from its first load
        let fresh = DocumentStorage(testStorageProvider, verbosity: .errorOnly, documentLogVerbosity: .errorOnly)
        _ = try await fresh.loadDoc(id: id)
        let learned = await fresh.latestHeads[id]
        XCTAssertEqual(learned, headsAfterFirstSave)
    }

    func testSubsystemPurgeDoc() async throws {
        let newDoc = try RepoHelpers.documentWithData()
        let newDocId = DocumentId()
        try await subsystem.saveDoc(id: newDocId, doc: newDoc)

        await assertCounts(docIds: 0, incrementals: 1)
        let incSize = await combinedIncData(id: newDocId)
        let docSize = await docDataSize(id: newDocId)
        XCTAssertEqual(docSize, 0)
        XCTAssertEqual(incSize, 106)

        try await subsystem.compact(id: newDocId, doc: newDoc)
        await assertCounts(docIds: 1, incrementals: 1)
        let compactedIncSize = await combinedIncData(id: newDocId)
        let compactedDocSize = await docDataSize(id: newDocId)
        XCTAssertEqual(compactedDocSize, 170)
        XCTAssertEqual(compactedIncSize, 0)

        try await subsystem.purgeDoc(id: newDocId)
        await assertCounts(docIds: 0, incrementals: 1)
        let purgedIncSize = await combinedIncData(id: newDocId)
        let purgedDocSize = await docDataSize(id: newDocId)
        XCTAssertEqual(purgedDocSize, 0)
        XCTAssertEqual(purgedIncSize, 0)
    }
}
