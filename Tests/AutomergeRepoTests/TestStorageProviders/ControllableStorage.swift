import AutomergeRepo
import struct Foundation.Data
import struct Foundation.UUID

/// An in-memory storage provider whose writes a test can slow down or make fail.
///
/// Each write (`save` or `addToRange`) consumes the next queued delay before it runs, so a test can make the
/// first save slow and the second fast to reorder their completion.
@AutomergeRepo
public final class ControllableStorage: StorageProvider {
    public struct WriteFailed: Error {}

    public nonisolated let id: STORAGE_ID = UUID().uuidString
    private let backing = InMemoryStorage()
    private var writeDelays: [Duration] = []
    private var loadDelays: [Duration] = []
    private var failWrites = false
    public private(set) var writeCount = 0

    public nonisolated init() {}

    /// Delays consumed in order by the next writes; a write with no queued delay runs immediately.
    public func queueWriteDelays(_ delays: [Duration]) {
        writeDelays.append(contentsOf: delays)
    }

    /// Delays consumed in order by the next `load` calls; a load with no queued delay runs immediately.
    public func queueLoadDelays(_ delays: [Duration]) {
        loadDelays.append(contentsOf: delays)
    }

    public func setWritesFail(_ fail: Bool) {
        failWrites = fail
    }

    private func beforeWrite() async throws {
        writeCount += 1
        if !writeDelays.isEmpty {
            let delay = writeDelays.removeFirst()
            if delay > .zero {
                try await Task.sleep(for: delay)
            }
        }
        if failWrites {
            throw WriteFailed()
        }
    }

    public func load(id: DocumentId) async throws -> Data? {
        if !loadDelays.isEmpty {
            let delay = loadDelays.removeFirst()
            if delay > .zero {
                try await Task.sleep(for: delay)
            }
        }
        return await backing.load(id: id)
    }

    public func save(id: DocumentId, data: Data) async throws {
        try await beforeWrite()
        await backing.save(id: id, data: data)
    }

    public func remove(id: DocumentId) async throws {
        await backing.remove(id: id)
    }

    public func addToRange(id: DocumentId, prefix: String, data: Data) async throws {
        try await beforeWrite()
        await backing.addToRange(id: id, prefix: prefix, data: data)
    }

    public func loadRange(id: DocumentId, prefix: String) async throws -> [Data] {
        await backing.loadRange(id: id, prefix: prefix)
    }

    public func removeRange(id: DocumentId, prefix: String, data: [Data]) async throws {
        await backing.removeRange(id: id, prefix: prefix, data: data)
    }
}
