import Automerge
@testable import AutomergeRepo
import Foundation
import XCTest

/// Races between the repo actor and the things that suspend it or run beside it: edits from other
/// threads, storage loads, and peers that ask for documents the repo does not have yet.
final class RepoConcurrencyTests: XCTestCase {
    private func peerRepo(
        storage: (any StorageProvider)? = nil,
        fetchDeadline: Duration = .seconds(300)
    ) async -> (Repo, RecordingNetworkProvider) {
        let repo: Repo = if let storage {
            Repo(
                sharePolicy: SharePolicy.agreeable,
                storage: storage,
                maxResolveFetchIterations: 10,
                resolveFetchIterationDelay: fetchDeadline / 10
            )
        } else {
            Repo(
                sharePolicy: SharePolicy.agreeable,
                maxResolveFetchIterations: 10,
                resolveFetchIterationDelay: fetchDeadline / 10
            )
        }
        let adapter = RecordingNetworkProvider(name: "peer")
        await repo.addNetworkAdapter(adapter: adapter)
        await adapter.peer(with: "the-peer")
        return (repo, adapter)
    }

    private func waitUntil(
        timeout: Duration = .seconds(5),
        _ condition: @escaping @Sendable () async -> Bool
    ) async -> Bool {
        let deadline = ContinuousClock.now + timeout
        while ContinuousClock.now < deadline {
            if await condition() { return true }
            try? await Task.sleep(for: .milliseconds(20))
        }
        return await condition()
    }

    /// A sync message for `id` as peer `from` would send it, carrying `from`'s whole document.
    ///
    /// A first message from a fresh sync state carries only heads. Exchange one round with an empty
    /// document first, so the peer knows the other side has nothing and sends every change.
    private func syncMessage(for id: DocumentId, from peer: PEER_ID, doc: Document, to repo: Repo) throws
        -> SyncV1Msg.SyncMsg
    {
        let peerState = SyncState()
        let empty = Document()
        let hello = try XCTUnwrap(empty.generateSyncMessage(state: SyncState()))
        try doc.receiveSyncMessage(state: peerState, message: hello)
        let data = try XCTUnwrap(doc.generateSyncMessage(state: peerState))
        return .init(documentId: id.description, senderId: peer, targetId: repo.peerId, sync_message: data)
    }

    // MARK: Sync state exists from first use

    func testTheFirstOfferToAPeerUsesAStoredSyncState() async throws {
        let (repo, adapter) = await peerRepo()
        let handle = try await repo.create()
        _ = await waitUntil { await !adapter.sent.isEmpty }
        let stored = await { @AutomergeRepo in repo.syncState(id: handle.id, peer: "the-peer") }()
        // the state used to generate the offer recorded that it was sent; a throwaway would not have,
        // and a fresh state would generate the same offer again
        XCTAssertNil(stored.theirHeads, "sanity: no reply yet")
        let generatesNothingNew = handle.doc.generateSyncMessage(state: stored) == nil
        XCTAssertTrue(generatesNothingNew, "the stored state already covers the offer that went out")
    }

    // MARK: Concurrent finds share one deadline

    func testConcurrentFindsForTheSameMissingDocumentWaitTheFullTimeout() async throws {
        let deadline: Duration = .milliseconds(600)
        let (repo, _) = await peerRepo(fetchDeadline: deadline)
        let id = DocumentId()
        let clock = ContinuousClock()
        async let first = clock.measure { _ = try? await repo.find(id: id) }
        async let second = clock.measure { _ = try? await repo.find(id: id) }
        let (a, b) = await (first, second)
        // each waiter would halve the other's wait if they shared a retry counter
        XCTAssertGreaterThanOrEqual(a, deadline - .milliseconds(50), "\(a)")
        XCTAssertGreaterThanOrEqual(b, deadline - .milliseconds(50), "\(b)")
    }

    // MARK: A sync that lands during a storage load

    func testSyncArrivingDuringAStorageLoadIsKeptAndMergedWithTheStoredCopy() async throws {
        let storage = ControllableStorage()
        let (repo, _) = await peerRepo(storage: storage)
        // a document on disk with one key, and a peer whose copy has a second key on top of it
        let id = DocumentId()
        let stored = Document()
        try stored.put(obj: .ROOT, key: "stored", value: .String("disk"))
        stored.commitWith()
        try await storage.save(id: id, data: stored.save())
        let peerCopy = try Document(stored.save())
        try peerCopy.put(obj: .ROOT, key: "remote", value: .String("peer"))
        peerCopy.commitWith()

        await storage.queueLoadDelays([.milliseconds(300)])
        let find = Task { try await repo.find(id: id) }
        try await Task.sleep(for: .milliseconds(50)) // find is now suspended in the storage load
        await repo.handleSync(msg: try syncMessage(for: id, from: "the-peer", doc: peerCopy, to: repo))

        let found = try await find.value
        XCTAssertEqual(try found.doc.get(obj: .ROOT, key: "stored"), .Scalar(.String("disk")))
        XCTAssertEqual(try found.doc.get(obj: .ROOT, key: "remote"), .Scalar(.String("peer")))
        let again = try await repo.find(id: id)
        XCTAssertTrue(again.doc === found.doc, "one document object, not a storage copy replacing the synced one")
    }

    func testSyncArrivingDuringAStorageMissIsKept() async throws {
        let storage = ControllableStorage()
        let (repo, _) = await peerRepo(storage: storage, fetchDeadline: .seconds(10))
        let id = DocumentId()
        let peerCopy = try RepoHelpers.documentWithData()

        await storage.queueLoadDelays([.milliseconds(300)])
        let find = Task { try await repo.find(id: id) }
        try await Task.sleep(for: .milliseconds(50))
        await repo.handleSync(msg: try syncMessage(for: id, from: "the-peer", doc: peerCopy, to: repo))

        let clock = ContinuousClock()
        let found = try await clock.measure { try await find.value }
        _ = found
        let handle = try await repo.find(id: id)
        XCTAssertTrue(handle.doc.equivalentContents(peerCopy))
        XCTAssertLessThan(found, .seconds(2), "the find must not go on to request a document it already has")
    }

    // MARK: Edits from another thread

    func testEditsFromABackgroundThreadStillSyncAndSave() async throws {
        let storage = ControllableStorage()
        let (repo, adapter) = await peerRepo(storage: storage)
        let handle = try await repo.create()
        await adapter.clearSent()
        let writesBefore = await storage.writeCount

        let done = DispatchSemaphore(value: 0)
        DispatchQueue.global().async {
            try? handle.doc.put(obj: .ROOT, key: "k", value: .String("v"))
            handle.doc.commitWith()
            done.signal()
        }
        done.wait()

        let synced = await waitUntil { await !adapter.sent.isEmpty }
        XCTAssertTrue(synced, "the edit should reach the peer")
        try await repo.savePendingChanges(id: handle.id)
        let writesAfter = await storage.writeCount
        XCTAssertGreaterThan(writesAfter, writesBefore)
    }

    func testABurstOfEditsProducesFewSyncPasses() async throws {
        let (repo, adapter) = await peerRepo()
        let handle = try await repo.create()
        _ = await waitUntil { await !adapter.sent.isEmpty } // the create's own offer
        await adapter.clearSent()

        let edits = 50
        for i in 0..<edits {
            try handle.doc.put(obj: .ROOT, key: "k\(i)", value: .String("\(i)"))
        }
        handle.doc.commitWith()
        let settled = await waitUntil { await !adapter.sent.isEmpty }
        XCTAssertTrue(settled)
        try await Task.sleep(for: .milliseconds(200))
        let sent = await adapter.sent
        XCTAssertLessThan(sent.count, edits / 5, "\(edits) edits should coalesce into a few syncs, got \(sent.count)")
    }
}
