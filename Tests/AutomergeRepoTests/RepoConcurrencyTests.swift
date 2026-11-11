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
}
