import Automerge
@testable import AutomergeRepo
import Foundation
import XCTest

/// A network provider that records what the repo asks it to send, and lets a test drive peer events
/// into the repo as if a transport had produced them.
@AutomergeRepo
final class RecordingNetworkProvider: NetworkProvider {
    typealias NetworkConnectionEndpoint = String

    let name: String
    var peeredConnections: [PeerConnectionInfo] = []
    var delegate: (any NetworkEventReceiver)?
    var sent: [(message: SyncV1Msg, to: PEER_ID?)] = []

    nonisolated init(name: String) {
        self.name = name
    }

    func connect(to _: String) async throws {}
    func disconnect() async {}

    func send(message: SyncV1Msg, to: PEER_ID?) async {
        sent.append((message, to))
    }

    func setDelegate(_ delegate: any NetworkEventReceiver, as _: PEER_ID, with _: PeerMetadata?) {
        self.delegate = delegate
    }

    // MARK: test controls

    private func connectionInfo(for peerId: PEER_ID) -> PeerConnectionInfo {
        // the repo keys peers by metadata; a nil here would leave the peer out of `Repo.peers()`
        PeerConnectionInfo(
            peerId: peerId,
            peerMetadata: PeerMetadata(storageId: nil, isEphemeral: true),
            endpoint: name,
            initiated: true,
            peered: true
        )
    }

    /// Reports a completed handshake with `peerId`, the way a transport does once it has peered.
    func peer(with peerId: PEER_ID) async {
        let info = connectionInfo(for: peerId)
        peeredConnections.append(info)
        await delegate?.receiveEvent(event: .ready(payload: info), from: self)
    }

    /// Reports a completed handshake without saying which adapter it came from (the pre-routing delegate call).
    func peerAnonymously(with peerId: PEER_ID) async {
        let info = connectionInfo(for: peerId)
        peeredConnections.append(info)
        await delegate?.receiveEvent(event: .ready(payload: info))
    }

    func dropPeer(_ peerId: PEER_ID) async {
        peeredConnections.removeAll { $0.peerId == peerId }
        await delegate?.receiveEvent(event: .peerDisconnect(payload: .init(peerId: peerId)), from: self)
    }

    func close() async {
        peeredConnections = []
        await delegate?.receiveEvent(event: .close, from: self)
    }

    func clearSent() {
        sent = []
    }
}

extension SyncV1Msg {
    /// The `targetId` carried in the message body, for the message kinds the repo routes by peer.
    var routedTarget: PEER_ID? {
        switch self {
        case let .sync(msg): msg.targetId
        case let .request(msg): msg.targetId
        case let .unavailable(msg): msg.targetId
        case let .ephemeral(msg): msg.targetId
        default: nil
        }
    }
}

final class NetworkRoutingTests: XCTestCase {
    let peerOne: PEER_ID = "peer-one"
    let peerTwo: PEER_ID = "peer-two"

    var repo: Repo!
    var adapterOne: RecordingNetworkProvider!
    var adapterTwo: RecordingNetworkProvider!

    override func setUp() async throws {
        repo = Repo(sharePolicy: SharePolicy.agreeable)
        adapterOne = RecordingNetworkProvider(name: "one")
        adapterTwo = RecordingNetworkProvider(name: "two")
        await repo.addNetworkAdapter(adapter: adapterOne)
        await repo.addNetworkAdapter(adapter: adapterTwo)
        await adapterOne.peer(with: peerOne)
        await adapterTwo.peer(with: peerTwo)
        let peers = await repo.peers()
        XCTAssertEqual(peers, [peerOne, peerTwo])
    }

    /// Polls `condition` until it holds or `timeout` passes.
    private func waitUntil(
        timeout: Duration = .seconds(5),
        _ condition: @escaping @Sendable () async -> Bool
    ) async -> Bool {
        let deadline = ContinuousClock.now + timeout
        while ContinuousClock.now < deadline {
            if await condition() {
                return true
            }
            try? await Task.sleep(for: .milliseconds(20))
        }
        return await condition()
    }

    private func ephemeral(to target: PEER_ID) async -> SyncV1Msg.EphemeralMsg {
        await .init(
            senderId: repo.peerId,
            targetId: target,
            count: 1,
            sessionId: "session",
            documentId: DocumentId().id,
            data: Data("hello".utf8)
        )
    }

    func testSyncGoesOnlyToTheAdapterThatReachedThePeer() async throws {
        let handle = try await repo.create()
        try handle.doc.put(obj: .ROOT, key: "title", value: .String("routed"))

        let bothAdaptersSawSync = await waitUntil { [adapterOne, adapterTwo] in
            let one = await adapterOne!.sent
            let two = await adapterTwo!.sent
            return !one.isEmpty && !two.isEmpty
        }
        XCTAssertTrue(bothAdaptersSawSync, "expected the new document to be offered to both peers")

        let sentOnOne = await adapterOne.sent
        let sentOnTwo = await adapterTwo.sent
        for (message, to) in sentOnOne {
            XCTAssertEqual(to, peerOne)
            XCTAssertEqual(message.routedTarget, peerOne, "\(message.debugDescription) went out on adapter one")
        }
        for (message, to) in sentOnTwo {
            XCTAssertEqual(to, peerTwo)
            XCTAssertEqual(message.routedTarget, peerTwo, "\(message.debugDescription) went out on adapter two")
        }
    }

    func testSendToPeerReachesOnlyItsAdapter() async throws {
        await repo.send(ephemeral(to: peerOne), to: peerOne)
        var one = await adapterOne.sent
        var two = await adapterTwo.sent
        XCTAssertEqual(one.count, 1)
        XCTAssertEqual(one.first?.to, peerOne)
        XCTAssertTrue(two.isEmpty)

        await adapterOne.clearSent()
        await repo.send(ephemeral(to: peerTwo), to: peerTwo)
        one = await adapterOne.sent
        two = await adapterTwo.sent
        XCTAssertTrue(one.isEmpty)
        XCTAssertEqual(two.count, 1)
        XCTAssertEqual(two.first?.to, peerTwo)
    }

    func testSendToUnknownPeerIsDroppedWithoutBroadcast() async throws {
        await repo.send(ephemeral(to: "unknown"), to: "unknown")
        let one = await adapterOne.sent
        let two = await adapterTwo.sent
        XCTAssertTrue(one.isEmpty)
        XCTAssertTrue(two.isEmpty)
    }

    func testSendWithoutPeerBroadcastsToEveryAdapter() async throws {
        await repo.send(ephemeral(to: repo.peerId), to: nil)
        let one = await adapterOne.sent
        let two = await adapterTwo.sent
        XCTAssertEqual(one.count, 1)
        XCTAssertNil(one.first?.to)
        XCTAssertEqual(two.count, 1)
        XCTAssertNil(two.first?.to)
    }

    func testPeerDisconnectRemovesRouteAndReadyRestoresIt() async throws {
        await adapterOne.dropPeer(peerOne)
        await repo.send(ephemeral(to: peerOne), to: peerOne)
        var one = await adapterOne.sent
        var two = await adapterTwo.sent
        XCTAssertTrue(one.isEmpty, "a disconnected peer has no route")
        XCTAssertTrue(two.isEmpty, "a disconnected peer's messages are not rerouted")

        await adapterOne.peer(with: peerOne)
        await repo.send(ephemeral(to: peerOne), to: peerOne)
        one = await adapterOne.sent
        two = await adapterTwo.sent
        XCTAssertEqual(one.count, 1)
        XCTAssertEqual(one.first?.to, peerOne)
        XCTAssertTrue(two.isEmpty)
    }

    func testCloseDropsEveryRouteOnThatAdapterOnly() async throws {
        await adapterOne.close()
        await repo.send(ephemeral(to: peerOne), to: peerOne)
        await repo.send(ephemeral(to: peerTwo), to: peerTwo)
        let one = await adapterOne.sent
        let two = await adapterTwo.sent
        XCTAssertTrue(one.isEmpty)
        XCTAssertEqual(two.count, 1)
        XCTAssertEqual(two.first?.to, peerTwo)
    }

    func testReplacementConnectionToSamePeerTakesOverTheRoute() async throws {
        // a second socket reaches the same peer id; the old one goes away afterwards
        await adapterTwo.peer(with: peerOne)
        await repo.send(ephemeral(to: peerOne), to: peerOne)
        var one = await adapterOne.sent
        var two = await adapterTwo.sent
        XCTAssertTrue(one.isEmpty)
        XCTAssertEqual(two.count, 1)

        await adapterTwo.clearSent()
        await adapterOne.dropPeer(peerOne)
        await repo.send(ephemeral(to: peerOne), to: peerOne)
        one = await adapterOne.sent
        two = await adapterTwo.sent
        XCTAssertTrue(one.isEmpty)
        XCTAssertEqual(two.count, 1, "the stale connection's disconnect must not drop the replacement's route")
    }

    func testPeerReportedWithoutAnAdapterHasNoRoute() async throws {
        let anonymous: PEER_ID = "peer-anonymous"
        await adapterOne.peerAnonymously(with: anonymous)
        let peers = await repo.peers()
        XCTAssertTrue(peers.contains(anonymous), "the repo still learns about the peer")

        await repo.send(ephemeral(to: anonymous), to: anonymous)
        let one = await adapterOne.sent
        let two = await adapterTwo.sent
        XCTAssertTrue(one.isEmpty)
        XCTAssertTrue(two.isEmpty)
    }
}
