// import AsyncAlgorithms
internal import Automerge
internal import struct Foundation.Data
internal import OSLog
internal import PotentCBOR

// riff
// https://github.com/automerge/automerge-repo/blob/main/packages/automerge-repo/src/network/NetworkSubsystem.ts

/// A type that hosts network subsystems to connect to peers.
///
/// The NetworkSubsystem instance is responsible for setting up and configuring any network providers, and responding to
/// messages from remote peers after the connection has been established. The connection handshake and peer negotiation
/// is
/// the responsibility of the network provider instance.
@AutomergeRepo
final class NetworkSubsystem {
    // TODO: When swift allows, switch this to a class that's locked to the same local actor
    // as Repo

    // a list of documents with a pending request for a documentId
    var requestedDocuments: [DocumentId: [PEER_ID]] = [:]

    public static let encoder = CBOREncoder()
    public static let decoder = CBORDecoder()

    // repo is a weak var to avoid a retain cycle - a network subsystem is
    // (so far) always created with a Repo and uses it for remote data storage of documents that
    // it fetches, syncs, or gossips about.
    //
    // TODO: revisit this and consider if the callbacks to repo should be exposed as a delegate
    weak var repo: Repo?
    var adapters: [any NetworkProvider]
    /// The adapter that reached each peer, so a message addressed to a peer goes out only on that adapter.
    ///
    /// Populated when an adapter reports `.ready` or `.peerCandidate` for a peer, and cleared when it reports
    /// `.peerDisconnect` for that peer or `.close`. Mirrors `#adaptersByPeer` in the JS `NetworkSubsystem`.
    private(set) var adaptersByPeer: [PEER_ID: any NetworkProvider] = [:]
    var loglevel: LogVerbosity

    nonisolated init(verbosity: LogVerbosity) {
        adapters = []
        self.loglevel = verbosity
    }

    /// Identity comparison for two adapters; every shipped provider is a class.
    private static func isSame(_ lhs: any NetworkProvider, _ rhs: any NetworkProvider) -> Bool {
        (lhs as AnyObject) === (rhs as AnyObject)
    }

    func setRepo(_ repo: Repo) {
        self.repo = repo
    }

    func setLogVerbosity(_ level: LogVerbosity) {
        self.loglevel = level
    }

    func addAdapter(adapter: some NetworkProvider) {
        guard let peerId = repo?.peerId else {
            fatalError("NO REPO CONFIGURED WHEN ADDING ADAPTERS")
        }
        adapter.setDelegate(self, as: peerId, with: repo?.localPeerMetadata)
        adapters.append(adapter)
    }

    /// Stops routing through `adapter`, disconnects it, and forgets the peers it reached.
    ///
    /// The adapter leaves the list first, so nothing it reports while disconnecting is acted on; the peers
    /// it owned routes for are then removed from the repo directly. Removing an unknown adapter is a no-op.
    func removeAdapter(adapter: any NetworkProvider) async {
        guard let index = adapters.firstIndex(where: { Self.isSame($0, adapter) }) else {
            if loglevel.canTrace() {
                Logger.network.trace("REPONET: ignoring removal of unknown adapter \(String(describing: adapter))")
            }
            return
        }
        adapters.remove(at: index)
        if loglevel.canTrace() {
            Logger.network.trace("REPONET: removing adapter \(String(describing: adapter))")
        }
        await adapter.disconnect()
        let orphaned = adaptersByPeer.filter { Self.isSame($0.value, adapter) }.map(\.key)
        for peerId in orphaned {
            if loglevel.canTrace() {
                Logger.network.trace("REPONET: dropping route for peer \(peerId), adapter removed")
            }
            adaptersByPeer.removeValue(forKey: peerId)
            repo?.removePeer(peer: peerId)
        }
    }

    private func isKnown(_ adapter: any NetworkProvider) -> Bool {
        adapters.contains { Self.isSame($0, adapter) }
    }

    func startRemoteFetch(id: DocumentId) async throws {
        // attempt to fetch the provided document Id from all (current) peers, returning the document
        // or returning nil if the document is unavailable.
        // Save the throwing scenarios for failures in connection, etc.
        guard let repo else {
            // invariant that there should be a valid doc handle available from the repo
            throw Errors.Unavailable(id: id)
        }
        if loglevel.canTrace() {
            Logger.network.trace("REPONET: Initiating remote fetch for \(id)")
        }
        let newDocument = Document()
        requestedDocuments[id] = []
        for adapter in adapters {
            for peerConnection in adapter.peeredConnections {
                if loglevel.canTrace() {
                    Logger.network
                        .trace(
                            "REPONET: requesting \(id) from peer \(peerConnection.peerId) at \(peerConnection.endpoint)"
                        )
                }
                // upsert the requested document into the list by peer
                if var existingList = requestedDocuments[id] {
                    existingList.append(peerConnection.peerId)
                    requestedDocuments[id] = existingList
                }
                // get a current sync state (creating one if needed for a fresh sync)
                let syncState = repo.syncState(id: id, peer: peerConnection.peerId)

                if let syncRequestData = newDocument.generateSyncMessage(state: syncState) {
                    await adapter.send(message: .request(SyncV1Msg.RequestMsg(
                        documentId: id.description,
                        senderId: repo.peerId,
                        targetId: peerConnection.peerId,
                        sync_message: syncRequestData
                    )), to: peerConnection.peerId)
                }
            }
        }
        guard let allRequestedPeers = requestedDocuments[id], !allRequestedPeers.isEmpty else {
            Logger.network
                .error("Finishing remote fetch without any active network requests to peers, reporting Unavailable")
            throw Errors.Unavailable(id: id)
        }
        if loglevel.canTrace() {
            Logger.network.trace("REPONET: remote fetch for \(id) finished")
        }
    }

    /// Sends a message to a peer, or to every adapter when no peer is given.
    ///
    /// A message addressed to a peer goes out only on the adapter that reached that peer. When no adapter has
    /// reported that peer, the message is logged and dropped rather than broadcast, matching the JS library.
    func send(message: SyncV1Msg, to: PEER_ID?) async {
        if let to {
            guard let adapter = adaptersByPeer[to] else {
                Logger.network
                    .warning(
                        "REPONET: no adapter is peered with \(to, privacy: .public), dropping \(message.debugDescription, privacy: .public)"
                    )
                return
            }
            if loglevel.canTrace() {
                Logger.network
                    .trace("REPONET: sending message on \(String(describing: adapter)) to \(to)")
            }
            await adapter.send(message: message, to: to)
            return
        }
        for adapter in adapters {
            if loglevel.canTrace() {
                Logger.network
                    .trace("REPONET: broadcasting message on \(String(describing: adapter))")
            }
            await adapter.send(message: message, to: nil)
        }
    }

    // MARK: Routing table maintenance

    private func recordPeer(_ peerId: PEER_ID, on adapter: any NetworkProvider) {
        if loglevel.canTrace() {
            Logger.network.trace("REPONET: routing peer \(peerId) through \(String(describing: adapter))")
        }
        adaptersByPeer[peerId] = adapter
    }

    /// Forgets the route for a peer. When the reporting adapter is known, only its own route is dropped, so a
    /// stale connection closing after a replacement has peered with the same peer leaves the new route alone.
    private func forgetPeer(_ peerId: PEER_ID, on adapter: (any NetworkProvider)?) {
        guard let current = adaptersByPeer[peerId] else { return }
        if let adapter, !Self.isSame(current, adapter) {
            if loglevel.canTrace() {
                Logger.network
                    .trace("REPONET: ignoring disconnect of \(peerId) from \(String(describing: adapter)), routed elsewhere")
            }
            return
        }
        if loglevel.canTrace() {
            Logger.network.trace("REPONET: dropping route for peer \(peerId)")
        }
        adaptersByPeer.removeValue(forKey: peerId)
    }

    private func forgetAllPeers(on adapter: any NetworkProvider) {
        let peers = adaptersByPeer.filter { Self.isSame($0.value, adapter) }.map(\.key)
        for peerId in peers {
            if loglevel.canTrace() {
                Logger.network.trace("REPONET: dropping route for peer \(peerId), adapter closed")
            }
            adaptersByPeer.removeValue(forKey: peerId)
        }
    }
}

extension NetworkSubsystem: NetworkEventReceiver {
    // Collection point for messages coming in from all network adapters.
    // The network subsystem forwards messages from network peers to the relevant places,
    // and forwards messages out to peers as needed
    //
    // In automerge-repo code, it appears to update information on an ephemeral information (
    // a sort of middleware) before emitting it upwards.
    public func receiveEvent(event: NetworkAdapterEvents) async {
        await handle(event: event, from: nil)
    }

    public func receiveEvent(event: NetworkAdapterEvents, from adapter: any NetworkProvider) async {
        await handle(event: event, from: adapter)
    }

    /// Handles an event from an adapter. Without the adapter, the routing table isn't updated, so messages
    /// addressed to peers the anonymous adapter reports are dropped by ``send(message:to:)``. Events from an
    /// adapter that has been removed are ignored.
    private func handle(event: NetworkAdapterEvents, from adapter: (any NetworkProvider)?) async {
        if loglevel.canTrace() {
            Logger.network.trace("REPONET: received event from network adapter: \(event.debugDescription)")
        }
        if let adapter, !isKnown(adapter) {
            if loglevel.canTrace() {
                Logger.network
                    .trace("REPONET: ignoring \(event.debugDescription) from removed adapter \(String(describing: adapter))")
            }
            return
        }
        guard let repo else {
            // No-op if there's no repo to update state or handle
            // further message passing
            return
        }
        switch event {
        case let .ready(payload):
            if let adapter {
                recordPeer(payload.peerId, on: adapter)
            }
            await repo.addPeerWithMetadata(peer: payload.peerId, metadata: payload.peerMetadata)
        case .close:
            if let adapter {
                forgetAllPeers(on: adapter)
            }
        // attempt to reconnect, or remove from active adapters?
        case let .peerCandidate(payload):
            if let adapter {
                recordPeer(payload.peerId, on: adapter)
            }
            await repo.addPeerWithMetadata(peer: payload.peerId, metadata: payload.peerMetadata)
        case let .peerDisconnect(payload):
            forgetPeer(payload.peerId, on: adapter)
            repo.removePeer(peer: payload.peerId)
        case let .message(payload):
            switch payload {
            case .peer, .join, .leave, .unknown:
                // ERROR FOR THESE MSG TYPES - expected to be handled at adapter
                Logger.network
                    .error(
                        "REPONET: Unexpected message type received by network subsystem: \(payload.debugDescription, privacy: .public)"
                    )
                #if DEBUG
                fatalError("UNEXPECTED MSG")
                #endif
            case let .error(errorMsg):
                Logger.network
                    .warning(
                        "REPONET: Error message received by network subsystem: \(errorMsg.debugDescription, privacy: .public)"
                    )
            case let .request(requestMsg):
                await repo.handleRequest(msg: requestMsg)
            case let .sync(syncMsg):
                await repo.handleSync(msg: syncMsg)
            case let .unavailable(unavailableMsg):
                guard let docId = DocumentId(unavailableMsg.documentId) else {
                    Logger.network
                        .error(
                            "REPONET: Invalid message Id \(unavailableMsg.documentId, privacy: .public) in unavailable msg: \(unavailableMsg.debugDescription, privacy: .public)"
                        )
                    return
                }
                if loglevel.canTrace() {
                    Logger.network.trace("REPONET: Received \(event.debugDescription) event")
                }
                if let peersRequested = requestedDocuments[docId] {
                    if loglevel.canTrace() {
                        Logger.network.trace("REPONET: We've requested \(docId) from \(peersRequested.count) peers:")
                        for p in peersRequested {
                            Logger.network.trace("REPONET: - Peer: \(p)")
                        }
                    }
                    // if we receive an unavailable from one peer, record it and wait until
                    // we receive unavailable from all available peers before marking it unavailable
                    let remainingPeersPending = peersRequested.filter { peerId in
                        // include the peers OTHER than the one sending the unavailable msg
                        peerId != unavailableMsg.senderId
                    }
                    if loglevel.canTrace() {
                        Logger.network
                            .trace(
                                "REPONET: Removing the sending peer, there are \(remainingPeersPending.count) remaining:"
                            )

                        for p in remainingPeersPending {
                            Logger.network.trace("REPONET: - Peer: \(p)")
                        }
                    }
                    if remainingPeersPending.isEmpty {
                        if loglevel.canTrace() {
                            Logger.network
                                .trace(
                                    "REPONET: No further peers with requests outstanding, so marking document \(docId) as unavailable"
                                )
                        }
                        await repo.markDocUnavailable(id: docId)
                        requestedDocuments.removeValue(forKey: docId)
                    } else {
                        // handle the scenario where we started with more adapters but
                        // lost a connection...

                        var currentConnectedPeers: [PEER_ID] = []
                        for adapter in adapters {
                            let connectedPeers: [PEER_ID] = adapter.peeredConnections
                                .map { peerConnection in
                                    peerConnection.peerId
                                }
                            currentConnectedPeers.append(contentsOf: connectedPeers)
                        }
                        let stillPending = remainingPeersPending.compactMap { peerId in
                            if currentConnectedPeers.contains(peerId) {
                                peerId
                            } else {
                                nil
                            }
                        }
                        // save the data back for other adapters to respond later...
                        requestedDocuments[docId] = stillPending
                    }
                } else {
                    // no peers are waiting to hear about a requested document, ignore
                    return
                }
            case let .ephemeral(ephemeralMsg):
                await repo.handleEphemeralMessage(ephemeralMsg)
            case let .remoteSubscriptionChange(remoteSubscriptionChangeMsg):
                Logger.network
                    .error(
                        "REPONET: UNIMPLEMENTED EPHEMERAL MESSAGE PASSING: \(remoteSubscriptionChangeMsg.debugDescription, privacy: .public)"
                    )
            case let .remoteHeadsChanged(remoteHeadsChangedMsg):
                Logger.network
                    .error(
                        "REPONET: UNIMPLEMENTED EPHEMERAL MESSAGE PASSING: \(remoteHeadsChangedMsg.debugDescription, privacy: .public)"
                    )
            }
        }
    }
}
