internal import Automerge
@preconcurrency public import Combine
public import Foundation
internal import Network
internal import OSLog

/// An Automerge-repo network provider that connects to other repositories using WebSocket.
@AutomergeRepo
public final class WebSocketProvider: NetworkProvider {
    /// The type that represents the endpoint that this provider connects with.
    public typealias NetworkConnectionEndpoint = URL

    /// The name of this provider.
    public let name = "WebSocket"

    /// A type that represents the configuration used to create the provider.
    public typealias ProviderConfiguration = WebSocketProviderConfiguration

    /// The active connection for this provider.
    public var peeredConnections: [PeerConnectionInfo]
    var delegate: (any NetworkEventReceiver)?
    var peerId: PEER_ID?
    var peerMetadata: PeerMetadata?
    var webSocketTask: URLSessionWebSocketTask?
    var ongoingReceiveMessageTask: Task<Void, any Error>?
    var config: WebSocketProviderConfiguration
    // reconnection logic variables
    /// Builds the request for each connection attempt, so credentials are minted at connect time.
    var endpoint: (@Sendable () async throws -> URLRequest)?
    var peered: Bool

    /// A connection that held at least this long earns a fresh backoff schedule when it drops.
    static let stableConnectionDuration: Duration = .seconds(30)
    /// The longest wait between reconnection attempts.
    static let maximumReconnectDelaySeconds = 30

    private let _statePublisher: CurrentValueSubject<WebSocketProviderState, Never> =
        CurrentValueSubject(.disconnected)

    /// The current state of the WebSocket connection.
    public var state: WebSocketProviderState {
        _statePublisher.value
    }

    /// The refusal that stopped the provider, once it has stopped for one; nil while it may still connect.
    public private(set) var lastRejection: Errors.ConnectionRejected?

    /// A publisher that provides state updates for the WebSocket connection.
    ///
    /// The initial value provides the current state of the connecting in the WebSocket provider,
    /// with updates published when the state changes. ``WebSocketProviderState/reconnecting`` means the
    /// provider is between attempts; ``WebSocketProviderState/disconnected`` means it has stopped.
    public lazy var statePublisher: AnyPublisher<WebSocketProviderState, Never> = _statePublisher
        .removeDuplicates().eraseToAnyPublisher()

    /// Creates a new instance of a WebSocket network provider with the configuration you provide.
    /// - Parameter config: The configuration for the provider.
    public nonisolated init(_ config: WebSocketProviderConfiguration = .default) {
        self.config = config
        peeredConnections = []
        delegate = nil
        peerId = nil
        peerMetadata = nil
        webSocketTask = nil
        ongoingReceiveMessageTask = nil
        peered = false
    }

    /// Initiate an outgoing connection to a URL.
    ///
    /// Creates a default `URLRequest` from the `URL` you provide and creates a WebSocket connection with it.
    public func connect(to url: URL) async throws {
        try await connect(to: URLRequest(url: url))
    }

    /// Initiate an outgoing connection to a URL Request.
    ///
    /// Create a WebSocket connection with the `URLRequest` you provide. Reconnections reuse the same
    /// request; pass a request builder instead when it carries credentials that expire.
    public func connect(to request: URLRequest) async throws {
        try await connect { request }
    }

    /// Initiate an outgoing connection, building the request freshly for every attempt.
    ///
    /// `makeRequest` runs before the initial connection and before each reconnection, so an
    /// `Authorization` header it sets is never replayed after it expires. An error it throws counts as a
    /// failed attempt.
    ///
    /// With `reconnectOnError`, a first attempt that fails in a way a retry could fix is treated like any
    /// other drop: the provider keeps trying in the background and reports
    /// ``WebSocketProviderState/reconnecting``. Otherwise a failed first attempt throws. Calling this
    /// while a reconnection is pending attempts immediately instead of waiting out the backoff; while
    /// peered or mid-handshake it does nothing.
    public func connect(to makeRequest: @escaping @Sendable () async throws -> URLRequest) async throws {
        switch state {
        case .ready, .connected:
            Logger.websocket.info("WEBSOCKET: connect ignored, already \(String(describing: self.state), privacy: .public)")
            return
        case .reconnecting:
            // the loop only touches state once it has confirmed it wasn't cancelled
            ongoingReceiveMessageTask?.cancel()
            ongoingReceiveMessageTask = nil
        case .disconnected:
            break
        }

        guard peerId != nil, delegate != nil else {
            Logger.websocket.error("Attempting to connect before connected to a delegate")
            throw Errors.NetworkProviderError(msg: "Attempting to connect before connected to a delegate")
        }

        endpoint = makeRequest
        lastRejection = nil
        do {
            let request = try await makeRequest()
            guard try await attemptConnect(to: request) else {
                if config.logLevel.canTrace(), let url = request.url {
                    Logger.websocket.trace("WEBSOCKET: failed to connect to \(url)")
                }
                return
            }
            if config.logLevel.canTrace(), let url = request.url {
                Logger.websocket.trace("WEBSOCKET: connected to \(url)")
            }
        } catch {
            guard config.reconnectOnError, Self.isRetryable(error), !Task.isCancelled else {
                endpoint = nil
                lastRejection = error as? Errors.ConnectionRejected
                _statePublisher.send(.disconnected)
                throw error
            }
            Logger.websocket.warning(
                "WEBSOCKET: initial connection failed, retrying in the background: \(error.localizedDescription, privacy: .public)"
            )
            _statePublisher.send(.reconnecting)
            startReceiveLoop(reconnectAttempts: 1)
            return
        }

        assert(peered == true)
        startReceiveLoop(reconnectAttempts: 0)
    }

    /// Disconnect and terminate any existing connection.
    public func disconnect() async {
        ongoingReceiveMessageTask?.cancel()
        await tearDown()
    }

    /// Reads messages and reconnects "out of band". A loop left over from a dropped connection keeps
    /// reading from whatever socket `connect` peers next, so at most one runs.
    private func startReceiveLoop(reconnectAttempts: UInt) {
        guard ongoingReceiveMessageTask == nil else { return }
        ongoingReceiveMessageTask = Task.detached {
            await self.ongoingReceiveWebSocketMessages(reconnectAttempts: reconnectAttempts)
            if await self.config.logLevel.canTrace() {
                Logger.websocket.trace("Terminated background read loop - socket expected to be disconnected")
            }
        }
    }

    /// Resets the provider and tells the delegate the peer is gone. Shared by `disconnect()` and the
    /// receive loop when it stops on its own.
    private func tearDown() async {
        peered = false
        webSocketTask?.cancel(with: .normalClosure, reason: nil)
        webSocketTask = nil
        ongoingReceiveMessageTask = nil
        endpoint = nil
        _statePublisher.send(.disconnected)

        if let connectedPeer = peeredConnections.first {
            peeredConnections.removeAll()
            await delegate?.receiveEvent(event: .peerDisconnect(payload: .init(peerId: connectedPeer.peerId)))
        }

        await delegate?.receiveEvent(event: .close)
    }

    /// Whether a later attempt could succeed. Only an HTTP rejection of the upgrade can say no.
    private static func isRetryable(_ error: any Error) -> Bool {
        (error as? Errors.ConnectionRejected)?.isRetryable ?? true
    }

    /// Requests the network transport to send a message.
    /// - Parameter message: The message to send.
    /// - Parameter to: An option peerId to identify the recipient for the message. If nil, the message is sent to all
    /// connected peers.
    public func send(message: SyncV1Msg, to: PEER_ID?) async {
        guard let webSocketTask else {
            Logger.websocket.warning("WEBSOCKET: Attempt to send a message without a connection")
            if config.logLevel.canTrace() {
                Logger.websocket.trace("WEBSOCKET: - msg \(message.debugDescription) to peer \(String(describing: to))")
            }
            return
        }
        var msgToSend = message
        if let peer = peerId {
            msgToSend = message.setTarget(to ?? peer)
        } else {
            Logger.websocket.warning("WEBSOCKET: No peer set to revise targeting of broadcast events")
        }
        do {
            if config.logLevel.canTrace() {
                Logger.websocket.trace("WEBSOCKET: SEND \(msgToSend.debugDescription)")
            }
            let data = try SyncV1Msg.encode(msgToSend)
            try await webSocketTask.send(.data(data))
        } catch {
            Logger.websocket
                .error("WEBSOCKET: Unable to encode and send message: \(error.localizedDescription, privacy: .public)")
        }
    }

    /// Set the delegate for the peer to peer provider.
    /// - Parameters:
    ///   - delegate: The delegate instance.
    ///   - peer: The peer ID to use for the peer to peer provider.
    ///   - metadata: The peer metadata, if any, to use for the peer to peer provider.
    ///
    /// This is typically called when the delegate adds the provider, and provides this network
    /// provider with a peer ID and associated metadata, as well as an endpoint that receives
    /// Automerge sync protocol sync message and network events.
    public func setDelegate(
        _ delegate: any NetworkEventReceiver,
        as peer: PEER_ID,
        with metadata: PeerMetadata?
    ) {
        self.delegate = delegate
        peerId = peer
        peerMetadata = metadata
    }

    // MARK: utility methods

    private func attemptToDecode(_ msg: URLSessionWebSocketTask.Message, peerOnly: Bool = false) throws -> SyncV1Msg {
        // Now that we have the WebSocket message, figure out if we got what we expected.
        // For the sync protocol handshake phase, it's essentially "peer or die" since
        // we were the initiating side of the connection.
        switch msg {
        case let .data(raw_data):
            if peerOnly {
                let msg = SyncV1Msg.decodePeer(raw_data)
                if case .peer = msg {
                    return msg
                } else {
                    // In the handshake phase and received anything other than a valid peer message
                    let decodeAttempted = SyncV1Msg.decode(raw_data)
                    Logger.websocket
                        .warning(
                            "WEBSOCKET: Decoding message, expecting peer only - and it wasn't a peer message. RECEIVED MSG: \(String(describing: decodeAttempted))"
                        )
                    throw Errors.UnexpectedMsg(msg: String(describing: decodeAttempted))
                }
            } else {
                let decodedMsg = SyncV1Msg.decode(raw_data)
                if case .unknown = decodedMsg {
                    Logger.websocket.warning("Unexpected message: \(decodedMsg.debugDescription)")
                    throw Errors.UnexpectedMsg(msg: decodedMsg.debugDescription)
                }
                return decodedMsg
            }

        case let .string(string):
            // In the handshake phase and received anything other than a valid peer message
            Logger.websocket
                .warning("WEBSOCKET: Unknown message received: .string(\(string))")
            throw Errors.UnexpectedMsg(msg: string)

        @unknown default:
            // In the handshake phase and received anything other than a valid peer message
            Logger.websocket
                .error("WEBSOCKET: Unknown message received: \(String(describing: msg))")
            throw Errors.UnexpectedMsg(msg: String(describing: msg))
        }
    }

    // Returns a `true` on success OR throws an error (log the error, but can retry)
    private func attemptConnect(to request: URLRequest?) async throws -> Bool {
        precondition(peered == false)
        guard let request,
              let url = request.url,
              let peerId,
              let delegate
        else {
            if config.logLevel.canTrace() {
                Logger.websocket.trace("Pre-requisites not available for attemptConnect, returning nil")
                Logger.websocket.trace("URL: \(String(describing: request?.url))")
                Logger.websocket.trace("PeerID: \(String(describing: self.peerId))")
                Logger.websocket.trace("Delegate: \(String(describing: self.delegate))")
            }
            return false
        }

        // establish the WebSocket connection

        let webSocketTask = URLSession.shared.webSocketTask(with: request)
        if config.logLevel.canTrace() {
            Logger.websocket.trace("WEBSOCKET: Activating websocket to \(url, privacy: .public)")
        }
        // start the websocket processing things
        webSocketTask.resume()

        // since we initiated the WebSocket, it's on us to send an initial 'join'
        // protocol message to start the handshake phase of the protocol
        let joinMessage = SyncV1Msg.JoinMsg(senderId: peerId, metadata: peerMetadata)
        let data = try SyncV1Msg.encode(joinMessage)
        do {
            try await webSocketTask.send(.data(data))
        } catch {
            webSocketTask.cancel()
            // a refused upgrade fails the first send; the task still holds the HTTP response
            if let status = (webSocketTask.response as? HTTPURLResponse)?.statusCode {
                Logger.websocket.error("WEBSOCKET: \(url.absoluteString, privacy: .public) refused the upgrade: HTTP \(status)")
                throw Errors.ConnectionRejected(statusCode: status)
            }
            throw error
        }
        _statePublisher.send(.connected)
        do {
            // Race a timeout against receiving a Peer message from the other side
            // of the WebSocket connection. If we fail that race, shut down the connection
            // and move into a .closed connectionState
            let websocketMsg = try await nextMessage(on: webSocketTask, withTimeout: .seconds(3.5))

            // Now that we have the WebSocket message, figure out if we got what we expected.
            // For the sync protocol handshake phase, it's essentially "peer or die" since
            // we were the initiating side of the connection.
            guard case let .peer(peerMsg) = try attemptToDecode(websocketMsg, peerOnly: true) else {
                Logger.websocket.warning("Unexpected message: \(String(describing: websocketMsg))")
                throw Errors.UnexpectedMsg(msg: String(describing: websocketMsg))
            }
            if config.logLevel.canTrace() {
                Logger.websocket.trace("WEBSOCKET: RECV: \(peerMsg.debugDescription)")
            }
            peered = true
            let peerConnectionDetails = PeerConnectionInfo(
                peerId: peerMsg.senderId,
                peerMetadata: peerMsg.peerMetadata,
                endpoint: url.absoluteString,
                initiated: true,
                peered: peered
            )
            peeredConnections = [peerConnectionDetails]
            // set _before_ the delegate hears we're peered, because that (can) trigger a sync
            self.webSocketTask = webSocketTask

            await delegate.receiveEvent(event: .ready(payload: peerConnectionDetails))
            _statePublisher.send(.ready)
            if config.logLevel.canTrace() {
                Logger.websocket.trace("WEBSOCKET: Peered to targetId: \(peerMsg.senderId) \(peerMsg.debugDescription)")
            }
        } catch {
            // Only this attempt's socket is ours to clean up; shared state, reconnection and the
            // published state are the caller's, and another connect() may have peered meanwhile.
            Logger.websocket
                .error(
                    "WEBSOCKET: Failed to peer with \(url.absoluteString, privacy: .public): \(error.localizedDescription, privacy: .public)"
                )
            webSocketTask.cancel()
            throw error
        }

        return true
    }

    // throw error on timeout
    // throw error on cancel
    // otherwise return the msg
    private nonisolated func nextMessage(
        on webSocketTask: URLSessionWebSocketTask,
        withTimeout: ContinuousClock.Instant
            .Duration?
    ) async throws -> URLSessionWebSocketTask.Message {
        // nil on timeout means we apply a default - 3.5 seconds, this setup keeps
        // the signature that _demands_ a timeout in the face of the developer (me)
        // who otherwise forgets its there.
        let timeout: ContinuousClock.Instant.Duration = if let providedTimeout = withTimeout {
            providedTimeout
        } else {
            .seconds(3.5)
        }

        // Co-operatively check to see if we're cancelled, and if so - we can bail out before
        // going into the receive loop.
        try Task.checkCancellation()

        // Race a timeout against receiving a Peer message from the other side
        // of the WebSocket connection. If we fail that race, shut down the connection
        // and move into a .closed connectionState
        let websocketMsg = try await withThrowingTaskGroup(of: URLSessionWebSocketTask.Message.self) { group in
            group.addTask {
                // retrieve the next websocket message
                try await webSocketTask.receive()
            }

            group.addTask {
                // Race against the receive call with a continuous timer
                try await Task.sleep(for: timeout)
                if await self.config.logLevel.canTrace() {
                    Logger.websocket.trace("WEBSOCKET: TIMEOUT \(timeout) waiting for next messsage")
                }
                throw Errors.Timeout()
            }

            guard let msg = try await group.next() else {
                if await self.config.logLevel.canTrace() {
                    Logger.websocket.trace("WEBSOCKET: throwing CancellationError")
                }
                throw CancellationError()
            }
            // cancel all ongoing tasks (the websocket receive request, in this case)
            group.cancelAll()
            return msg
        }
        return websocketMsg
    }

    /// Loops over incoming messages from the websocket and updates the state machine based on the messages
    /// received.
    ///
    /// If the provider configuration (``WebSocketProviderConfiguration``) has `reconnectOnError`
    /// set to `true`, this function re-establishes the WebSocket connection after a connection failure or
    /// read error, building each request afresh from the endpoint closure and backing off between attempts.
    /// If that value is false, the loop ends on the first error.
    ///
    /// The loop also ends when the server refuses the upgrade with a status a retry cannot fix, or after
    /// `maxNumberOfConnectRetries` attempts when that is set. Either way the provider is reset and reports
    /// ``WebSocketProviderState/disconnected``; while it is between attempts it reports
    /// ``WebSocketProviderState/reconnecting``.
    ///
    /// - Parameter reconnectAttempts: Where the backoff schedule starts; non-zero when the initial
    /// connection already failed once.
    private func ongoingReceiveWebSocketMessages(reconnectAttempts: UInt) async {
        var reconnectAttempts = reconnectAttempts
        let reconnectOnError = config.reconnectOnError
        var peeredAt: ContinuousClock.Instant? = peered ? .now : nil
        var msgFromWebSocket: URLSessionWebSocketTask.Message?

        while true {
            msgFromWebSocket = nil

            // disconnect() already reset state before cancelling; connect() may have re-peered since
            if Task.isCancelled {
                break
            }

            if !peered, reconnectOnError {
                if let maxRetries = config.maxNumberOfConnectRetries, maxRetries > 0, reconnectAttempts >= maxRetries {
                    Logger.websocket.warning("WEBSOCKET: giving up after \(maxRetries) reconnect attempts")
                    break
                }

                _statePublisher.send(.reconnecting)
                let waitBeforeReconnect = min(
                    Backoff.delay(reconnectAttempts, withJitter: true), Self.maximumReconnectDelaySeconds
                )
                if config.logLevel.canTrace() {
                    Logger.websocket
                        .trace(
                            "WEBSOCKET: Reconnect attempt #\(reconnectAttempts), waiting for \(waitBeforeReconnect) seconds."
                        )
                }
                reconnectAttempts += 1

                do {
                    try await Self.waitToReconnect(seconds: waitBeforeReconnect)
                    let request = try await endpoint?()
                    // both waits suspended this actor: connect() may have superseded this loop or peered
                    try Task.checkCancellation()
                    if !peered, let request {
                        _ = try await attemptConnect(to: request)
                    }
                    if peered {
                        peeredAt = .now
                    }
                } catch let rejection as Errors.ConnectionRejected where !rejection.isRetryable {
                    if Task.isCancelled {
                        break
                    }
                    Logger.websocket.error("WEBSOCKET: refused with HTTP \(rejection.statusCode); not retrying")
                    lastRejection = rejection
                    break
                } catch {
                    if Task.isCancelled {
                        break
                    }
                    webSocketTask = nil
                    peered = false
                }
            }

            do {
                msgFromWebSocket = try await webSocketTask?.receive()
            } catch {
                if Task.isCancelled {
                    break
                }
                // error scenario with the WebSocket connection
                Logger.websocket.warning("WEBSOCKET: Error reading websocket: \(error.localizedDescription)")
                peered = false
                msgFromWebSocket = nil
                // a connection that held for a while starts the schedule over; a flapping one keeps climbing
                if let lastPeeredAt = peeredAt, .now - lastPeeredAt >= Self.stableConnectionDuration {
                    reconnectAttempts = 0
                }
                peeredAt = nil
                guard reconnectOnError else {
                    break
                }
                _statePublisher.send(.reconnecting)
            }

            if let encodedMessage = msgFromWebSocket {
                do {
                    let msg = try attemptToDecode(encodedMessage)
                    if config.logLevel.canTrace() {
                        Logger.websocket.trace("WEBSOCKET: RECV: \(msg.debugDescription)")
                    }
                    await handleMessage(msg: msg)
                } catch {
                    // catch decode failures, but don't terminate the whole shebang
                    // on a failure
                    Logger.websocket
                        .warning(
                            "WEBSOCKET: Unable to decode websocket message: \(error.localizedDescription, privacy: .public)"
                        )
                }
            }
        }

        if !Task.isCancelled {
            await tearDown()
        }
        Logger.websocket.warning("WEBSOCKET: receive and reconnect loop terminated")
    }

    /// Waits `seconds`, or less if the network path becomes satisfied in the meantime.
    nonisolated static func waitToReconnect(seconds: Int) async throws {
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await Task.sleep(for: .seconds(seconds))
            }

            group.addTask {
                let monitor = NWPathMonitor()
                var last: NWPath.Status?
                for await path in monitor.paths() {
                    // the first update is the current path, not a transition
                    if let last, last != .satisfied, path.status == .satisfied {
                        Logger.websocket.info("WEBSOCKET: Network path satisfied while waiting to reconnect")
                        return
                    }
                    last = path.status
                }
            }

            try await group.next()
            group.cancelAll()
        }
    }

    func handleMessage(msg: SyncV1Msg) async {
        // - .peer and .join messages should be handled here locally, and aren't expected
        //   in this method (all handling of them should happen before getting here)
        // - .leave invokes the disconnect, and associated messages to the delegate
        // - otherwise forward the message to the delegate to work with
        switch msg {
        case let .leave(msg):
            if config.logLevel.canTrace() {
                Logger.websocket.trace("WEBSOCKET: \(msg.senderId) requests to kill the connection")
            }
            await disconnect()
        case let .join(msg):
            Logger.websocket.error("WEBSOCKET: Unexpected message received: \(msg.debugDescription)")
        case let .peer(msg):
            Logger.websocket.error("WEBSOCKET: Unexpected message received: \(msg.debugDescription)")
        default:
            await delegate?.receiveEvent(event: .message(payload: msg))
            if config.logLevel.canTrace() {
                Logger.websocket.trace("WEBSOCKET: FWD TO DELEGATE: \(msg.debugDescription)")
            }
        }
    }
}
