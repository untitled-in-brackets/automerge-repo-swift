import Foundation
import Network

/// A local listener that accepts WebSocket upgrades and then never sends anything, which is how a
/// server that takes the join and does not answer looks to a client. With `replyToFirstMessage` set
/// it answers each client's first message with that frame, then stays silent.
final class SilentWebSocketServer: @unchecked Sendable {
    private let listener: NWListener
    private let queue = DispatchQueue(label: "SilentWebSocketServer")
    private let replyToFirstMessage: Data?
    // only touched on `queue`
    private var connections: [NWConnection] = []

    init(replyToFirstMessage: Data? = nil) throws {
        self.replyToFirstMessage = replyToFirstMessage
        let parameters = NWParameters.tcp
        parameters.acceptLocalOnly = true
        parameters.defaultProtocolStack.applicationProtocols.insert(NWProtocolWebSocket.Options(), at: 0)
        listener = try NWListener(using: parameters)
    }

    /// How many clients have connected so far.
    var connectionCount: Int {
        queue.sync { connections.count }
    }

    /// Starts listening on a free port and returns the URL to connect to.
    func start() async throws -> URL {
        listener.newConnectionHandler = { [weak self] connection in
            guard let self else { return }
            connections.append(connection)
            connection.start(queue: queue)
            if let reply = replyToFirstMessage {
                connection.receiveMessage { _, _, _, error in
                    guard error == nil else { return }
                    let metadata = NWProtocolWebSocket.Metadata(opcode: .binary)
                    let context = NWConnection.ContentContext(identifier: "reply", metadata: [metadata])
                    connection.send(content: reply, contentContext: context, completion: .idempotent)
                }
            }
        }
        let port: NWEndpoint.Port = try await withCheckedThrowingContinuation { continuation in
            listener.stateUpdateHandler = { [listener] state in
                switch state {
                case .ready:
                    listener.stateUpdateHandler = nil
                    if let port = listener.port {
                        continuation.resume(returning: port)
                    } else {
                        continuation.resume(throwing: NWError.posix(.EADDRNOTAVAIL))
                    }
                case let .failed(error):
                    listener.stateUpdateHandler = nil
                    continuation.resume(throwing: error)
                default:
                    break
                }
            }
            listener.start(queue: queue)
        }
        return URL(string: "ws://127.0.0.1:\(port.rawValue)")!
    }

    func stop() {
        listener.cancel()
        queue.sync {
            for connection in connections {
                connection.cancel()
            }
            connections = []
        }
    }
}
