@testable import AutomergeRepo
import XCTest

/// A handshake with a server that accepts the upgrade and never answers the join. Nothing but the
/// provider can end it: the async `receive()` does not return when the task awaiting it is
/// cancelled, only when its socket is.
final class WebSocketProviderCancellationTests: XCTestCase {
    private struct IgnoringReceiver: NetworkEventReceiver {
        func receiveEvent(event _: NetworkAdapterEvents) async {}
    }

    private var server: SilentWebSocketServer!
    private var url: URL!

    override func setUp() async throws {
        server = try SilentWebSocketServer()
        url = try await server.start()
    }

    override func tearDown() async throws {
        server.stop()
    }

    private func makeProvider(reconnectOnError: Bool) async -> WebSocketProvider {
        let provider = WebSocketProvider(.init(reconnectOnError: reconnectOnError))
        await provider.setDelegate(IgnoringReceiver(), as: "client", with: nil)
        return provider
    }

    private func waitUntil(
        _ what: String, within timeout: Duration = .seconds(3), _ condition: @Sendable () async -> Bool
    ) async throws {
        let deadline = ContinuousClock.now + timeout
        while await !condition() {
            guard ContinuousClock.now < deadline else {
                return XCTFail("Timed out waiting until \(what)")
            }
            try await Task.sleep(for: .milliseconds(10))
        }
    }

    func testCancelledConnectThrowsCancellationAndLeavesDisconnected() async throws {
        let provider = await makeProvider(reconnectOnError: true)
        let url = try XCTUnwrap(url)
        let connect = Task { try await provider.connect(to: url) }
        // the join is sent, and the provider is waiting for the reply
        try await waitUntil("the provider is mid-handshake") { await provider.state == .connected }

        let cancelledAt = ContinuousClock.now
        connect.cancel()
        do {
            try await connect.value
            XCTFail("connect returned normally")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("connect threw \(error), not CancellationError")
        }
        XCTAssertLessThan(ContinuousClock.now - cancelledAt, .seconds(1))

        let state = await provider.state
        XCTAssertEqual(state, .disconnected)
        // a cancel is final: nothing retries in the background
        let reconnecting = await provider.ongoingReceiveMessageTask
        XCTAssertNil(reconnecting)
        let peers = await provider.peeredConnections
        XCTAssertTrue(peers.isEmpty)
    }

    func testConnectOnACancelledTaskOpensNoSocket() async throws {
        let provider = await makeProvider(reconnectOnError: true)
        let url = try XCTUnwrap(url)
        let connect = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            try await provider.connect(to: url)
        }
        do {
            try await connect.value
            XCTFail("connect returned normally")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("connect threw \(error), not CancellationError")
        }

        let state = await provider.state
        XCTAssertEqual(state, .disconnected)
        XCTAssertEqual(server.connectionCount, 0)
    }

    func testSilentServerTimesOutTheHandshake() async throws {
        let provider = await makeProvider(reconnectOnError: false)
        let url = try XCTUnwrap(url)
        let startedAt = ContinuousClock.now
        do {
            try await provider.connect(to: url)
            XCTFail("connect returned normally")
        } catch is Errors.Timeout {
            // expected
        } catch {
            XCTFail("connect threw \(error), not Errors.Timeout")
        }
        let elapsed = ContinuousClock.now - startedAt
        XCTAssertGreaterThanOrEqual(elapsed, .seconds(3.4))
        XCTAssertLessThan(elapsed, .seconds(5))

        let state = await provider.state
        XCTAssertEqual(state, .disconnected)
    }
}
