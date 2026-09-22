@testable import AutomergeRepo
import XCTest

final class WebSocketProviderReconnectTests: XCTestCase {
    func testRejectionRetryability() {
        for status in [400, 401, 403, 404, 409, 426] {
            XCTAssertFalse(Errors.ConnectionRejected(statusCode: status).isRetryable, "\(status)")
        }
        for status in [408, 425, 429, 500, 502, 503, 504] {
            XCTAssertTrue(Errors.ConnectionRejected(statusCode: status).isRetryable, "\(status)")
        }
    }

    /// The wait races the delay against the network path *becoming* satisfied; a path that is already
    /// satisfied when the wait starts must not end it early.
    func testReconnectWaitIsNotCutShortByTheCurrentPath() async throws {
        let elapsed = try await ContinuousClock().measure {
            try await WebSocketProvider.waitToReconnect(seconds: 1)
        }
        XCTAssertGreaterThanOrEqual(elapsed, .milliseconds(900))
    }

    func testRetryInitialConnectRequiresReconnectOnError() {
        XCTAssertFalse(
            WebSocketProviderConfiguration(reconnectOnError: false, retryInitialConnect: true).retryInitialConnect
        )
        XCTAssertTrue(
            WebSocketProviderConfiguration(reconnectOnError: true, retryInitialConnect: true).retryInitialConnect
        )
    }
}
