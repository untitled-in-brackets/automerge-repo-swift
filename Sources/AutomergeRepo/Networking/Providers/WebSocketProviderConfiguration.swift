public import Automerge

/// The configuration options for a WebSocket network provider.
public struct WebSocketProviderConfiguration: Sendable {
    /// A Boolean value that indicates if the provider should attempt to reconnect when it fails with an error.
    public let reconnectOnError: Bool
    /// A Boolean value that indicates if a failed initial connection should keep retrying in the background.
    ///
    /// Requires ``reconnectOnError``. When set, `connect` returns normally after a retryable failure and the
    /// provider reports ``WebSocketProviderState/reconnecting`` until it peers or gives up.
    public let retryInitialConnect: Bool
    /// The maximum number of reconnections allowed before the WebSocket provider disconnects.
    ///
    /// If ``reconnectOnError`` is `false`, this value is ignored.
    /// If `nil`, the default, the WebSocketProvider does not enforce a maximum number of retries.
    public let maxNumberOfConnectRetries: Int?
    /// The verbosity of the logs sent to the unified logging system.
    public let logLevel: LogVerbosity

    /// The default configuration for the WebSocket network provider.
    ///
    /// In the default configuration:
    ///
    /// - `reconnectOnError` is `true`
    public static let `default` = WebSocketProviderConfiguration(reconnectOnError: true)

    /// Creates a new WebSocket network provider configuration instance.
    /// - Parameter reconnectOnError: A Boolean value that indicates if the provider should attempt to reconnect
    /// when it fails with an error.
    /// - Parameter loggingAt: The verbosity of the logs sent to the unified logging system.
    /// - Parameter maxNumberOfConnectRetries: The maximum number of reconnections allowed before the WebSocket provider disconnects. If `nil`, the default, retries continue forever.
    /// - Parameter retryInitialConnect: A Boolean value that indicates if a failed initial connection should keep
    /// retrying in the background. Ignored unless `reconnectOnError` is `true`.
    public init(
        reconnectOnError: Bool,
        loggingAt: LogVerbosity = .errorOnly,
        maxNumberOfConnectRetries: Int? = nil,
        retryInitialConnect: Bool = false
    ) {
        self.reconnectOnError = reconnectOnError
        self.maxNumberOfConnectRetries = maxNumberOfConnectRetries
        self.logLevel = loggingAt
        self.retryInitialConnect = retryInitialConnect && reconnectOnError
    }
}
