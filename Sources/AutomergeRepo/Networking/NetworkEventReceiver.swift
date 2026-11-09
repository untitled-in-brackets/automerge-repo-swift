/// A type that accepts provides a method for a Network Provider to call with network events.
///
/// Mostly commonly, this is a ``Repo`` instance, and describes the interface that a network provider
/// uses for its delegate callbacks.
@AutomergeRepo
public protocol NetworkEventReceiver: Sendable {
    /// Receive and process an event from a Network Provider.
    /// - Parameter event: The event to process.
    func receiveEvent(event: NetworkAdapterEvents) async

    /// Receive and process an event from a Network Provider, identifying the provider that sent it.
    /// - Parameters:
    ///   - event: The event to process.
    ///   - adapter: The network provider that emitted the event.
    ///
    /// Prefer this method when emitting events from a provider. The receiver uses the provider to route
    /// messages addressed to a peer back through the provider that reached that peer. The default
    /// implementation forwards to ``receiveEvent(event:)``.
    func receiveEvent(event: NetworkAdapterEvents, from adapter: any NetworkProvider) async
}

public extension NetworkEventReceiver {
    func receiveEvent(event: NetworkAdapterEvents, from _: any NetworkProvider) async {
        await receiveEvent(event: event)
    }
}
