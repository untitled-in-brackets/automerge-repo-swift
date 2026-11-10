public import struct Automerge.ChangeHash
public import struct Automerge.Patch

/// A change the repo applied to a document on behalf of someone other than the caller: a sync
/// message from a peer, or a merge the repo performed.
///
/// Writes made directly to a handle's document are not reported; the caller made those. The repo
/// publishes these through ``Repo/documentChangePublisher`` and ``Repo/documentChanges(for:)``.
///
/// `Patch`, `PatchAction`, `PathElement` and `DeleteSeq` in automerge-swift are not yet declared
/// `Sendable`, although every stored property of theirs is a `Sendable` value type. The conformance
/// is unchecked until those four declarations gain it; drop `@unchecked` when they do.
public struct DocumentChange: @unchecked Sendable {
    /// Who the change came from.
    public enum Source: Sendable, Equatable {
        /// Applied from a sync message sent by `peer`.
        case sync(peer: PEER_ID)
        /// Applied by ``Repo/merge(id:with:)`` or by ``Repo/import(handle:)`` into a document the repo already held.
        case merge
    }

    /// The document that changed.
    public let id: DocumentId
    /// The patches the application produced, in order. Never empty.
    public let patches: [Patch]
    /// The document's heads immediately after the application.
    ///
    /// A cursor, not a transaction boundary: a local commit can land between the application and the
    /// read, in which case it is included here but not in `patches`.
    public let heads: Set<ChangeHash>
    /// Who the change came from.
    public let source: Source

    init(id: DocumentId, patches: [Patch], heads: Set<ChangeHash>, source: Source) {
        self.id = id
        self.patches = patches
        self.heads = heads
        self.source = source
    }
}
