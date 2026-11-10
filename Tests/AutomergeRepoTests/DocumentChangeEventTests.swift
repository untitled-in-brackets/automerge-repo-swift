import Automerge
@testable import AutomergeRepo
import Combine
import Foundation
import XCTest

/// Collects published changes from any isolation, so a test can inspect them after the fact.
final class ChangeCollector: @unchecked Sendable {
    private let lock = NSLock()
    private var _changes: [DocumentChange] = []
    private var cancellable: AnyCancellable?
    /// Runs inside the sink, on the repo's actor, with the change just received.
    var onChange: (@Sendable (DocumentChange) -> Void)?

    init(_ repo: Repo) {
        cancellable = repo.documentChangePublisher.sink { [weak self] change in
            guard let self else { return }
            lock.withLock { self._changes.append(change) }
            onChange?(change)
        }
    }

    var changes: [DocumentChange] {
        lock.withLock { _changes }
    }

    func waitForChanges(
        where predicate: @escaping ([DocumentChange]) -> Bool = { !$0.isEmpty },
        timeout: Duration = .seconds(5)
    ) async -> [DocumentChange] {
        let deadline = ContinuousClock.now + timeout
        while ContinuousClock.now < deadline {
            let current = changes
            if predicate(current) {
                return current
            }
            try? await Task.sleep(for: .milliseconds(20))
        }
        return changes
    }
}

extension [Patch] {
    /// Keys put on the root object by these patches.
    var rootKeysPut: Set<String> {
        var keys: Set<String> = []
        for patch in self {
            if case let .Put(obj, .Key(key), _) = patch.action, obj == .ROOT {
                keys.insert(key)
            }
        }
        return keys
    }
}

final class DocumentChangeEventTests: XCTestCase {
    let network = InMemoryNetwork.shared
    var repoA: Repo!
    var repoB: Repo!
    var adapterA: InMemoryNetworkEndpoint!
    var adapterB: InMemoryNetworkEndpoint!
    var changesOnA: ChangeCollector!
    var changesOnB: ChangeCollector!

    override func setUp() async throws {
        await network.resetTestNetwork()
        repoA = Repo(sharePolicy: SharePolicy.agreeable)
        adapterA = await network.createNetworkEndpoint(config: .init(listeningNetwork: false, name: "A"))
        await repoA.addNetworkAdapter(adapter: adapterA)

        repoB = Repo(sharePolicy: SharePolicy.agreeable)
        adapterB = await network.createNetworkEndpoint(config: .init(listeningNetwork: true, name: "B"))
        await repoB.addNetworkAdapter(adapter: adapterB)

        changesOnA = ChangeCollector(repoA)
        changesOnB = ChangeCollector(repoB)
    }

    private func connect() async throws {
        try await adapterA.connect(to: "B")
    }

    /// Polls until both repos hold `id` with equal heads.
    private func waitUntilInSync(_ id: DocumentId, timeout: Duration = .seconds(5)) async throws {
        let deadline = ContinuousClock.now + timeout
        while ContinuousClock.now < deadline {
            if let a = try? await repoA.findLocal(id: id), let b = try? await repoB.findLocal(id: id),
               a.doc.heads() == b.doc.heads()
            {
                return
            }
            try await Task.sleep(for: .milliseconds(20))
        }
        XCTFail("documents did not converge within \(timeout)")
    }

    func testRemoteCommitProducesExactlyOneEventOnTheReceiver() async throws {
        let handleA = try await repoA.create()
        try await connect()
        try await waitUntilInSync(handleA.id)
        let changesBeforeWrite = changesOnB.changes.count

        try handleA.doc.put(obj: .ROOT, key: "title", value: .String("from A"))
        handleA.doc.commitWith(message: "title")

        let received = await changesOnB.waitForChanges { $0.count > changesBeforeWrite }
        let new = Array(received.dropFirst(changesBeforeWrite))
        XCTAssertEqual(new.count, 1)
        let change = try XCTUnwrap(new.first)
        XCTAssertEqual(change.id, handleA.id)
        XCTAssertEqual(change.source, .sync(peer: repoA.peerId))
        XCTAssertEqual(change.patches.rootKeysPut, ["title"])

        try await waitUntilInSync(handleA.id)
        let localB = try await repoB.findLocal(id: handleA.id)
        let handleB = try XCTUnwrap(localB)
        XCTAssertEqual(change.heads, handleB.doc.heads())
        // B's reply carries nothing A lacks, so A's receive produced no patches
        XCTAssertTrue(changesOnA.changes.isEmpty, "A should not hear about its own write: \(changesOnA.changes)")
    }

    func testHeadsInTheEventMatchTheDocumentAtTheMomentOfPublication() async throws {
        let handleA = try await repoA.create()
        try await connect()
        try await waitUntilInSync(handleA.id)
        let before = changesOnB.changes.count

        let headsSeenInSink = HeadsBox()
        changesOnB.onChange = { [repoB] change in
            // the sink runs on the repo actor, where the handle is already .ready
            Task { @AutomergeRepo in
                if let handle = try? await repoB!.findLocal(id: change.id) {
                    headsSeenInSink.record(handle.doc.heads())
                }
            }
        }
        try handleA.doc.put(obj: .ROOT, key: "k", value: .String("v"))
        handleA.doc.commitWith()

        let received = await changesOnB.waitForChanges { $0.count > before }
        let change = try XCTUnwrap(received.last)
        let recorded = await headsSeenInSink.waitForValue()
        XCTAssertEqual(recorded, change.heads)
    }

    func testAgreeingRepositoriesProduceNoEvents() async throws {
        let handleA = try await repoA.create()
        try handleA.doc.put(obj: .ROOT, key: "title", value: .String("shared"))
        handleA.doc.commitWith()
        // B gets an identical copy out of band, then they connect
        let copy = try Document(handleA.doc.save())
        try await repoB.import(handle: DocHandle(id: handleA.id, doc: copy))

        try await connect()
        try await waitUntilInSync(handleA.id)
        // give any stray reply a chance to land
        try await Task.sleep(for: .milliseconds(200))
        XCTAssertTrue(changesOnA.changes.isEmpty, "\(changesOnA.changes)")
        XCTAssertTrue(changesOnB.changes.isEmpty, "\(changesOnB.changes)")
    }

    func testOfflineCommitsArriveWithPatchesCoveringAllKeys() async throws {
        let handleA = try await repoA.create()
        try await connect()
        try await waitUntilInSync(handleA.id)
        await adapterA.disconnect()
        let before = changesOnB.changes.count

        try handleA.doc.put(obj: .ROOT, key: "first", value: .String("1"))
        handleA.doc.commitWith(message: "first")
        try handleA.doc.put(obj: .ROOT, key: "second", value: .String("2"))
        handleA.doc.commitWith(message: "second")

        try await connect()
        try await waitUntilInSync(handleA.id)
        let received = await changesOnB.waitForChanges { changes in
            changes.dropFirst(before).flatMap(\.patches).rootKeysPut == ["first", "second"]
        }
        let new = Array(received.dropFirst(before))
        XCTAssertFalse(new.isEmpty)
        XCTAssertEqual(new.flatMap(\.patches).rootKeysPut, ["first", "second"])
        for change in new {
            XCTAssertEqual(change.source, .sync(peer: repoA.peerId))
        }
        XCTAssertEqual(new.last?.heads, handleA.doc.heads())
    }

    func testMergeEmitsOneEventAndMergingAgainEmitsNothing() async throws {
        let target = try await repoA.create()
        let other = try await repoA.create()
        try other.doc.put(obj: .ROOT, key: "fromOther", value: .String("yes"))
        other.doc.commitWith()

        try await repoA.merge(id: target.id, with: other.id)
        var changes = changesOnA.changes
        XCTAssertEqual(changes.count, 1)
        let change = try XCTUnwrap(changes.first)
        XCTAssertEqual(change.id, target.id)
        XCTAssertEqual(change.source, .merge)
        XCTAssertEqual(change.patches.rootKeysPut, ["fromOther"])
        XCTAssertEqual(change.heads, target.doc.heads())

        try await repoA.merge(id: target.id, with: other.id)
        changes = changesOnA.changes
        XCTAssertEqual(changes.count, 1, "a merge that changes nothing publishes nothing")
    }

    func testImportIntoAnExistingDocumentEmitsOnlyWhenSomethingIsNew() async throws {
        let existing = try await repoA.create()
        try existing.doc.put(obj: .ROOT, key: "base", value: .String("b"))
        existing.doc.commitWith()

        let identical = try Document(existing.doc.save())
        try await repoA.import(handle: DocHandle(id: existing.id, doc: identical))
        XCTAssertTrue(changesOnA.changes.isEmpty, "importing an identical copy publishes nothing")

        let ahead = try Document(existing.doc.save())
        try ahead.put(obj: .ROOT, key: "extra", value: .String("e"))
        ahead.commitWith()
        try await repoA.import(handle: DocHandle(id: existing.id, doc: ahead))
        let changes = changesOnA.changes
        XCTAssertEqual(changes.count, 1)
        let change = try XCTUnwrap(changes.first)
        XCTAssertEqual(change.id, existing.id)
        XCTAssertEqual(change.source, .merge)
        XCTAssertEqual(change.patches.rootKeysPut, ["extra"])
        XCTAssertEqual(change.heads, existing.doc.heads())
    }

    func testImportOfAnUnknownDocumentEmitsNothing() async throws {
        let handle = try RepoHelpers.docHandleWithData()
        try await repoA.import(handle: handle)
        XCTAssertTrue(changesOnA.changes.isEmpty, "the caller supplied the document as-is")
    }

    func testFillingARequestedDocumentEmitsASyncEventWithAReadyHandle() async throws {
        // An agreeable A would push the document at connect time. This A only answers requests, so the
        // document reaches B through B's own `find`, filling a handle that was `.requesting`.
        let answersRequests = RequestOnlySharePolicy()
        let repoA = Repo(
            sharePolicy: answersRequests,
            maxResolveFetchIterations: 50,
            resolveFetchIterationDelay: .milliseconds(20)
        )
        let adapterA = await network.createNetworkEndpoint(config: .init(listeningNetwork: false, name: "A2"))
        await repoA.addNetworkAdapter(adapter: adapterA)
        let repoB = Repo(
            sharePolicy: SharePolicy.agreeable,
            maxResolveFetchIterations: 50,
            resolveFetchIterationDelay: .milliseconds(20)
        )
        let adapterB = await network.createNetworkEndpoint(config: .init(listeningNetwork: true, name: "B2"))
        await repoB.addNetworkAdapter(adapter: adapterB)
        let changesOnB = ChangeCollector(repoB)

        let handleA = try await repoA.create()
        try handleA.doc.put(obj: .ROOT, key: "title", value: .String("held by A"))
        handleA.doc.commitWith()
        try await adapterA.connect(to: "B2")
        try await Task.sleep(for: .milliseconds(100))
        let idsOnB = await repoB.documentIds()
        XCTAssertFalse(idsOnB.contains(handleA.id), "A must not have pushed the document at connect")

        let headsSeenInSink = HeadsBox()
        changesOnB.onChange = { change in
            Task { @AutomergeRepo in
                if let handle = try? await repoB.find(id: change.id) {
                    headsSeenInSink.record(handle.doc.heads())
                }
            }
        }
        await answersRequests.allow()

        let found = try await repoB.find(id: handleA.id)
        XCTAssertEqual(found.id, handleA.id)
        let received = await changesOnB.waitForChanges()
        let fill = try XCTUnwrap(received.first { $0.id == handleA.id })
        XCTAssertEqual(fill.source, .sync(peer: repoA.peerId))
        XCTAssertEqual(fill.patches.rootKeysPut, ["title"])
        XCTAssertEqual(fill.heads, found.doc.heads())
        let recorded = await headsSeenInSink.waitForValue()
        XCTAssertEqual(recorded, fill.heads, "a find inside the sink sees a ready handle at the event's heads")
    }

    func testDocumentChangesStreamYieldsOnlyItsDocument() async throws {
        let first = try await repoA.create()
        let second = try await repoA.create()
        try await connect()
        try await waitUntilInSync(first.id)
        try await waitUntilInSync(second.id)

        let streamed = ChangeStreamBox()
        let reader = Task {
            for await change in repoB.documentChanges(for: first.id) {
                streamed.append(change)
            }
        }
        // let the task subscribe before writing
        try await Task.sleep(for: .milliseconds(50))

        try first.doc.put(obj: .ROOT, key: "one", value: .String("1"))
        first.doc.commitWith()
        try second.doc.put(obj: .ROOT, key: "two", value: .String("2"))
        second.doc.commitWith()

        let allOnB = await changesOnB.waitForChanges { changes in
            changes.contains { $0.id == first.id } && changes.contains { $0.id == second.id }
        }
        XCTAssertTrue(allOnB.contains { $0.id == second.id }, "sanity: the publisher saw both documents")

        let fromStream = await streamed.waitForValues { !$0.isEmpty }
        reader.cancel()
        XCTAssertFalse(fromStream.isEmpty)
        for change in fromStream {
            XCTAssertEqual(change.id, first.id)
            XCTAssertEqual(change.patches.rootKeysPut, ["one"])
        }
    }

    func testEventHeadsIncludeALocalTransactionThatHeldTheLock() async throws {
        #if !canImport(Darwin)
        throw XCTSkip("needs a background thread to hold the transaction open")
        #endif
        let handleA = try await repoA.create()
        try await connect()
        try await waitUntilInSync(handleA.id)
        let localB = try await repoB.findLocal(id: handleA.id)
        let handleB = try XCTUnwrap(localB)
        let before = changesOnB.changes.count

        // B's transaction holds the document lock while A's sync message arrives
        let transactionStarted = DispatchSemaphore(value: 0)
        let transactionDone = DispatchGroup()
        DispatchQueue.global().async(group: transactionDone) {
            try? handleB.doc.transact(message: "local") { doc in
                transactionStarted.signal()
                Thread.sleep(forTimeInterval: 0.3)
                try doc.put(obj: .ROOT, key: "local", value: .String("B"))
            }
        }
        transactionStarted.wait()

        try handleA.doc.put(obj: .ROOT, key: "remote", value: .String("A"))
        handleA.doc.commitWith()

        transactionDone.wait()
        try await waitUntilInSync(handleA.id)
        let received = await changesOnB.waitForChanges { changes in
            changes.dropFirst(before).flatMap(\.patches).rootKeysPut.contains("remote")
        }
        let new = Array(received.dropFirst(before))
        let remoteEvent = try XCTUnwrap(new.first { $0.patches.rootKeysPut.contains("remote") })
        XCTAssertEqual(remoteEvent.patches.rootKeysPut, ["remote"], "only A's keys are foreign")
        XCTAssertFalse(remoteEvent.patches.rootKeysPut.contains("local"))
        // the lock serialised them, so the heads read after the application include B's commit; A never
        // saw that commit before writing, so nothing can have built on it and it must be a head itself
        let localCommit = try XCTUnwrap(handleB.doc.getHistory().first { hash in
            handleB.doc.change(hash: hash)?.message == "local"
        })
        XCTAssertTrue(
            remoteEvent.heads.contains(localCommit),
            "event heads \(remoteEvent.heads) should include B's commit \(localCommit)"
        )
    }
}

/// Shares nothing until `allow()`, so a repo neither pushes on connect nor answers a request before the test is ready.
actor RequestOnlySharePolicy: ShareAuthorizing {
    private var sharing = false

    func allow() {
        sharing = true
    }

    func share(peer _: PEER_ID, docId _: DocumentId) async -> Bool {
        sharing
    }
}

/// A thread-safe slot for heads recorded inside a sink.
final class HeadsBox: @unchecked Sendable {
    private let lock = NSLock()
    private var value: Set<ChangeHash>?

    func record(_ heads: Set<ChangeHash>) {
        lock.withLock { value = heads }
    }

    func waitForValue(timeout: Duration = .seconds(5)) async -> Set<ChangeHash>? {
        let deadline = ContinuousClock.now + timeout
        while ContinuousClock.now < deadline {
            if let v = lock.withLock({ value }) {
                return v
            }
            try? await Task.sleep(for: .milliseconds(20))
        }
        return lock.withLock { value }
    }
}

/// A thread-safe list for changes read from an `AsyncStream`.
final class ChangeStreamBox: @unchecked Sendable {
    private let lock = NSLock()
    private var values: [DocumentChange] = []

    func append(_ change: DocumentChange) {
        lock.withLock { values.append(change) }
    }

    func waitForValues(
        where predicate: @escaping ([DocumentChange]) -> Bool,
        timeout: Duration = .seconds(5)
    ) async -> [DocumentChange] {
        let deadline = ContinuousClock.now + timeout
        while ContinuousClock.now < deadline {
            let current = lock.withLock { values }
            if predicate(current) {
                return current
            }
            try? await Task.sleep(for: .milliseconds(20))
        }
        return lock.withLock { values }
    }
}
