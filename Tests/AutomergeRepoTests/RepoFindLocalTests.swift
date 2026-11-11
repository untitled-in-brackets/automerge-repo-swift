import Automerge
@testable import AutomergeRepo
import AutomergeUtilities
import Foundation
import XCTest

final class RepoFindLocalTests: XCTestCase {
    var storage: InMemoryStorage!
    var repo: Repo!

    override func setUp() async throws {
        storage = InMemoryStorage()
        repo = Repo(sharePolicy: SharePolicy.agreeable, storage: storage)
    }

    private func assertChunkOnly(_ id: DocumentId, file: StaticString = #filePath, line: UInt = #line) async {
        let base = await storage.load(id: id)
        let chunks = await storage.loadRange(id: id, prefix: "incrChanges")
        XCTAssertNil(base, "expected no base snapshot in storage", file: file, line: line)
        XCTAssertFalse(chunks.isEmpty, "expected incremental chunks in storage", file: file, line: line)
    }

    func testUnknownIdWithAdapterAttachedReturnsNilWithoutWaiting() async throws {
        let websocket = WebSocketProvider(.init(reconnectOnError: false))
        await repo.addNetworkAdapter(adapter: websocket)
        let id = DocumentId()

        let clock = ContinuousClock()
        let elapsed = try await clock.measure {
            let found = try await repo.findLocal(id: id)
            XCTAssertNil(found)
        }
        // find would spend at least one resolveFetchIterationDelay (1s by default) before giving up
        XCTAssertLessThan(elapsed, .milliseconds(500))

        let ids = await repo.documentIds()
        XCTAssertFalse(ids.contains(id), "a miss must not leave a handle behind")
    }

    func testUnknownIdWithoutStorageReturnsNil() async throws {
        let memoryOnly = Repo(sharePolicy: SharePolicy.agreeable)
        let found = try await memoryOnly.findLocal(id: DocumentId())
        XCTAssertNil(found)
    }

    func testFindsDocumentStoredAsBaseSnapshotBySharingRepo() async throws {
        let imported = try await repo.import(handle: RepoHelpers.docHandleWithData())
        // the storage subsystem writes incremental chunks first; replace them with a base snapshot
        // so this exercises the `load(id:)` path
        let chunks = await storage.loadRange(id: imported.id, prefix: "incrChanges")
        await storage.removeRange(id: imported.id, prefix: "incrChanges", data: chunks)
        await storage.save(id: imported.id, data: imported.doc.save())
        let base = await storage.load(id: imported.id)
        XCTAssertNotNil(base)
        let remaining = await storage.loadRange(id: imported.id, prefix: "incrChanges")
        XCTAssertTrue(remaining.isEmpty)

        let other = Repo(sharePolicy: SharePolicy.agreeable, storage: storage)
        let found = try await other.findLocal(id: imported.id)
        let unwrapped = try XCTUnwrap(found)
        XCTAssertEqual(unwrapped.id, imported.id)
        XCTAssertTrue(unwrapped.doc.equivalentContents(imported.doc))

        let ids = await other.documentIds()
        XCTAssertTrue(ids.contains(imported.id))
    }

    func testFindsDocumentStoredOnlyAsIncrementalChunks() async throws {
        let imported = try await repo.import(handle: RepoHelpers.docHandleWithData())
        await assertChunkOnly(imported.id)

        let other = Repo(sharePolicy: SharePolicy.agreeable, storage: storage)
        let found = try await other.findLocal(id: imported.id)
        let unwrapped = try XCTUnwrap(found)
        XCTAssertEqual(unwrapped.id, imported.id)
        XCTAssertTrue(unwrapped.doc.equivalentContents(imported.doc))
    }

    func testFindsDocumentWithLaterIncrementalChangesOnly() async throws {
        // a second save with new changes on top of chunk-only storage stays chunk-only until compaction
        let imported = try await repo.import(handle: DocHandle(id: DocumentId(), doc: Document()))
        try imported.doc.put(obj: .ROOT, key: "title", value: .String("after import"))
        try await repo.savePendingChanges(id: imported.id)
        await assertChunkOnly(imported.id)

        let other = Repo(sharePolicy: SharePolicy.agreeable, storage: storage)
        let found = try await other.findLocal(id: imported.id)
        let unwrapped = try XCTUnwrap(found)
        XCTAssertTrue(unwrapped.doc.equivalentContents(imported.doc))
        XCTAssertEqual(try unwrapped.doc.get(obj: .ROOT, key: "title"), .Scalar(.String("after import")))
    }

    func testRecoversAfterFindGaveUp() async throws {
        let impatient = Repo(
            sharePolicy: SharePolicy.agreeable,
            storage: storage,
            maxResolveFetchIterations: 1,
            resolveFetchIterationDelay: .milliseconds(50)
        )
        // a peer that never answers, so find polls out instead of failing for want of peers
        let silent = RecordingNetworkProvider(name: "silent")
        await impatient.addNetworkAdapter(adapter: silent)
        await silent.peer(with: "silent-peer")

        let id = DocumentId()
        do {
            _ = try await impatient.find(id: id)
            XCTFail("expected find to give up")
        } catch let error as Errors.Unavailable {
            XCTAssertEqual(error.id, id)
        }
        let requests = await silent.sent
        XCTAssertFalse(requests.isEmpty, "find should have asked the peer")

        // another path writes the document into storage
        let doc = try RepoHelpers.documentWithData()
        await storage.save(id: id, data: doc.save())

        let found = try await impatient.findLocal(id: id)
        let unwrapped = try XCTUnwrap(found)
        XCTAssertTrue(unwrapped.doc.equivalentContents(doc))

        let foundAgain = try await impatient.find(id: id)
        XCTAssertTrue(foundAgain.doc === unwrapped.doc)
    }

    func testUnavailableIdStaysUnavailableWhenStorageIsStillEmpty() async throws {
        let impatient = Repo(
            sharePolicy: SharePolicy.agreeable,
            storage: storage,
            maxResolveFetchIterations: 1,
            resolveFetchIterationDelay: .milliseconds(50)
        )
        let silent = RecordingNetworkProvider(name: "silent")
        await impatient.addNetworkAdapter(adapter: silent)
        await silent.peer(with: "silent-peer")

        let id = DocumentId()
        do {
            _ = try await impatient.find(id: id)
            XCTFail("expected find to give up")
        } catch is Errors.Unavailable {}

        let found = try await impatient.findLocal(id: id)
        XCTAssertNil(found)
        do {
            _ = try await impatient.find(id: id)
            XCTFail("find should still report the document unavailable")
        } catch is Errors.Unavailable {}
    }

    func testDeletedIdThrowsDocDeleted() async throws {
        let created = try await repo.create()
        try await repo.delete(id: created.id)
        do {
            _ = try await repo.findLocal(id: created.id)
            XCTFail("expected DocDeleted")
        } catch let error as Errors.DocDeleted {
            XCTAssertEqual(error.id, created.id)
        }
    }

    func testReadyHandleReturnsTheSameDocumentAsFind() async throws {
        let created = try await repo.create()
        try created.doc.put(obj: .ROOT, key: "title", value: .String("in memory"))

        let viaFind = try await repo.find(id: created.id)
        let viaFindLocal = try await repo.findLocal(id: created.id)
        let unwrapped = try XCTUnwrap(viaFindLocal)
        XCTAssertTrue(unwrapped.doc === viaFind.doc)
        XCTAssertTrue(unwrapped.doc === created.doc)
    }
}
