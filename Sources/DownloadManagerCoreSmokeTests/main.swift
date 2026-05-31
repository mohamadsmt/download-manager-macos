import DownloadManagerCore
import Foundation

enum SmokeTestFailure: Error, CustomStringConvertible {
    case failed(String)

    var description: String {
        switch self {
        case .failed(let message): return message
        }
    }
}

func expect(_ condition: @autoclosure () -> Bool, _ message: String) throws {
    if !condition() {
        throw SmokeTestFailure.failed(message)
    }
}

func testSegmentPlanner() throws {
    let unsupported = SegmentPlanner().makeSegments(totalBytes: 10_000_000, acceptsRanges: false)
    try expect(unsupported.count == 1, "Range-unsupported downloads should use one segment.")
    try expect(unsupported[0].range.upperBound == nil, "Range-unsupported segment should be open-ended.")

    let planner = SegmentPlanner(defaultMaxSegments: 8, absoluteMaxSegments: 16, minimumSegmentSize: 1_000)
    let segments = planner.makeSegments(totalBytes: 8_000, acceptsRanges: true)
    try expect(segments.count == 8, "Known byte ranges should split into eight segments.")
    try expect(segments.first?.range.lowerBound == 0, "First segment should start at zero.")
    try expect(segments.last?.range.upperBound == 7_999, "Last segment should end at totalBytes - 1.")
    try expect(segments.reduce(0) { $0 + ($1.expectedLength ?? 0) } == 8_000, "Segments should cover the full byte range.")
}

func testQueueCoordinator() throws {
    let destination = URL(fileURLWithPath: NSTemporaryDirectory())
    let first = DownloadItem(url: URL(string: "https://example.com/a.zip")!, destinationDirectory: destination, status: .queued, createdAt: Date(timeIntervalSince1970: 1))
    let second = DownloadItem(url: URL(string: "https://example.com/b.zip")!, destinationDirectory: destination, status: .queued, createdAt: Date(timeIntervalSince1970: 2))
    let running = DownloadItem(url: URL(string: "https://example.com/c.zip")!, destinationDirectory: destination, status: .downloading)
    let coordinator = QueueCoordinator(globalConcurrency: 1)

    try expect(coordinator.nextStartableID(in: [second, first]) == first.id, "Coordinator should start the oldest queued item first.")
    try expect(coordinator.nextStartableID(in: [running, first]) == nil, "Coordinator should not start another item while one is downloading.")
}

func testFilenameResolver() throws {
    let url = URL(string: "https://example.com/download")!
    let name = FilenameResolver.fileName(from: url, contentDisposition: #"attachment; filename="report.pdf""#)
    try expect(name == "report.pdf", "Content-Disposition filename should win.")
    try expect(FilenameResolver.sanitize("bad/name:file?.zip") == "bad-name-file-.zip", "Filename sanitizer should replace unsafe characters.")
}

func testProgressStateIsMonotonicAndThrottled() async throws {
    let destination = URL(fileURLWithPath: NSTemporaryDirectory())
    let segments = [
        DownloadSegment(index: 0, range: ByteRange(lowerBound: 0, upperBound: 99)),
        DownloadSegment(index: 1, range: ByteRange(lowerBound: 100, upperBound: 199))
    ]
    let item = DownloadItem(
        url: URL(string: "https://example.com/file.bin")!,
        destinationDirectory: destination,
        totalBytes: 200,
        segments: segments,
        acceptsRanges: true
    )
    let state = DownloadProgressState(item: item, minimumEmitInterval: 60)

    var first = segments[0]
    first.status = .downloading
    first.bytesWritten = 40
    let firstEvent = await state.update(segment: first, bytesDelta: 40, force: true)
    try expect(firstEvent?.completedBytes == 40, "First forced progress event should include initial segment bytes.")

    var second = segments[1]
    second.status = .downloading
    second.bytesWritten = 50
    let throttledEvent = await state.update(segment: second, bytesDelta: 50)
    try expect(throttledEvent == nil, "Non-forced events should be throttled inside the minimum interval.")

    first.bytesWritten = 10
    let monotonicEvent = await state.update(segment: first, bytesDelta: 0, force: true)
    let firstSegment = monotonicEvent?.segments.first { $0.index == 0 }
    let secondSegment = monotonicEvent?.segments.first { $0.index == 1 }
    try expect(firstSegment?.bytesWritten == 40, "Segment progress should never move backward.")
    try expect(secondSegment?.bytesWritten == 50, "Throttled segment updates should still be retained internally.")
    try expect(monotonicEvent?.completedBytes == 90, "Aggregate progress should include all retained segment progress.")
}

func testResourceResolver() async throws {
    MockURLProtocol.requestHandler = { request in
        try expect(request.httpMethod == "HEAD", "Resolver should probe with HEAD first.")
        let response = HTTPURLResponse(
            url: request.url!,
            statusCode: 200,
            httpVersion: "HTTP/1.1",
            headerFields: [
                "Content-Length": "4096",
                "Accept-Ranges": "bytes",
                "ETag": "\"abc\"",
                "Last-Modified": "Sun, 31 May 2026 10:00:00 GMT",
                "Content-Disposition": #"attachment; filename="sample.dmg""#
            ]
        )!
        return (response, Data())
    }

    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [MockURLProtocol.self]
    let resolver = ResourceResolver(session: URLSession(configuration: configuration))
    let probe = try await resolver.resolve(url: URL(string: "https://example.com/download")!)

    try expect(probe.fileName == "sample.dmg", "Resolver should read Content-Disposition filenames.")
    try expect(probe.totalBytes == 4_096, "Resolver should read Content-Length.")
    try expect(probe.acceptsRanges, "Resolver should detect byte-range support.")
    try expect(probe.etag == "\"abc\"", "Resolver should preserve ETag.")
}

final class MockURLProtocol: URLProtocol {
    static var requestHandler: ((URLRequest) throws -> (HTTPURLResponse, Data))?

    override class func canInit(with request: URLRequest) -> Bool {
        true
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        request
    }

    override func startLoading() {
        guard let handler = MockURLProtocol.requestHandler else {
            client?.urlProtocol(self, didFailWithError: URLError(.badServerResponse))
            return
        }

        do {
            let (response, data) = try handler(request)
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        } catch {
            client?.urlProtocol(self, didFailWithError: error)
        }
    }

    override func stopLoading() {}
}

do {
    try testSegmentPlanner()
    try testQueueCoordinator()
    try testFilenameResolver()
    try await testProgressStateIsMonotonicAndThrottled()
    try await testResourceResolver()
    print("DownloadManagerCoreSmokeTests passed")
} catch {
    fputs("DownloadManagerCoreSmokeTests failed: \(error)\n", stderr)
    exit(1)
}
