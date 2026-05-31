import Foundation

public struct NativeSegmentedDownloadEngine: DownloadEngine {
    public let mode: DownloadEngineMode = .native
    private let resolver: ResourceResolver
    private let planner: SegmentPlanner
    private let session: URLSession

    public init(
        resolver: ResourceResolver = ResourceResolver(),
        planner: SegmentPlanner = SegmentPlanner(),
        session: URLSession = .shared
    ) {
        self.resolver = resolver
        self.planner = planner
        self.session = session
    }

    public func download(
        item: DownloadItem,
        options: DownloadOptions,
        progress: @escaping @Sendable (DownloadProgressEvent) async -> Void
    ) async throws -> DownloadItem {
        var workingItem = item
        workingItem.status = .resolving
        workingItem.updatedAt = Date()

        let probe = try await resolver.resolve(
            url: workingItem.url,
            referrer: workingItem.referrer,
            headers: options.additionalHeaders
        )

        workingItem.url = probe.finalURL
        workingItem.fileName = workingItem.suggestedFileName ?? probe.fileName
        workingItem.category = DownloadCategory.infer(fileName: workingItem.fileName)
        workingItem.totalBytes = probe.totalBytes
        workingItem.acceptsRanges = probe.acceptsRanges
        workingItem.etag = probe.etag
        workingItem.lastModified = probe.lastModified
        workingItem.segments = planner.makeSegments(
            totalBytes: probe.totalBytes,
            acceptsRanges: probe.acceptsRanges,
            requestedMaxSegments: options.maxSegments
        )

        let itemDirectory = options.workingDirectory.appendingPathComponent(workingItem.id.uuidString, isDirectory: true)
        let manifestStore = SidecarManifestStore(directory: itemDirectory.appendingPathComponent("manifests", isDirectory: true))
        try FileManager.default.createDirectory(at: itemDirectory, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: workingItem.destinationDirectory, withIntermediateDirectories: true)
        workingItem.segments = hydrateSegments(workingItem.segments, itemDirectory: itemDirectory, acceptsRanges: probe.acceptsRanges)
        workingItem.completedBytes = workingItem.segments.reduce(0) { $0 + $1.bytesWritten }
        workingItem.status = .downloading
        workingItem.updatedAt = Date()
        try manifestStore.save(SidecarManifest(item: workingItem))

        await progress(event(for: workingItem, speed: 0))

        let limiter = SpeedLimiter(
            limitBytesPerSecond: workingItem.speedLimitBytesPerSecond ?? options.globalSpeedLimitBytesPerSecond
        )
        let progressState = DownloadProgressState(item: workingItem)

        let segments = try await downloadSegments(
            item: workingItem,
            itemDirectory: itemDirectory,
            headers: options.additionalHeaders,
            limiter: limiter,
            progressState: progressState,
            progress: progress
        )

        try Task.checkCancellation()

        workingItem.segments = segments
        workingItem.completedBytes = segments.reduce(0) { $0 + $1.bytesWritten }
        try merge(segments: segments, itemDirectory: itemDirectory, destination: workingItem.destinationFileURL)
        workingItem.status = .completed
        workingItem.completedAt = Date()
        workingItem.updatedAt = Date()
        workingItem.speedBytesPerSecond = 0
        try manifestStore.save(SidecarManifest(item: workingItem))
        await progress(event(for: workingItem, speed: 0))

        return workingItem
    }

    private func downloadSegments(
        item: DownloadItem,
        itemDirectory: URL,
        headers: [String: String],
        limiter: SpeedLimiter,
        progressState: DownloadProgressState,
        progress: @escaping @Sendable (DownloadProgressEvent) async -> Void
    ) async throws -> [DownloadSegment] {
        let originalSegments = item.segments

        return try await withThrowingTaskGroup(of: DownloadSegment.self) { group in
            for segment in originalSegments {
                group.addTask {
                    try await downloadSegment(
                        segment,
                        item: item,
                        itemDirectory: itemDirectory,
                        headers: headers,
                        limiter: limiter,
                        progressState: progressState,
                        progress: progress
                    )
                }
            }

            var completed: [DownloadSegment] = []
            for try await segment in group {
                completed.append(segment)
            }

            return completed.sorted { $0.index < $1.index }
        }
    }

    private func downloadSegment(
        _ segment: DownloadSegment,
        item: DownloadItem,
        itemDirectory: URL,
        headers: [String: String],
        limiter: SpeedLimiter,
        progressState: DownloadProgressState,
        progress: @escaping @Sendable (DownloadProgressEvent) async -> Void
    ) async throws -> DownloadSegment {
        var currentSegment = segment
        let segmentURL = itemDirectory.appendingPathComponent(segment.fileName)
        var existingBytes = existingFileSize(at: segmentURL)

        if !item.acceptsRanges, existingBytes > 0 {
            try? FileManager.default.removeItem(at: segmentURL)
            existingBytes = 0
        }

        if let expected = segment.expectedLength, existingBytes >= expected {
            currentSegment.bytesWritten = expected
            currentSegment.status = .completed
            return currentSegment
        }

        currentSegment.bytesWritten = existingBytes
        currentSegment.status = .downloading
        if let event = await progressState.update(segment: currentSegment, bytesDelta: 0, force: true) {
            await progress(event)
        }

        var request = URLRequest(url: item.url)
        request.httpMethod = "GET"
        request.timeoutInterval = 60
        headers.forEach { request.setValue($0.value, forHTTPHeaderField: $0.key) }
        if let referrer = item.referrer {
            request.setValue(referrer.absoluteString, forHTTPHeaderField: "Referer")
        }

        let rangeStart = segment.range.lowerBound + existingBytes
        let requestedRange = ByteRange(lowerBound: rangeStart, upperBound: segment.range.upperBound)
        if item.acceptsRanges, let header = requestedRange.headerValue {
            request.setValue(header, forHTTPHeaderField: "Range")
        }

        let (bytes, response) = try await session.bytes(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw DownloadEngineError.invalidResponse
        }

        if segment.range.upperBound != nil {
            guard http.statusCode == 206 || (segment.index == 0 && item.segments.count == 1 && http.statusCode == 200) else {
                throw DownloadEngineError.httpStatus(http.statusCode)
            }
        } else {
            guard (200..<300).contains(http.statusCode) else {
                throw DownloadEngineError.httpStatus(http.statusCode)
            }
        }

        if !FileManager.default.fileExists(atPath: segmentURL.path) {
            FileManager.default.createFile(atPath: segmentURL.path, contents: nil)
        }

        let handle = try FileHandle(forWritingTo: segmentURL)
        try handle.seekToEnd()
        defer {
            try? handle.close()
        }

        var buffer = Data()

        for try await byte in bytes {
            try Task.checkCancellation()
            buffer.append(byte)

            if buffer.count >= 64 * 1024 {
                let written = buffer.count
                try handle.write(contentsOf: buffer)
                buffer.removeAll(keepingCapacity: true)
                currentSegment.bytesWritten += Int64(written)
                await limiter.throttle(bytes: written)
                if let event = await progressState.update(segment: currentSegment, bytesDelta: Int64(written)) {
                    await progress(event)
                }
            }
        }

        if !buffer.isEmpty {
            let written = buffer.count
            try handle.write(contentsOf: buffer)
            currentSegment.bytesWritten += Int64(written)
            await limiter.throttle(bytes: written)
            if let event = await progressState.update(segment: currentSegment, bytesDelta: Int64(written)) {
                await progress(event)
            }
        }

        currentSegment.status = .completed
        if let event = await progressState.update(segment: currentSegment, bytesDelta: 0, force: true) {
            await progress(event)
        }
        return currentSegment
    }

    private func merge(segments: [DownloadSegment], itemDirectory: URL, destination: URL) throws {
        let temporaryDestination = destination.deletingLastPathComponent().appendingPathComponent(".\(destination.lastPathComponent).download")
        if FileManager.default.fileExists(atPath: temporaryDestination.path) {
            try FileManager.default.removeItem(at: temporaryDestination)
        }
        FileManager.default.createFile(atPath: temporaryDestination.path, contents: nil)

        let output = try FileHandle(forWritingTo: temporaryDestination)
        defer {
            try? output.close()
        }

        for segment in segments.sorted(by: { $0.index < $1.index }) {
            let segmentURL = itemDirectory.appendingPathComponent(segment.fileName)
            let input = try FileHandle(forReadingFrom: segmentURL)
            defer {
                try? input.close()
            }

            while true {
                let data = try input.read(upToCount: 512 * 1024) ?? Data()
                if data.isEmpty { break }
                try output.write(contentsOf: data)
            }
        }

        if FileManager.default.fileExists(atPath: destination.path) {
            try FileManager.default.removeItem(at: destination)
        }
        try FileManager.default.moveItem(at: temporaryDestination, to: destination)
    }

    private func existingFileSize(at url: URL) -> Int64 {
        guard let attributes = try? FileManager.default.attributesOfItem(atPath: url.path),
              let size = attributes[.size] as? NSNumber else {
            return 0
        }
        return size.int64Value
    }

    private func hydrateSegments(_ segments: [DownloadSegment], itemDirectory: URL, acceptsRanges: Bool) -> [DownloadSegment] {
        guard acceptsRanges else { return segments }

        return segments.map { segment in
            var hydrated = segment
            let fileSize = existingFileSize(at: itemDirectory.appendingPathComponent(segment.fileName))
            if let expected = segment.expectedLength {
                hydrated.bytesWritten = min(fileSize, expected)
                hydrated.status = hydrated.bytesWritten >= expected ? .completed : .pending
            } else {
                hydrated.bytesWritten = fileSize
                hydrated.status = fileSize > 0 ? .pending : segment.status
            }
            return hydrated
        }
    }

    private func event(for item: DownloadItem, speed: Int64) -> DownloadProgressEvent {
        DownloadProgressEvent(
            itemID: item.id,
            completedBytes: item.completedBytes,
            totalBytes: item.totalBytes,
            speedBytesPerSecond: speed,
            segments: item.segments,
            status: item.status,
            fileName: item.fileName,
            category: item.category,
            acceptsRanges: item.acceptsRanges,
            etag: item.etag,
            lastModified: item.lastModified
        )
    }
}

public actor DownloadProgressState {
    private var item: DownloadItem
    private var segments: [DownloadSegment]
    private var recentWindowStart = Date()
    private var recentBytes: Int64 = 0
    private var smoothedSpeed: Double = 0
    private var lastEmitAt = Date.distantPast
    private let minimumEmitInterval: TimeInterval

    public init(item: DownloadItem, minimumEmitInterval: TimeInterval = 0.7) {
        self.item = item
        self.minimumEmitInterval = minimumEmitInterval
        self.segments = item.segments.map { segment in
            var updated = segment
            if updated.status != .completed {
                updated.status = updated.bytesWritten > 0 ? .downloading : .pending
            }
            return updated
        }
    }

    public func update(segment: DownloadSegment, bytesDelta: Int64, force: Bool = false) -> DownloadProgressEvent? {
        merge(segment)

        if bytesDelta > 0 {
            recentBytes += bytesDelta
        }

        let now = Date()
        let windowElapsed = now.timeIntervalSince(recentWindowStart)
        if windowElapsed >= minimumEmitInterval {
            let instantSpeed = Double(recentBytes) / max(windowElapsed, 0.1)
            smoothedSpeed = smoothedSpeed == 0 ? instantSpeed : (smoothedSpeed * 0.72 + instantSpeed * 0.28)
            recentBytes = 0
            recentWindowStart = now
        }

        guard force || now.timeIntervalSince(lastEmitAt) >= minimumEmitInterval else {
            return nil
        }

        lastEmitAt = now
        return event(speed: Int64(smoothedSpeed.rounded()))
    }

    private func merge(_ incoming: DownloadSegment) {
        guard let index = segments.firstIndex(where: { $0.id == incoming.id }) else { return }

        let previous = segments[index]
        var updated = incoming
        updated.bytesWritten = max(previous.bytesWritten, incoming.bytesWritten)

        if previous.status == .completed || incoming.status == .completed {
            updated.status = .completed
        } else if updated.bytesWritten > 0 || incoming.status == .downloading || previous.status == .downloading {
            updated.status = .downloading
        } else {
            updated.status = .pending
        }

        segments[index] = updated
    }

    private func event(speed: Int64) -> DownloadProgressEvent {
        let orderedSegments = segments.sorted { $0.index < $1.index }
        let completedBytes = orderedSegments.reduce(0) { $0 + $1.bytesWritten }

        return DownloadProgressEvent(
            itemID: item.id,
            completedBytes: completedBytes,
            totalBytes: item.totalBytes,
            speedBytesPerSecond: speed,
            segments: orderedSegments,
            status: .downloading,
            fileName: item.fileName,
            category: item.category,
            acceptsRanges: item.acceptsRanges,
            etag: item.etag,
            lastModified: item.lastModified
        )
    }
}
