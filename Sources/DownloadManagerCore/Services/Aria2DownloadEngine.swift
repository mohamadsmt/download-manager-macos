import Foundation

public struct Aria2DownloadEngine: DownloadEngine {
    public let mode: DownloadEngineMode = .aria2
    public var executableURL: URL

    public init(executableURL: URL) {
        self.executableURL = executableURL
    }

    public func download(
        item: DownloadItem,
        options: DownloadOptions,
        progress: @escaping @Sendable (DownloadProgressEvent) async -> Void
    ) async throws -> DownloadItem {
        guard FileManager.default.isExecutableFile(atPath: executableURL.path) else {
            throw DownloadEngineError.missingAria2Binary(executableURL)
        }

        try FileManager.default.createDirectory(at: item.destinationDirectory, withIntermediateDirectories: true)

        var workingItem = item
        workingItem.status = .downloading
        workingItem.updatedAt = Date()
        await progress(
            DownloadProgressEvent(
                itemID: item.id,
                completedBytes: item.completedBytes,
                totalBytes: item.totalBytes,
                speedBytesPerSecond: 0,
                segments: item.segments,
                status: .downloading,
                fileName: item.fileName,
                category: item.category,
                acceptsRanges: item.acceptsRanges,
                etag: item.etag,
                lastModified: item.lastModified
            )
        )

        let process = Process()
        process.executableURL = executableURL

        var arguments = [
            "--continue=true",
            "--allow-overwrite=false",
            "--auto-file-renaming=false",
            "--split=\(options.maxSegments)",
            "--max-connection-per-server=\(options.maxSegments)",
            "--min-split-size=1M",
            "--dir=\(item.destinationDirectory.path)",
            "--out=\(item.fileName)",
            item.url.absoluteString
        ]

        if let referrer = item.referrer {
            arguments.insert("--referer=\(referrer.absoluteString)", at: arguments.count - 1)
        }

        if let speedLimit = item.speedLimitBytesPerSecond ?? options.globalSpeedLimitBytesPerSecond, speedLimit > 0 {
            arguments.insert("--max-download-limit=\(speedLimit)", at: arguments.count - 1)
        }

        process.arguments = arguments

        try process.run()
        process.waitUntilExit()

        if Task.isCancelled {
            process.terminate()
            throw DownloadEngineError.cancelled
        }

        guard process.terminationStatus == 0 else {
            throw DownloadEngineError.aria2Failed(process.terminationStatus)
        }

        let finalURL = item.destinationFileURL
        let fileSize = (try? FileManager.default.attributesOfItem(atPath: finalURL.path)[.size] as? NSNumber)?.int64Value
        workingItem.completedBytes = fileSize ?? item.totalBytes ?? 0
        workingItem.totalBytes = item.totalBytes ?? fileSize
        workingItem.status = .completed
        workingItem.completedAt = Date()
        workingItem.updatedAt = Date()
        workingItem.speedBytesPerSecond = 0

        await progress(
            DownloadProgressEvent(
                itemID: item.id,
                completedBytes: workingItem.completedBytes,
                totalBytes: workingItem.totalBytes,
                speedBytesPerSecond: 0,
                segments: workingItem.segments,
                status: .completed,
                fileName: workingItem.fileName,
                category: workingItem.category,
                acceptsRanges: workingItem.acceptsRanges,
                etag: workingItem.etag,
                lastModified: workingItem.lastModified
            )
        )

        return workingItem
    }
}
