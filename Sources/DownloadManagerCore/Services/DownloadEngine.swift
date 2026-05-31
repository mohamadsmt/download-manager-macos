import Foundation

public enum DownloadEngineError: Error, LocalizedError, Sendable {
    case invalidResponse
    case httpStatus(Int)
    case missingAria2Binary(URL)
    case aria2Failed(Int32)
    case cancelled
    case fileSystem(String)

    public var errorDescription: String? {
        switch self {
        case .invalidResponse:
            return "The server returned an invalid response."
        case .httpStatus(let status):
            return "The server returned HTTP \(status)."
        case .missingAria2Binary(let url):
            return "aria2c was not found at \(url.path)."
        case .aria2Failed(let status):
            return "aria2c exited with status \(status)."
        case .cancelled:
            return "The download was cancelled."
        case .fileSystem(let message):
            return message
        }
    }
}

public struct DownloadOptions: Sendable {
    public var maxSegments: Int
    public var globalSpeedLimitBytesPerSecond: Int64?
    public var additionalHeaders: [String: String]
    public var workingDirectory: URL

    public init(
        maxSegments: Int = 8,
        globalSpeedLimitBytesPerSecond: Int64? = nil,
        additionalHeaders: [String: String] = [:],
        workingDirectory: URL
    ) {
        self.maxSegments = max(1, min(16, maxSegments))
        self.globalSpeedLimitBytesPerSecond = globalSpeedLimitBytesPerSecond
        self.additionalHeaders = additionalHeaders
        self.workingDirectory = workingDirectory
    }
}

public struct DownloadProgressEvent: Sendable {
    public var itemID: DownloadItem.ID
    public var completedBytes: Int64
    public var totalBytes: Int64?
    public var speedBytesPerSecond: Int64
    public var segments: [DownloadSegment]
    public var status: DownloadStatus
    public var fileName: String?
    public var category: DownloadCategory?
    public var acceptsRanges: Bool?
    public var etag: String?
    public var lastModified: String?

    public init(
        itemID: DownloadItem.ID,
        completedBytes: Int64,
        totalBytes: Int64?,
        speedBytesPerSecond: Int64,
        segments: [DownloadSegment],
        status: DownloadStatus,
        fileName: String? = nil,
        category: DownloadCategory? = nil,
        acceptsRanges: Bool? = nil,
        etag: String? = nil,
        lastModified: String? = nil
    ) {
        self.itemID = itemID
        self.completedBytes = completedBytes
        self.totalBytes = totalBytes
        self.speedBytesPerSecond = speedBytesPerSecond
        self.segments = segments
        self.status = status
        self.fileName = fileName
        self.category = category
        self.acceptsRanges = acceptsRanges
        self.etag = etag
        self.lastModified = lastModified
    }
}

public protocol DownloadEngine: Sendable {
    var mode: DownloadEngineMode { get }

    func download(
        item: DownloadItem,
        options: DownloadOptions,
        progress: @escaping @Sendable (DownloadProgressEvent) async -> Void
    ) async throws -> DownloadItem
}
