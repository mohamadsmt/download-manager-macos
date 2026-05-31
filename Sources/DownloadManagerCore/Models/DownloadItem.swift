import Foundation

public struct DownloadItem: Identifiable, Codable, Hashable, Sendable {
    public var id: UUID
    public var url: URL
    public var referrer: URL?
    public var suggestedFileName: String?
    public var fileName: String
    public var destinationDirectory: URL
    public var status: DownloadStatus
    public var engineMode: DownloadEngineMode
    public var category: DownloadCategory
    public var totalBytes: Int64?
    public var completedBytes: Int64
    public var speedBytesPerSecond: Int64
    public var speedLimitBytesPerSecond: Int64?
    public var segments: [DownloadSegment]
    public var etag: String?
    public var lastModified: String?
    public var acceptsRanges: Bool
    public var errorMessage: String?
    public var createdAt: Date
    public var updatedAt: Date
    public var completedAt: Date?
    public var scheduleStart: Date?
    public var scheduleEnd: Date?
    public var priority: Int

    public init(
        id: UUID = UUID(),
        url: URL,
        referrer: URL? = nil,
        suggestedFileName: String? = nil,
        fileName: String? = nil,
        destinationDirectory: URL,
        status: DownloadStatus = .pending,
        engineMode: DownloadEngineMode = .automatic,
        category: DownloadCategory = .other,
        totalBytes: Int64? = nil,
        completedBytes: Int64 = 0,
        speedBytesPerSecond: Int64 = 0,
        speedLimitBytesPerSecond: Int64? = nil,
        segments: [DownloadSegment] = [],
        etag: String? = nil,
        lastModified: String? = nil,
        acceptsRanges: Bool = false,
        errorMessage: String? = nil,
        createdAt: Date = Date(),
        updatedAt: Date = Date(),
        completedAt: Date? = nil,
        scheduleStart: Date? = nil,
        scheduleEnd: Date? = nil,
        priority: Int = 0
    ) {
        self.id = id
        self.url = url
        self.referrer = referrer
        self.suggestedFileName = suggestedFileName
        self.fileName = fileName ?? suggestedFileName ?? FilenameResolver.fileName(from: url)
        self.destinationDirectory = destinationDirectory
        self.status = status
        self.engineMode = engineMode
        self.category = category == .other ? DownloadCategory.infer(fileName: self.fileName) : category
        self.totalBytes = totalBytes
        self.completedBytes = completedBytes
        self.speedBytesPerSecond = speedBytesPerSecond
        self.speedLimitBytesPerSecond = speedLimitBytesPerSecond
        self.segments = segments
        self.etag = etag
        self.lastModified = lastModified
        self.acceptsRanges = acceptsRanges
        self.errorMessage = errorMessage
        self.createdAt = createdAt
        self.updatedAt = updatedAt
        self.completedAt = completedAt
        self.scheduleStart = scheduleStart
        self.scheduleEnd = scheduleEnd
        self.priority = priority
    }

    public var destinationFileURL: URL {
        destinationDirectory.appendingPathComponent(fileName)
    }

    public var progressFraction: Double {
        guard let totalBytes, totalBytes > 0 else { return completedBytes > 0 ? 0.01 : 0 }
        return min(1, max(0, Double(completedBytes) / Double(totalBytes)))
    }

    public func isEligibleToStart(at date: Date = Date()) -> Bool {
        guard status.isStartable else { return false }

        if let scheduleStart, date < scheduleStart {
            return false
        }

        if let scheduleEnd, date > scheduleEnd {
            return false
        }

        return true
    }
}
