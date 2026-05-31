import Foundation

public struct DownloadProbe: Codable, Hashable, Sendable {
    public var finalURL: URL
    public var fileName: String
    public var totalBytes: Int64?
    public var acceptsRanges: Bool
    public var etag: String?
    public var lastModified: String?
    public var mimeType: String?

    public init(
        finalURL: URL,
        fileName: String,
        totalBytes: Int64?,
        acceptsRanges: Bool,
        etag: String?,
        lastModified: String?,
        mimeType: String?
    ) {
        self.finalURL = finalURL
        self.fileName = fileName
        self.totalBytes = totalBytes
        self.acceptsRanges = acceptsRanges
        self.etag = etag
        self.lastModified = lastModified
        self.mimeType = mimeType
    }
}
