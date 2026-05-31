import Foundation

public struct SidecarManifest: Codable, Sendable {
    public var itemID: UUID
    public var url: URL
    public var fileName: String
    public var totalBytes: Int64?
    public var etag: String?
    public var lastModified: String?
    public var acceptsRanges: Bool
    public var segments: [DownloadSegment]
    public var updatedAt: Date

    public init(item: DownloadItem) {
        self.itemID = item.id
        self.url = item.url
        self.fileName = item.fileName
        self.totalBytes = item.totalBytes
        self.etag = item.etag
        self.lastModified = item.lastModified
        self.acceptsRanges = item.acceptsRanges
        self.segments = item.segments
        self.updatedAt = Date()
    }
}

public struct SidecarManifestStore: Sendable {
    public var directory: URL

    public init(directory: URL) {
        self.directory = directory
    }

    public func manifestURL(for id: UUID) -> URL {
        directory.appendingPathComponent("\(id.uuidString).json")
    }

    public func load(id: UUID) throws -> SidecarManifest? {
        let url = manifestURL(for: id)
        guard FileManager.default.fileExists(atPath: url.path) else { return nil }
        let data = try Data(contentsOf: url)
        return try JSONDecoder.downloadManager.decode(SidecarManifest.self, from: data)
    }

    public func save(_ manifest: SidecarManifest) throws {
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let data = try JSONEncoder.downloadManager.encode(manifest)
        try data.write(to: manifestURL(for: manifest.itemID), options: [.atomic])
    }
}
