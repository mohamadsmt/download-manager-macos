import Foundation

public struct DownloadSegment: Identifiable, Codable, Hashable, Sendable {
    public var id: UUID
    public var index: Int
    public var range: ByteRange
    public var bytesWritten: Int64
    public var status: DownloadSegmentStatus
    public var fileName: String

    public init(
        id: UUID = UUID(),
        index: Int,
        range: ByteRange,
        bytesWritten: Int64 = 0,
        status: DownloadSegmentStatus = .pending,
        fileName: String? = nil
    ) {
        self.id = id
        self.index = index
        self.range = range
        self.bytesWritten = bytesWritten
        self.status = status
        self.fileName = fileName ?? "segment-\(index).part"
    }

    public var expectedLength: Int64? {
        range.length
    }
}
