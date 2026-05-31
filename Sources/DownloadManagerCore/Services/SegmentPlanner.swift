import Foundation

public struct SegmentPlanner: Sendable {
    public var defaultMaxSegments: Int
    public var absoluteMaxSegments: Int
    public var minimumSegmentSize: Int64

    public init(
        defaultMaxSegments: Int = 8,
        absoluteMaxSegments: Int = 16,
        minimumSegmentSize: Int64 = 1_048_576
    ) {
        self.defaultMaxSegments = defaultMaxSegments
        self.absoluteMaxSegments = absoluteMaxSegments
        self.minimumSegmentSize = minimumSegmentSize
    }

    public func makeSegments(totalBytes: Int64?, acceptsRanges: Bool, requestedMaxSegments: Int? = nil) -> [DownloadSegment] {
        guard acceptsRanges, let totalBytes, totalBytes > 0 else {
            return [
                DownloadSegment(index: 0, range: ByteRange(lowerBound: 0, upperBound: nil))
            ]
        }

        let requested = requestedMaxSegments ?? defaultMaxSegments
        let capped = max(1, min(absoluteMaxSegments, requested))
        let sizeLimited = max(1, min(capped, Int(max(1, totalBytes / minimumSegmentSize))))
        let segmentCount = min(sizeLimited, Int(totalBytes))
        let baseSize = totalBytes / Int64(segmentCount)
        let remainder = totalBytes % Int64(segmentCount)

        var lower: Int64 = 0
        return (0..<segmentCount).map { index in
            let extra: Int64 = Int64(index) < remainder ? 1 : 0
            let size = baseSize + extra
            let upper = lower + size - 1
            defer { lower = upper + 1 }
            return DownloadSegment(
                index: index,
                range: ByteRange(lowerBound: lower, upperBound: upper),
                fileName: "segment-\(index).part"
            )
        }
    }
}
