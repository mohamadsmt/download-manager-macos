import Foundation

public struct ByteRange: Codable, Hashable, Sendable {
    public var lowerBound: Int64
    public var upperBound: Int64?

    public init(lowerBound: Int64, upperBound: Int64?) {
        self.lowerBound = lowerBound
        self.upperBound = upperBound
    }

    public var headerValue: String? {
        if let upperBound {
            return "bytes=\(lowerBound)-\(upperBound)"
        }

        if lowerBound > 0 {
            return "bytes=\(lowerBound)-"
        }

        return nil
    }

    public var length: Int64? {
        guard let upperBound else { return nil }
        return max(0, upperBound - lowerBound + 1)
    }
}
