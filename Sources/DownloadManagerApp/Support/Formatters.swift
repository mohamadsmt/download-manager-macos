import Foundation

enum Formatters {
    static let bytes: ByteCountFormatter = {
        let formatter = ByteCountFormatter()
        formatter.countStyle = .file
        formatter.includesActualByteCount = false
        return formatter
    }()

    static let relative: RelativeDateTimeFormatter = {
        let formatter = RelativeDateTimeFormatter()
        formatter.unitsStyle = .abbreviated
        return formatter
    }()

    static func fileSize(_ value: Int64?) -> String {
        guard let value else { return "Unknown" }
        return bytes.string(fromByteCount: value)
    }

    static func speed(_ value: Int64) -> String {
        guard value > 0 else { return "Idle" }
        return "\(bytes.string(fromByteCount: value))/s"
    }
}
