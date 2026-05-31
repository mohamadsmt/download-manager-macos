import Foundation

public enum DownloadStatus: String, Codable, CaseIterable, Identifiable, Sendable {
    case pending
    case resolving
    case queued
    case scheduled
    case downloading
    case paused
    case completed
    case failed
    case cancelled

    public var id: String { rawValue }

    public var isStartable: Bool {
        switch self {
        case .pending, .queued, .paused, .failed, .scheduled:
            return true
        case .resolving, .downloading, .completed, .cancelled:
            return false
        }
    }

    public var isTerminal: Bool {
        switch self {
        case .completed, .cancelled:
            return true
        default:
            return false
        }
    }
}

public enum DownloadSegmentStatus: String, Codable, CaseIterable, Sendable {
    case pending
    case downloading
    case completed
    case failed
}

public enum DownloadEngineMode: String, Codable, CaseIterable, Identifiable, Sendable {
    case native
    case aria2
    case automatic

    public var id: String { rawValue }
}

public enum DownloadCategory: String, Codable, CaseIterable, Identifiable, Sendable {
    case all
    case active
    case queued
    case completed
    case failed
    case documents
    case archives
    case media
    case software
    case other

    public var id: String { rawValue }

    public static func infer(fileName: String) -> DownloadCategory {
        let ext = URL(fileURLWithPath: fileName).pathExtension.lowercased()

        if ["zip", "rar", "7z", "tar", "gz", "bz2", "xz"].contains(ext) {
            return .archives
        }

        if ["mp4", "mkv", "mov", "avi", "mp3", "wav", "flac", "aac", "m4a"].contains(ext) {
            return .media
        }

        if ["pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "md"].contains(ext) {
            return .documents
        }

        if ["dmg", "pkg", "app", "ipa", "exe", "msi"].contains(ext) {
            return .software
        }

        return .other
    }
}
