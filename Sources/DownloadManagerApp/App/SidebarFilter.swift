import DownloadManagerCore
import Foundation

enum SidebarFilter: String, CaseIterable, Identifiable, Hashable {
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

    var id: String { rawValue }

    var title: String {
        switch self {
        case .all: return "All Downloads"
        case .active: return "Active"
        case .queued: return "Queue"
        case .completed: return "Completed"
        case .failed: return "Failed"
        case .documents: return "Documents"
        case .archives: return "Archives"
        case .media: return "Media"
        case .software: return "Software"
        case .other: return "Other"
        }
    }

    var systemImage: String {
        switch self {
        case .all: return "tray.full"
        case .active: return "arrow.down.circle"
        case .queued: return "text.line.first.and.arrowtriangle.forward"
        case .completed: return "checkmark.circle"
        case .failed: return "exclamationmark.triangle"
        case .documents: return "doc"
        case .archives: return "archivebox"
        case .media: return "play.rectangle"
        case .software: return "shippingbox"
        case .other: return "folder"
        }
    }

    func includes(_ item: DownloadItem) -> Bool {
        switch self {
        case .all:
            return true
        case .active:
            return item.status == .downloading || item.status == .resolving
        case .queued:
            return item.status == .pending || item.status == .queued || item.status == .scheduled || item.status == .paused
        case .completed:
            return item.status == .completed
        case .failed:
            return item.status == .failed || item.status == .cancelled
        case .documents:
            return item.category == .documents
        case .archives:
            return item.category == .archives
        case .media:
            return item.category == .media
        case .software:
            return item.category == .software
        case .other:
            return item.category == .other
        }
    }
}
