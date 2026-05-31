import AppKit
import Foundation

@MainActor
final class ClipboardMonitor {
    private var timer: Timer?
    private var lastChangeCount = NSPasteboard.general.changeCount
    private var recentlySeen = Set<String>()
    var onURL: ((URL) -> Void)?

    func start() {
        guard timer == nil else { return }
        timer = Timer.scheduledTimer(withTimeInterval: 1.5, repeats: true) { [weak self] _ in
            Task { @MainActor in
                self?.poll()
            }
        }
    }

    func stop() {
        timer?.invalidate()
        timer = nil
    }

    private func poll() {
        let pasteboard = NSPasteboard.general
        guard pasteboard.changeCount != lastChangeCount else { return }
        lastChangeCount = pasteboard.changeCount

        guard let string = pasteboard.string(forType: .string),
              let url = URL(string: string.trimmingCharacters(in: .whitespacesAndNewlines)),
              ["http", "https"].contains(url.scheme?.lowercased() ?? "") else {
            return
        }

        guard !recentlySeen.contains(url.absoluteString) else { return }
        recentlySeen.insert(url.absoluteString)
        if recentlySeen.count > 30 {
            recentlySeen.remove(recentlySeen.first!)
        }
        onURL?(url)
    }
}
