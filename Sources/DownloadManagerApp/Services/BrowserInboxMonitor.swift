import DownloadManagerCore
import Foundation

struct BrowserCaptureMessage: Codable {
    var url: URL
    var referrer: URL?
    var suggestedFileName: String?
    var headers: [String: String]?
    var receivedAt: Date?
}

@MainActor
final class BrowserInboxMonitor {
    private var timer: Timer?
    private var processedLineCount = 0
    var onMessage: ((BrowserCaptureMessage) -> Void)?

    func start() {
        guard timer == nil else { return }
        timer = Timer.scheduledTimer(withTimeInterval: 2, repeats: true) { [weak self] _ in
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
        let url = AppPaths.browserInboxMessages
        guard FileManager.default.fileExists(atPath: url.path),
              let text = try? String(contentsOf: url, encoding: .utf8) else {
            return
        }

        let lines = text.split(separator: "\n", omittingEmptySubsequences: true)
        guard lines.count > processedLineCount else { return }

        let decoder = JSONDecoder.downloadManager
        for line in lines.dropFirst(processedLineCount) {
            guard let data = String(line).data(using: .utf8),
                  let message = try? decoder.decode(BrowserCaptureMessage.self, from: data) else {
                continue
            }
            onMessage?(message)
        }

        processedLineCount = lines.count
    }
}
