import DownloadManagerCore
import Foundation

struct AppSettings: Codable, Equatable {
    var defaultDownloadDirectoryPath: String
    var maxSegments: Int
    var globalSpeedLimitBytesPerSecond: Int64?
    var engineMode: DownloadEngineMode
    var clipboardMonitorEnabled: Bool
    var browserCaptureEnabled: Bool
    var languageCode: String

    static var defaults: AppSettings {
        AppSettings(
            defaultDownloadDirectoryPath: AppPaths.defaultDownloadDirectory.path,
            maxSegments: 8,
            globalSpeedLimitBytesPerSecond: nil,
            engineMode: .automatic,
            clipboardMonitorEnabled: true,
            browserCaptureEnabled: true,
            languageCode: Locale.current.language.languageCode?.identifier ?? "en"
        )
    }

    static func load() -> AppSettings {
        guard let data = UserDefaults.standard.data(forKey: "AppSettings"),
              let settings = try? JSONDecoder.downloadManager.decode(AppSettings.self, from: data) else {
            return .defaults
        }
        return settings
    }

    func save() {
        if let data = try? JSONEncoder.downloadManager.encode(self) {
            UserDefaults.standard.set(data, forKey: "AppSettings")
        }
    }

    var defaultDownloadDirectory: URL {
        URL(fileURLWithPath: defaultDownloadDirectoryPath, isDirectory: true)
    }
}
