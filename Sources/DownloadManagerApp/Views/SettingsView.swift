import DownloadManagerCore
import SwiftUI

struct SettingsView: View {
    @ObservedObject var controller: DownloadController

    var body: some View {
        Form {
            Section("Downloads") {
                TextField("Default folder", text: $controller.settings.defaultDownloadDirectoryPath)
                Stepper(value: $controller.settings.maxSegments, in: 1...16) {
                    Text("Segments per file: \(controller.settings.maxSegments)")
                }
                Picker("Engine", selection: $controller.settings.engineMode) {
                    Text("Automatic").tag(DownloadEngineMode.automatic)
                    Text("Native").tag(DownloadEngineMode.native)
                    Text("aria2").tag(DownloadEngineMode.aria2)
                }
                TextField(
                    "Global speed limit bytes/sec",
                    value: Binding(
                        get: { controller.settings.globalSpeedLimitBytesPerSecond ?? 0 },
                        set: { controller.settings.globalSpeedLimitBytesPerSecond = $0 <= 0 ? nil : $0 }
                    ),
                    format: .number
                )
            }

            Section("Capture") {
                Toggle("Monitor Clipboard", isOn: $controller.settings.clipboardMonitorEnabled)
                Toggle("Enable Browser Inbox", isOn: $controller.settings.browserCaptureEnabled)
            }

            Section("Language") {
                Picker("Preferred language", selection: $controller.settings.languageCode) {
                    Text("System").tag("")
                    Text("English").tag("en")
                    Text("فارسی").tag("fa")
                }
                Text("Language changes are applied by the system localization on next launch.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            Section("aria2") {
                LabeledContent("Bundled path", value: AppPaths.bundledAria2.path)
                Text(FileManager.default.isExecutableFile(atPath: AppPaths.bundledAria2.path) ? "aria2 is available only when selected explicitly. Automatic mode uses the native engine." : "aria2 binary is not bundled yet; Automatic mode uses the native engine.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
        .formStyle(.grouped)
        .padding(20)
    }
}
