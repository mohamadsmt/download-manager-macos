import AppKit
import DownloadManagerCore
import SwiftUI

@main
struct DownloadManagerApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate
    @StateObject private var controller = DownloadController()

    var body: some Scene {
        WindowGroup("Download Manager") {
            ContentView(controller: controller)
                .frame(minWidth: 1040, minHeight: 680)
                .onOpenURL { url in
                    controller.handleIncomingURL(url)
                }
        }
        .commands {
            CommandGroup(after: .newItem) {
                Button("Add Download") {
                    controller.showingAddSheet = true
                }
                .keyboardShortcut("n", modifiers: [.command])

                Button("Pause All") {
                    controller.pauseAll()
                }
                .keyboardShortcut("p", modifiers: [.command, .option])

                Button("Resume Queue") {
                    controller.resumeQueue()
                }
                .keyboardShortcut("r", modifiers: [.command, .option])

                Button("Remove from List") {
                    controller.removeSelected()
                }
                .keyboardShortcut(.delete, modifiers: [])
                .disabled(controller.selectedID == nil)
            }
        }

        Settings {
            SettingsView(controller: controller)
                .frame(width: 520)
        }

        MenuBarExtra("Download Manager", systemImage: "arrow.down.circle") {
            MenuBarStatusView(controller: controller)
        }
    }
}

final class AppDelegate: NSObject, NSApplicationDelegate {
    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApp.setActivationPolicy(.regular)
        NSApp.activate(ignoringOtherApps: true)
    }
}
