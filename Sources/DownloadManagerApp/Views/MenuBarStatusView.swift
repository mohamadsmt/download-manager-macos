import SwiftUI

struct MenuBarStatusView: View {
    @ObservedObject var controller: DownloadController

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            if let active = controller.activeItem {
                Text(active.fileName)
                    .lineLimit(1)
                    .truncationMode(.middle)
                ProgressView(value: active.progressFraction)
                Text(Formatters.speed(active.speedBytesPerSecond))
                    .foregroundStyle(.secondary)
            } else {
                Label("Idle", systemImage: "checkmark.circle")
            }

            Divider()

            Button("Add Download") {
                controller.showingAddSheet = true
            }
            Button("Pause All") {
                controller.pauseAll()
            }
            Button("Resume Queue") {
                controller.resumeQueue()
            }
        }
        .padding(8)
        .frame(width: 260)
    }
}
