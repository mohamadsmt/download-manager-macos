import DownloadManagerCore
import SwiftUI

struct DownloadListView: View {
    @ObservedObject var controller: DownloadController

    var body: some View {
        List(selection: $controller.selectedID) {
            ForEach(controller.filteredItems) { item in
                DownloadRowView(item: item)
                    .tag(item.id)
                    .contextMenu {
                        Button("Resume") { controller.resume(id: item.id) }
                        Button("Pause") { controller.pause(id: item.id) }
                        Button("Retry") { controller.retry(id: item.id) }
                        Divider()
                        Button("Reveal in Finder") {
                            controller.selectedID = item.id
                            controller.revealSelectedInFinder()
                        }
                        Button("Cancel", role: .destructive) { controller.cancel(id: item.id) }
                        Button("Remove from List", role: .destructive) { controller.remove(id: item.id) }
                    }
            }
            .onMove(perform: controller.move)
        }
        .overlay {
            if controller.filteredItems.isEmpty {
                ContentUnavailableView("No Downloads", systemImage: "arrow.down.circle", description: Text("Add a URL or import a list to start the queue."))
            }
        }
    }
}

struct DownloadRowView: View {
    let item: DownloadItem

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(alignment: .firstTextBaseline) {
                Image(systemName: iconName)
                    .foregroundStyle(iconColor)
                    .frame(width: 22)

                VStack(alignment: .leading, spacing: 3) {
                    Text(item.fileName)
                        .lineLimit(1)
                        .truncationMode(.middle)
                    Text(item.url.absoluteString)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                        .truncationMode(.middle)
                }

                Spacer(minLength: 16)

                VStack(alignment: .trailing, spacing: 3) {
                    Text(LocalizedStringKey(item.status.rawValue.capitalized))
                        .font(.caption)
                    Text(Formatters.speed(item.speedBytesPerSecond))
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .monospacedDigit()
                }
            }

            HStack(spacing: 10) {
                ProgressView(value: item.progressFraction)
                    .progressViewStyle(.linear)
                    .animation(.easeOut(duration: 0.35), value: item.completedBytes)
                Text("\(Formatters.fileSize(item.completedBytes)) / \(Formatters.fileSize(item.totalBytes))")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .monospacedDigit()
                    .frame(minWidth: 160, alignment: .trailing)
            }
        }
        .padding(.vertical, 6)
    }

    private var iconName: String {
        switch item.status {
        case .completed: return "checkmark.circle.fill"
        case .failed, .cancelled: return "exclamationmark.triangle.fill"
        case .paused: return "pause.circle.fill"
        case .downloading, .resolving: return "arrow.down.circle.fill"
        default: return "clock.fill"
        }
    }

    private var iconColor: Color {
        switch item.status {
        case .completed: return .green
        case .failed, .cancelled: return .red
        case .paused: return .orange
        case .downloading, .resolving: return .blue
        default: return .secondary
        }
    }
}
