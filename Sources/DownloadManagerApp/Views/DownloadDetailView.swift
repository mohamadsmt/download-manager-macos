import DownloadManagerCore
import SwiftUI

struct DownloadDetailView: View {
    let item: DownloadItem?
    @ObservedObject var controller: DownloadController

    var body: some View {
        Group {
            if let item {
                ScrollView {
                    VStack(alignment: .leading, spacing: 18) {
                        header(item)
                        stats(item)
                        segments(item)
                        metadata(item)
                    }
                    .padding(24)
                    .frame(maxWidth: .infinity, alignment: .leading)
                }
            } else {
                ContentUnavailableView("Select a Download", systemImage: "sidebar.right", description: Text("Choose an item to inspect progress, segments, and source metadata."))
            }
        }
        .toolbar {
            ToolbarItemGroup {
                Button {
                    controller.revealSelectedInFinder()
                } label: {
                    Label("Reveal", systemImage: "finder")
                }
                .disabled(item == nil)
            }
        }
    }

    private func header(_ item: DownloadItem) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(item.fileName)
                .font(.title2.weight(.semibold))
                .lineLimit(2)
            Text(item.url.absoluteString)
                .font(.callout)
                .foregroundStyle(.secondary)
                .textSelection(.enabled)
            ProgressView(value: item.progressFraction)
                .progressViewStyle(.linear)
                .animation(.easeOut(duration: 0.35), value: item.completedBytes)
        }
    }

    private func stats(_ item: DownloadItem) -> some View {
        Grid(alignment: .leading, horizontalSpacing: 28, verticalSpacing: 10) {
            GridRow {
                label("Status")
                value(item.status.rawValue.capitalized)
                label("Speed")
                value(Formatters.speed(item.speedBytesPerSecond))
            }
            GridRow {
                label("Size")
                value(Formatters.fileSize(item.totalBytes))
                label("Completed")
                value(Formatters.fileSize(item.completedBytes))
            }
            GridRow {
                label("Engine")
                value(item.engineMode.rawValue)
                label("Destination")
                Text(item.destinationDirectory.path)
                    .lineLimit(1)
                    .truncationMode(.middle)
                    .textSelection(.enabled)
            }
        }
        .padding(14)
        .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 8))
    }

    private func segments(_ item: DownloadItem) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Segments")
                .font(.headline)
            if item.segments.isEmpty {
                Text("Segments are created after the download resolves server Range support.")
                    .foregroundStyle(.secondary)
            } else {
                ForEach(item.segments) { segment in
                    HStack {
                        Text("#\(segment.index + 1)")
                            .font(.caption.monospacedDigit())
                            .frame(width: 48, alignment: .leading)
                        ProgressView(value: segmentProgress(segment))
                            .animation(.easeOut(duration: 0.35), value: segment.bytesWritten)
                        Text(segmentStatusTitle(segment))
                            .font(.caption)
                            .frame(width: 86, alignment: .trailing)
                            .contentTransition(.opacity)
                    }
                }
            }
        }
    }

    private func metadata(_ item: DownloadItem) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Source Metadata")
                .font(.headline)
            if let etag = item.etag {
                Text("ETag: \(etag)").textSelection(.enabled)
            }
            if let lastModified = item.lastModified {
                Text("Last-Modified: \(lastModified)").textSelection(.enabled)
            }
            Text("Range support: \(item.acceptsRanges ? "Yes" : "No")")
            if let error = item.errorMessage {
                Text(error)
                    .foregroundStyle(.red)
                    .textSelection(.enabled)
            }
        }
        .font(.callout)
    }

    private func label(_ text: String) -> some View {
        Text(LocalizedStringKey(text))
            .foregroundStyle(.secondary)
    }

    private func value(_ text: String) -> some View {
        Text(LocalizedStringKey(text))
            .monospacedDigit()
    }

    private func segmentProgress(_ segment: DownloadSegment) -> Double {
        guard let expected = segment.expectedLength, expected > 0 else {
            return segment.bytesWritten > 0 ? 0.01 : 0
        }
        return min(1, Double(segment.bytesWritten) / Double(expected))
    }

    private func segmentStatusTitle(_ segment: DownloadSegment) -> String {
        switch segment.status {
        case .completed:
            return "Done"
        case .failed:
            return "Failed"
        case .downloading:
            return "Active"
        case .pending:
            return segment.bytesWritten > 0 ? "Active" : "Waiting"
        }
    }
}
