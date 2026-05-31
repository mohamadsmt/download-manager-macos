import SwiftUI

struct AddDownloadSheet: View {
    @ObservedObject var controller: DownloadController
    @Environment(\.dismiss) private var dismiss
    @State private var urlText = ""

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            HStack {
                Image(systemName: "plus.circle.fill")
                    .font(.title2)
                    .foregroundStyle(.blue)
                Text("Add Downloads")
                    .font(.title3.weight(.semibold))
            }

            TextEditor(text: $urlText)
                .font(.system(.body, design: .monospaced))
                .frame(width: 560, height: 180)
                .overlay(
                    RoundedRectangle(cornerRadius: 6)
                        .stroke(.separator, lineWidth: 1)
                )
                .onAppear {
                    if urlText.isEmpty, let proposed = controller.proposedClipboardURL {
                        urlText = proposed.absoluteString
                        controller.proposedClipboardURL = nil
                    }
                }

            HStack {
                VStack(alignment: .leading, spacing: 4) {
                    Text("Destination")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Text(controller.settings.defaultDownloadDirectoryPath)
                        .font(.caption)
                        .lineLimit(1)
                        .truncationMode(.middle)
                }

                Spacer()

                Button("Cancel") {
                    dismiss()
                }
                .keyboardShortcut(.cancelAction)

                Button("Add to Queue") {
                    controller.addDownload(urlString: urlText)
                    dismiss()
                }
                .keyboardShortcut(.defaultAction)
                .disabled(urlText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
            }
        }
        .padding(22)
    }
}
