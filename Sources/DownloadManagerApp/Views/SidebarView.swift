import SwiftUI

struct SidebarView: View {
    @ObservedObject var controller: DownloadController

    var body: some View {
        List(SidebarFilter.allCases, selection: $controller.filter) { filter in
            Label(LocalizedStringKey(filter.title), systemImage: filter.systemImage)
                .tag(filter)
        }
        .listStyle(.sidebar)
        .safeAreaInset(edge: .bottom) {
            VStack(alignment: .leading, spacing: 8) {
                HStack {
                    Image(systemName: "speedometer")
                    Text(Formatters.speed(controller.aggregateSpeed))
                        .monospacedDigit()
                }
                .foregroundStyle(.secondary)

                if let active = controller.activeItem {
                    Text(active.fileName)
                        .font(.caption)
                        .lineLimit(1)
                        .truncationMode(.middle)
                } else {
                    Text("No active download")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            .font(.callout)
            .padding(12)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(.bar)
        }
    }
}
