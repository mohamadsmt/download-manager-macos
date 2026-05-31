import DownloadManagerCore
import SwiftUI

struct ContentView: View {
    @ObservedObject var controller: DownloadController
    @State private var importingTextFile = false

    var body: some View {
        NavigationSplitView {
            SidebarView(controller: controller)
                .navigationSplitViewColumnWidth(min: 220, ideal: 250, max: 300)
        } content: {
            DownloadListView(controller: controller)
                .navigationTitle(LocalizedStringKey(controller.filter.title))
                .toolbar {
                    ToolbarItemGroup {
                        Button {
                            controller.showingAddSheet = true
                        } label: {
                            Label("Add", systemImage: "plus")
                        }
                        .keyboardShortcut("n", modifiers: [.command])

                        Button {
                            importingTextFile = true
                        } label: {
                            Label("Import", systemImage: "square.and.arrow.down")
                        }

                        Divider()

                        Button {
                            if let id = controller.selectedID {
                                controller.pause(id: id)
                            }
                        } label: {
                            Label("Pause", systemImage: "pause.fill")
                        }
                        .disabled(controller.selectedID == nil)

                        Button {
                            if let id = controller.selectedID {
                                controller.resume(id: id)
                            }
                        } label: {
                            Label("Resume", systemImage: "play.fill")
                        }
                        .disabled(controller.selectedID == nil)

                        Button {
                            if let id = controller.selectedID {
                                controller.cancel(id: id)
                            }
                        } label: {
                            Label("Cancel", systemImage: "xmark")
                        }
                        .disabled(controller.selectedID == nil)

                        Button {
                            controller.removeSelected()
                        } label: {
                            Label("Remove", systemImage: "trash")
                        }
                        .disabled(controller.selectedID == nil)
                    }
                }
        } detail: {
            DownloadDetailView(item: controller.selectedItem, controller: controller)
        }
        .searchable(text: $controller.searchText, prompt: "Search downloads")
        .sheet(isPresented: $controller.showingAddSheet) {
            AddDownloadSheet(controller: controller)
        }
        .fileImporter(
            isPresented: $importingTextFile,
            allowedContentTypes: [.plainText],
            allowsMultipleSelection: false
        ) { result in
            if case .success(let urls) = result, let url = urls.first {
                controller.importTextFile(url: url)
            }
        }
        .alert("Download Manager", isPresented: Binding(
            get: { controller.lastError != nil },
            set: { if !$0 { controller.lastError = nil } }
        )) {
            Button("OK", role: .cancel) {}
        } message: {
            Text(controller.lastError ?? "")
        }
    }
}
