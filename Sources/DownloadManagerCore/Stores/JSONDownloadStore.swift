import Foundation

public actor JSONDownloadStore {
    private let fileURL: URL
    private var items: [DownloadItem] = []

    public init(fileURL: URL) {
        self.fileURL = fileURL
    }

    public func load() throws -> [DownloadItem] {
        guard FileManager.default.fileExists(atPath: fileURL.path) else {
            items = []
            return []
        }

        let data = try Data(contentsOf: fileURL)
        items = try JSONDecoder.downloadManager.decode([DownloadItem].self, from: data)
        return items
    }

    public func all() -> [DownloadItem] {
        items
    }

    public func upsert(_ item: DownloadItem) throws {
        if let index = items.firstIndex(where: { $0.id == item.id }) {
            items[index] = item
        } else {
            items.append(item)
        }
        try save()
    }

    public func replaceAll(_ newItems: [DownloadItem]) throws {
        items = newItems
        try save()
    }

    public func remove(id: DownloadItem.ID) throws {
        items.removeAll { $0.id == id }
        try save()
    }

    private func save() throws {
        try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        let data = try JSONEncoder.downloadManager.encode(items)
        try data.write(to: fileURL, options: [.atomic])
    }
}
