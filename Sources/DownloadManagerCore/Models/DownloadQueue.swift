import Foundation

public struct DownloadQueue: Codable, Sendable {
    public private(set) var items: [DownloadItem]

    public init(items: [DownloadItem] = []) {
        self.items = items
    }

    public mutating func append(_ item: DownloadItem) {
        items.append(item)
    }

    public mutating func replace(_ item: DownloadItem) {
        guard let index = items.firstIndex(where: { $0.id == item.id }) else {
            items.append(item)
            return
        }

        items[index] = item
    }

    public mutating func remove(id: DownloadItem.ID) {
        items.removeAll { $0.id == id }
    }

    public mutating func move(from source: IndexSet, to destination: Int) {
        let moving = source.sorted().map { items[$0] }
        for index in source.sorted(by: >) {
            items.remove(at: index)
        }

        let adjustedDestination = destination - source.filter { $0 < destination }.count
        items.insert(contentsOf: moving, at: max(0, min(adjustedDestination, items.count)))
    }

    public func nextEligibleID(at date: Date = Date()) -> DownloadItem.ID? {
        items
            .sorted { lhs, rhs in
                if lhs.priority != rhs.priority {
                    return lhs.priority > rhs.priority
                }
                return lhs.createdAt < rhs.createdAt
            }
            .first { $0.isEligibleToStart(at: date) }?
            .id
    }
}
