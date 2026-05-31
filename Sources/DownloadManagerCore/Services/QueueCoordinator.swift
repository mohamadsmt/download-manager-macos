import Foundation

public struct QueueCoordinator: Sendable {
    public var globalConcurrency: Int

    public init(globalConcurrency: Int = 1) {
        self.globalConcurrency = max(1, globalConcurrency)
    }

    public func runningCount(in items: [DownloadItem]) -> Int {
        items.filter { $0.status == .downloading || $0.status == .resolving }.count
    }

    public func canStartMore(in items: [DownloadItem]) -> Bool {
        runningCount(in: items) < globalConcurrency
    }

    public func nextStartableID(in items: [DownloadItem], at date: Date = Date()) -> DownloadItem.ID? {
        guard canStartMore(in: items) else { return nil }
        return DownloadQueue(items: items).nextEligibleID(at: date)
    }
}
