import AppKit
import Combine
import DownloadManagerCore
import Foundation

@MainActor
final class DownloadController: ObservableObject {
    @Published var items: [DownloadItem] = []
    @Published var selectedID: DownloadItem.ID?
    @Published var filter: SidebarFilter = .all
    @Published var searchText = ""
    @Published var settings = AppSettings.load() {
        didSet {
            settings.maxSegments = max(1, min(16, settings.maxSegments))
            settings.save()
            configureMonitors()
        }
    }
    @Published var showingAddSheet = false
    @Published var proposedClipboardURL: URL?
    @Published var lastError: String?

    private let fallbackStore = JSONDownloadStore(fileURL: AppPaths.queueJSON)
    private var activeTasks: [DownloadItem.ID: Task<Void, Never>] = [:]
    private let clipboardMonitor = ClipboardMonitor()
    private let browserInboxMonitor = BrowserInboxMonitor()

    init() {
        try? FileManager.default.createDirectory(at: AppPaths.applicationSupport, withIntermediateDirectories: true)
        try? FileManager.default.createDirectory(at: AppPaths.browserInbox, withIntermediateDirectories: true)
        try? FileManager.default.createDirectory(at: AppPaths.defaultDownloadDirectory, withIntermediateDirectories: true)

        clipboardMonitor.onURL = { [weak self] url in
            self?.proposedClipboardURL = url
            self?.showingAddSheet = true
        }
        browserInboxMonitor.onMessage = { [weak self] message in
            self?.addDownload(
                url: message.url,
                referrer: message.referrer,
                suggestedFileName: message.suggestedFileName
            )
        }

        Task {
            await load()
            configureMonitors()
            resumeQueue()
        }
    }

    var filteredItems: [DownloadItem] {
        items
            .filter { filter.includes($0) }
            .filter { item in
                guard !searchText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return true }
                let needle = searchText.lowercased()
                return item.fileName.lowercased().contains(needle) || item.url.absoluteString.lowercased().contains(needle)
            }
    }

    var selectedItem: DownloadItem? {
        guard let selectedID else { return nil }
        return items.first { $0.id == selectedID }
    }

    var activeItem: DownloadItem? {
        items.first { $0.status == .downloading || $0.status == .resolving }
    }

    var aggregateSpeed: Int64 {
        items.reduce(0) { $0 + $1.speedBytesPerSecond }
    }

    func load() async {
        do {
            items = try await fallbackStore.load()
            normalizeLoadedQueue()
        } catch {
            lastError = error.localizedDescription
        }
    }

    func configureMonitors() {
        settings.clipboardMonitorEnabled ? clipboardMonitor.start() : clipboardMonitor.stop()
        settings.browserCaptureEnabled ? browserInboxMonitor.start() : browserInboxMonitor.stop()
    }

    func addDownload(urlString: String, startImmediately: Bool = true) {
        let urls = urlString
            .components(separatedBy: .newlines)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .compactMap(URL.init(string:))
            .filter { ["http", "https"].contains($0.scheme?.lowercased() ?? "") }

        for url in urls {
            addDownload(url: url, referrer: nil, suggestedFileName: nil, startImmediately: false)
        }

        if startImmediately {
            resumeQueue()
        }
    }

    func addDownload(
        url: URL,
        referrer: URL?,
        suggestedFileName: String?,
        startImmediately: Bool = true
    ) {
        guard ["http", "https"].contains(url.scheme?.lowercased() ?? "") else { return }

        var item = DownloadItem(
            url: url,
            referrer: referrer,
            suggestedFileName: suggestedFileName,
            destinationDirectory: settings.defaultDownloadDirectory,
            status: .queued,
            engineMode: settings.engineMode,
            speedLimitBytesPerSecond: nil
        )
        item.updatedAt = Date()
        items.append(item)
        selectedID = item.id
        persist(item)

        if startImmediately {
            resumeQueue()
        }
    }

    func importTextFile(url: URL) {
        guard let text = try? String(contentsOf: url, encoding: .utf8) else { return }
        addDownload(urlString: text)
    }

    func move(from source: IndexSet, to destination: Int) {
        let moving = source.sorted().map { items[$0] }
        for index in source.sorted(by: >) {
            items.remove(at: index)
        }
        let adjustedDestination = destination - source.filter { $0 < destination }.count
        items.insert(contentsOf: moving, at: max(0, min(adjustedDestination, items.count)))
        persistAll()
    }

    func pause(id: DownloadItem.ID) {
        activeTasks[id]?.cancel()
        activeTasks[id] = nil
        update(id: id) { item in
            item.status = .paused
            item.speedBytesPerSecond = 0
            item.updatedAt = Date()
        }
        startQueueIfNeeded()
    }

    func resume(id: DownloadItem.ID) {
        update(id: id) { item in
            item.status = .queued
            item.errorMessage = nil
            item.updatedAt = Date()
        }
        startQueueIfNeeded()
    }

    func cancel(id: DownloadItem.ID) {
        activeTasks[id]?.cancel()
        activeTasks[id] = nil
        update(id: id) { item in
            item.status = .cancelled
            item.speedBytesPerSecond = 0
            item.updatedAt = Date()
        }
        startQueueIfNeeded()
    }

    func removeSelected() {
        guard let selectedID else { return }
        remove(id: selectedID)
    }

    func remove(id: DownloadItem.ID) {
        let removedIndex = items.firstIndex { $0.id == id }
        let removedItem = removedIndex.flatMap { items[$0] }

        activeTasks[id]?.cancel()
        activeTasks[id] = nil

        items.removeAll { $0.id == id }
        if selectedID == id {
            selectedID = nextSelection(afterRemovingIndex: removedIndex)
        }

        if let removedItem {
            removeWorkingFiles(for: removedItem)
        }

        Task {
            do {
                try await fallbackStore.remove(id: id)
            } catch {
                lastError = error.localizedDescription
            }
        }

        startQueueIfNeeded()
    }

    func retry(id: DownloadItem.ID) {
        update(id: id) { item in
            item.status = .queued
            item.errorMessage = nil
            item.updatedAt = Date()
        }
        startQueueIfNeeded()
    }

    func pauseAll() {
        for id in activeTasks.keys {
            pause(id: id)
        }
    }

    func resumeQueue() {
        for item in items where shouldRequeueOnResume(item) {
            update(id: item.id) { mutable in
                mutable.status = .queued
                mutable.speedBytesPerSecond = 0
                mutable.updatedAt = Date()
            }
        }
        startQueueIfNeeded()
    }

    func revealSelectedInFinder() {
        guard let selectedItem else { return }
        NSWorkspace.shared.activateFileViewerSelecting([selectedItem.destinationFileURL])
    }

    func handleIncomingURL(_ url: URL) {
        guard url.scheme == "downloadmanager",
              url.host == "add",
              let components = URLComponents(url: url, resolvingAgainstBaseURL: false),
              let rawURL = components.queryItems?.first(where: { $0.name == "url" })?.value,
              let downloadURL = URL(string: rawURL) else {
            return
        }

        let referrer = components.queryItems?.first(where: { $0.name == "referrer" })?.value.flatMap(URL.init(string:))
        let name = components.queryItems?.first(where: { $0.name == "filename" })?.value
        addDownload(url: downloadURL, referrer: referrer, suggestedFileName: name)
    }

    private func startQueueIfNeeded() {
        guard activeTasks.count < 1 else { return }
        guard let nextID = DownloadQueue(items: items).nextEligibleID() else { return }
        guard activeTasks[nextID] == nil, let item = items.first(where: { $0.id == nextID }) else { return }

        update(id: nextID) { mutable in
            mutable.status = .resolving
            mutable.errorMessage = nil
            mutable.speedBytesPerSecond = 0
            mutable.updatedAt = Date()
        }

        let engine = makeEngine(for: item.engineMode)
        let options = DownloadOptions(
            maxSegments: settings.maxSegments,
            globalSpeedLimitBytesPerSecond: settings.globalSpeedLimitBytesPerSecond,
            additionalHeaders: [:],
            workingDirectory: AppPaths.workingDirectory
        )

        activeTasks[nextID] = Task { [weak self] in
            guard let self else { return }

            do {
                let latestItem = self.item(id: nextID) ?? item
                let completed = try await engine.download(item: latestItem, options: options) { event in
                    await self.apply(progress: event)
                }
                self.finish(completed)
            } catch is CancellationError {
                self.markPausedIfNeeded(id: nextID)
            } catch {
                self.fail(id: nextID, error: error)
            }
        }
    }

    private func makeEngine(for mode: DownloadEngineMode) -> any DownloadEngine {
        switch mode {
        case .native:
            return NativeSegmentedDownloadEngine()
        case .aria2:
            return Aria2DownloadEngine(executableURL: AppPaths.bundledAria2)
        case .automatic:
            if FileManager.default.isExecutableFile(atPath: AppPaths.bundledAria2.path) {
                return Aria2DownloadEngine(executableURL: AppPaths.bundledAria2)
            }
            return NativeSegmentedDownloadEngine()
        }
    }

    private func item(id: DownloadItem.ID) -> DownloadItem? {
        items.first { $0.id == id }
    }

    private func apply(progress: DownloadProgressEvent) {
        update(id: progress.itemID) { item in
            item.completedBytes = progress.completedBytes
            item.totalBytes = progress.totalBytes ?? item.totalBytes
            item.speedBytesPerSecond = progress.speedBytesPerSecond
            item.segments = progress.segments
            item.status = progress.status
            item.fileName = progress.fileName ?? item.fileName
            item.category = progress.category ?? item.category
            item.acceptsRanges = progress.acceptsRanges ?? item.acceptsRanges
            item.etag = progress.etag ?? item.etag
            item.lastModified = progress.lastModified ?? item.lastModified
            item.updatedAt = Date()
        }
    }

    private func finish(_ completed: DownloadItem) {
        activeTasks[completed.id] = nil
        update(id: completed.id) { item in
            item = completed
        }
        startQueueIfNeeded()
    }

    private func markPausedIfNeeded(id: DownloadItem.ID) {
        activeTasks[id] = nil
        update(id: id) { item in
            if item.status != .cancelled {
                item.status = .paused
                item.speedBytesPerSecond = 0
                item.updatedAt = Date()
            }
        }
        startQueueIfNeeded()
    }

    private func fail(id: DownloadItem.ID, error: Error) {
        activeTasks[id] = nil
        update(id: id) { item in
            item.status = .failed
            item.errorMessage = error.localizedDescription
            item.speedBytesPerSecond = 0
            item.updatedAt = Date()
        }
        startQueueIfNeeded()
    }

    private func update(id: DownloadItem.ID, mutate: (inout DownloadItem) -> Void) {
        guard let index = items.firstIndex(where: { $0.id == id }) else { return }
        mutate(&items[index])
        persist(items[index])
    }

    private func persist(_ item: DownloadItem) {
        Task {
            do {
                try await fallbackStore.upsert(item)
            } catch {
                lastError = error.localizedDescription
            }
        }
    }

    private func persistAll() {
        let snapshot = items
        Task {
            do {
                try await fallbackStore.replaceAll(snapshot)
            } catch {
                lastError = error.localizedDescription
            }
        }
    }

    private func shouldRequeueOnResume(_ item: DownloadItem) -> Bool {
        switch item.status {
        case .paused, .failed, .pending:
            return true
        case .downloading, .resolving:
            return activeTasks[item.id] == nil
        case .queued, .scheduled, .completed, .cancelled:
            return false
        }
    }

    private func normalizeLoadedQueue() {
        var changed = false

        for index in items.indices {
            switch items[index].status {
            case .downloading, .resolving:
                items[index].status = .queued
                items[index].speedBytesPerSecond = 0
                items[index].updatedAt = Date()
                changed = true
            default:
                break
            }
        }

        if changed {
            persistAll()
        }
    }

    private func nextSelection(afterRemovingIndex removedIndex: Int?) -> DownloadItem.ID? {
        guard !items.isEmpty else { return nil }
        guard let removedIndex else { return items.first?.id }
        let index = min(removedIndex, items.count - 1)
        return items[index].id
    }

    private func removeWorkingFiles(for item: DownloadItem) {
        let workingDirectory = AppPaths.workingDirectory.appendingPathComponent(item.id.uuidString, isDirectory: true)
        try? FileManager.default.removeItem(at: workingDirectory)
    }
}
