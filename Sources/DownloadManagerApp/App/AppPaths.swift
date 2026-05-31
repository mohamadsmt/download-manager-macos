import Foundation

enum AppPaths {
    static var applicationSupport: URL {
        let base = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first!
        return base.appendingPathComponent("Download Manager", isDirectory: true)
    }

    static var defaultDownloadDirectory: URL {
        let base = FileManager.default.urls(for: .downloadsDirectory, in: .userDomainMask).first!
        return base.appendingPathComponent("Download Manager", isDirectory: true)
    }

    static var queueJSON: URL {
        applicationSupport.appendingPathComponent("downloads.json")
    }

    static var swiftDataStore: URL {
        applicationSupport.appendingPathComponent("Downloads.store")
    }

    static var workingDirectory: URL {
        applicationSupport.appendingPathComponent("Working", isDirectory: true)
    }

    static var browserInbox: URL {
        applicationSupport.appendingPathComponent("BrowserInbox", isDirectory: true)
    }

    static var browserInboxMessages: URL {
        browserInbox.appendingPathComponent("messages.jsonl")
    }

    static var bundledAria2: URL {
        Bundle.main.resourceURL?
            .appendingPathComponent("Vendor", isDirectory: true)
            .appendingPathComponent("aria2", isDirectory: true)
            .appendingPathComponent("aria2c") ?? applicationSupport.appendingPathComponent("aria2c")
    }
}
