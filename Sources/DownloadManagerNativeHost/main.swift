import DownloadManagerCore
import Foundation

struct NativeMessage: Codable {
    var url: URL
    var referrer: URL?
    var suggestedFileName: String?
    var headers: [String: String]?
    var receivedAt: Date?
}

struct NativeResponse: Codable {
    var ok: Bool
    var message: String
}

let stdinHandle = FileHandle.standardInput
let stdoutHandle = FileHandle.standardOutput

func applicationSupport() -> URL {
    let base = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first!
    return base.appendingPathComponent("Download Manager", isDirectory: true)
}

func inboxURL() -> URL {
    applicationSupport()
        .appendingPathComponent("BrowserInbox", isDirectory: true)
        .appendingPathComponent("messages.jsonl")
}

func readMessage() throws -> NativeMessage? {
    let lengthData = stdinHandle.readData(ofLength: 4)
    guard lengthData.count == 4 else { return nil }

    let length = lengthData.withUnsafeBytes { rawBuffer in
        rawBuffer.load(as: UInt32.self).littleEndian
    }

    guard length > 0, length < 4_000_000 else {
        throw DownloadEngineError.fileSystem("Invalid native-message length.")
    }

    let payload = stdinHandle.readData(ofLength: Int(length))
    guard payload.count == Int(length) else {
        throw DownloadEngineError.fileSystem("Native-message payload was truncated.")
    }

    return try JSONDecoder.downloadManager.decode(NativeMessage.self, from: payload)
}

func append(_ message: NativeMessage) throws {
    let url = inboxURL()
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
    let data = try JSONEncoder.downloadManager.encode(message)

    if !FileManager.default.fileExists(atPath: url.path) {
        FileManager.default.createFile(atPath: url.path, contents: nil)
    }

    let handle = try FileHandle(forWritingTo: url)
    defer { try? handle.close() }
    try handle.seekToEnd()
    try handle.write(contentsOf: data)
    try handle.write(contentsOf: Data("\n".utf8))
}

func writeResponse(_ response: NativeResponse) throws {
    let payload = try JSONEncoder.downloadManager.encode(response)
    var length = UInt32(payload.count).littleEndian
    let lengthData = Data(bytes: &length, count: 4)
    try stdoutHandle.write(contentsOf: lengthData)
    try stdoutHandle.write(contentsOf: payload)
}

do {
    if let message = try readMessage() {
        try append(message)
        try writeResponse(NativeResponse(ok: true, message: "queued"))
    }
} catch {
    try? writeResponse(NativeResponse(ok: false, message: error.localizedDescription))
    exit(1)
}
