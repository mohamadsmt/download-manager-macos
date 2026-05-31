import Foundation

public struct ResourceResolver: Sendable {
    private let session: URLSession

    public init(session: URLSession = .shared) {
        self.session = session
    }

    public func resolve(url: URL, referrer: URL? = nil, headers: [String: String] = [:]) async throws -> DownloadProbe {
        var request = URLRequest(url: url)
        request.httpMethod = "HEAD"
        request.timeoutInterval = 30
        if let referrer {
            request.setValue(referrer.absoluteString, forHTTPHeaderField: "Referer")
        }
        headers.forEach { request.setValue($0.value, forHTTPHeaderField: $0.key) }

        do {
            let (_, response) = try await session.data(for: request)
            return try probe(from: response, fallbackURL: url)
        } catch {
            var fallback = URLRequest(url: url)
            fallback.httpMethod = "GET"
            fallback.timeoutInterval = 30
            fallback.setValue("bytes=0-0", forHTTPHeaderField: "Range")
            if let referrer {
                fallback.setValue(referrer.absoluteString, forHTTPHeaderField: "Referer")
            }
            headers.forEach { fallback.setValue($0.value, forHTTPHeaderField: $0.key) }
            let (_, response) = try await session.data(for: fallback)
            return try probe(from: response, fallbackURL: url)
        }
    }

    private func probe(from response: URLResponse, fallbackURL: URL) throws -> DownloadProbe {
        guard let http = response as? HTTPURLResponse else {
            throw DownloadEngineError.invalidResponse
        }

        guard (200..<400).contains(http.statusCode) || http.statusCode == 206 else {
            throw DownloadEngineError.httpStatus(http.statusCode)
        }

        let finalURL = http.url ?? fallbackURL
        let contentDisposition = http.value(forHTTPHeaderField: "Content-Disposition")
        let mimeType = http.mimeType ?? http.value(forHTTPHeaderField: "Content-Type")
        let contentLength = parseContentLength(from: http)
        let acceptsRanges = http.value(forHTTPHeaderField: "Accept-Ranges")?.lowercased() == "bytes" || http.statusCode == 206

        return DownloadProbe(
            finalURL: finalURL,
            fileName: FilenameResolver.fileName(from: finalURL, contentDisposition: contentDisposition, mimeType: mimeType),
            totalBytes: contentLength,
            acceptsRanges: acceptsRanges,
            etag: http.value(forHTTPHeaderField: "ETag"),
            lastModified: http.value(forHTTPHeaderField: "Last-Modified"),
            mimeType: mimeType
        )
    }

    private func parseContentLength(from response: HTTPURLResponse) -> Int64? {
        if let contentRange = response.value(forHTTPHeaderField: "Content-Range"),
           let slash = contentRange.lastIndex(of: "/") {
            let total = contentRange[contentRange.index(after: slash)...]
            if total != "*", let value = Int64(total) {
                return value
            }
        }

        if let value = response.value(forHTTPHeaderField: "Content-Length") {
            return Int64(value)
        }

        return nil
    }
}
