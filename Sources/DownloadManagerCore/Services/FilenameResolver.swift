import Foundation

public enum FilenameResolver {
    public static func fileName(from url: URL, contentDisposition: String? = nil, mimeType: String? = nil) -> String {
        if let headerName = fileNameFromContentDisposition(contentDisposition) {
            return sanitize(headerName)
        }

        let lastPathComponent = url.lastPathComponent.removingPercentEncoding ?? url.lastPathComponent
        if !lastPathComponent.isEmpty, lastPathComponent != "/" {
            return sanitize(lastPathComponent)
        }

        if let ext = extensionForMimeType(mimeType) {
            return "download.\(ext)"
        }

        return "download.bin"
    }

    public static func sanitize(_ fileName: String) -> String {
        let forbidden = CharacterSet(charactersIn: "/\\:?%*|\"<>")
        let parts = fileName.components(separatedBy: forbidden)
        let joined = parts.joined(separator: "-").trimmingCharacters(in: .whitespacesAndNewlines)
        return joined.isEmpty ? "download.bin" : joined
    }

    private static func fileNameFromContentDisposition(_ value: String?) -> String? {
        guard let value else { return nil }
        let components = value.split(separator: ";").map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }

        for component in components {
            let lower = component.lowercased()
            if lower.hasPrefix("filename*="),
               let range = component.range(of: "''") {
                return String(component[range.upperBound...]).removingPercentEncoding
            }

            if lower.hasPrefix("filename=") {
                var name = String(component.dropFirst("filename=".count))
                if name.hasPrefix("\""), name.hasSuffix("\"") {
                    name.removeFirst()
                    name.removeLast()
                }
                return name
            }
        }

        return nil
    }

    private static func extensionForMimeType(_ value: String?) -> String? {
        guard let value else { return nil }
        let normalized = value.split(separator: ";").first?.lowercased()

        switch normalized {
        case "application/pdf":
            return "pdf"
        case "application/zip":
            return "zip"
        case "application/x-apple-diskimage":
            return "dmg"
        case "text/plain":
            return "txt"
        case "image/jpeg":
            return "jpg"
        case "image/png":
            return "png"
        default:
            return nil
        }
    }
}
