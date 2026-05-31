import Foundation

public actor SpeedLimiter {
    private var limitBytesPerSecond: Int64?
    private var windowStart: Date
    private var bytesInWindow: Int64

    public init(limitBytesPerSecond: Int64?) {
        self.limitBytesPerSecond = limitBytesPerSecond
        self.windowStart = Date()
        self.bytesInWindow = 0
    }

    public func update(limitBytesPerSecond: Int64?) {
        self.limitBytesPerSecond = limitBytesPerSecond
        self.windowStart = Date()
        self.bytesInWindow = 0
    }

    public func throttle(bytes: Int) async {
        guard let limitBytesPerSecond, limitBytesPerSecond > 0 else { return }

        bytesInWindow += Int64(bytes)
        let elapsed = Date().timeIntervalSince(windowStart)

        if elapsed >= 1 {
            windowStart = Date()
            bytesInWindow = 0
            return
        }

        if bytesInWindow > limitBytesPerSecond {
            let remaining = max(0, 1 - elapsed)
            try? await Task.sleep(nanoseconds: UInt64(remaining * 1_000_000_000))
            windowStart = Date()
            bytesInWindow = 0
        }
    }
}
