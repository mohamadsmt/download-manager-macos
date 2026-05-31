#!/usr/bin/env swift

import AppKit
import Foundation

let root = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
let iconset = root.appendingPathComponent("Assets/AppIcon.iconset", isDirectory: true)
let output = root.appendingPathComponent("Assets/AppIcon.icns")

try FileManager.default.createDirectory(at: iconset, withIntermediateDirectories: true)

let sizes: [(String, Int)] = [
    ("icon_16x16.png", 16),
    ("icon_16x16@2x.png", 32),
    ("icon_32x32.png", 32),
    ("icon_32x32@2x.png", 64),
    ("icon_128x128.png", 128),
    ("icon_128x128@2x.png", 256),
    ("icon_256x256.png", 256),
    ("icon_256x256@2x.png", 512),
    ("icon_512x512.png", 512),
    ("icon_512x512@2x.png", 1024)
]

func drawIcon(size: Int) -> NSImage {
    let image = NSImage(size: NSSize(width: size, height: size))
    image.lockFocus()
    defer { image.unlockFocus() }

    let scale = CGFloat(size) / 1024.0
    let canvas = CGRect(x: 0, y: 0, width: CGFloat(size), height: CGFloat(size))
    NSColor.clear.setFill()
    canvas.fill()

    func rect(_ x: CGFloat, _ y: CGFloat, _ width: CGFloat, _ height: CGFloat) -> CGRect {
        CGRect(x: x * scale, y: y * scale, width: width * scale, height: height * scale)
    }

    let backgroundPath = NSBezierPath(roundedRect: rect(64, 64, 896, 896), xRadius: 210 * scale, yRadius: 210 * scale)
    let backgroundGradient = NSGradient(colors: [
        NSColor(calibratedRed: 0.055, green: 0.647, blue: 0.914, alpha: 1),
        NSColor(calibratedRed: 0.145, green: 0.388, blue: 0.922, alpha: 1),
        NSColor(calibratedRed: 0.067, green: 0.094, blue: 0.153, alpha: 1)
    ])!
    backgroundGradient.draw(in: backgroundPath, angle: 315)

    NSGraphicsContext.current?.saveGraphicsState()
    NSShadow.drop(color: NSColor(calibratedWhite: 0, alpha: 0.28), blur: 26 * scale, y: -22 * scale).set()
    NSColor(calibratedRed: 0.850, green: 0.970, blue: 1.000, alpha: 1).setStroke()
    let tray = NSBezierPath()
    tray.lineWidth = 72 * scale
    tray.lineCapStyle = .round
    tray.move(to: NSPoint(x: 238 * scale, y: 292 * scale))
    tray.line(to: NSPoint(x: 786 * scale, y: 292 * scale))
    tray.stroke()
    NSGraphicsContext.current?.restoreGraphicsState()

    NSColor(calibratedRed: 0.490, green: 0.827, blue: 0.988, alpha: 0.64).setStroke()
    let segmentLine = NSBezierPath()
    segmentLine.lineWidth = 42 * scale
    segmentLine.lineCapStyle = .round
    segmentLine.move(to: NSPoint(x: 318 * scale, y: 422 * scale))
    segmentLine.line(to: NSPoint(x: 706 * scale, y: 422 * scale))
    segmentLine.stroke()

    NSGraphicsContext.current?.saveGraphicsState()
    NSShadow.drop(color: NSColor(calibratedWhite: 0, alpha: 0.34), blur: 24 * scale, y: -20 * scale).set()
    NSColor.white.setStroke()
    let shaft = NSBezierPath()
    shaft.lineWidth = 108 * scale
    shaft.lineCapStyle = .round
    shaft.move(to: NSPoint(x: 512 * scale, y: 788 * scale))
    shaft.line(to: NSPoint(x: 512 * scale, y: 406 * scale))
    shaft.stroke()

    let arrow = NSBezierPath()
    arrow.lineWidth = 108 * scale
    arrow.lineCapStyle = .round
    arrow.lineJoinStyle = .round
    arrow.move(to: NSPoint(x: 332 * scale, y: 526 * scale))
    arrow.line(to: NSPoint(x: 512 * scale, y: 320 * scale))
    arrow.line(to: NSPoint(x: 692 * scale, y: 526 * scale))
    arrow.stroke()
    NSGraphicsContext.current?.restoreGraphicsState()

    NSColor(calibratedRed: 0.133, green: 0.773, blue: 0.369, alpha: 1).setFill()
    NSBezierPath(ovalIn: rect(740, 720, 104, 104)).fill()

    NSColor(calibratedWhite: 1, alpha: 0.22).setStroke()
    let shine = NSBezierPath()
    shine.lineWidth = 22 * scale
    shine.lineCapStyle = .round
    shine.move(to: NSPoint(x: 190 * scale, y: 826 * scale))
    shine.curve(
        to: NSPoint(x: 456 * scale, y: 884 * scale),
        controlPoint1: NSPoint(x: 270 * scale, y: 886 * scale),
        controlPoint2: NSPoint(x: 360 * scale, y: 900 * scale)
    )
    shine.stroke()

    return image
}

extension NSShadow {
    static func drop(color: NSColor, blur: CGFloat, y: CGFloat) -> NSShadow {
        let shadow = NSShadow()
        shadow.shadowColor = color
        shadow.shadowBlurRadius = blur
        shadow.shadowOffset = NSSize(width: 0, height: y)
        return shadow
    }
}

for (name, size) in sizes {
    let image = drawIcon(size: size)
    guard let tiff = image.tiffRepresentation,
          let bitmap = NSBitmapImageRep(data: tiff),
          let png = bitmap.representation(using: .png, properties: [:]) else {
        fatalError("Could not render \(name)")
    }
    try png.write(to: iconset.appendingPathComponent(name), options: [.atomic])
}

let process = Process()
process.executableURL = URL(fileURLWithPath: "/usr/bin/iconutil")
process.arguments = ["-c", "icns", iconset.path, "-o", output.path]
try process.run()
process.waitUntilExit()

if process.terminationStatus != 0 {
    fatalError("iconutil failed with status \(process.terminationStatus)")
}

print(output.path)
