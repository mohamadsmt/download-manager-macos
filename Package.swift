// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "DownloadManager",
    defaultLocalization: "en",
    platforms: [
        .macOS(.v14)
    ],
    products: [
        .library(name: "DownloadManagerCore", targets: ["DownloadManagerCore"]),
        .executable(name: "DownloadManagerApp", targets: ["DownloadManagerApp"]),
        .executable(name: "DownloadManagerNativeHost", targets: ["DownloadManagerNativeHost"]),
        .executable(name: "DownloadManagerCoreSmokeTests", targets: ["DownloadManagerCoreSmokeTests"])
    ],
    targets: [
        .target(
            name: "DownloadManagerCore"
        ),
        .executableTarget(
            name: "DownloadManagerApp",
            dependencies: ["DownloadManagerCore"],
            resources: [
                .process("Resources")
            ]
        ),
        .executableTarget(
            name: "DownloadManagerNativeHost",
            dependencies: ["DownloadManagerCore"]
        ),
        .executableTarget(
            name: "DownloadManagerCoreSmokeTests",
            dependencies: ["DownloadManagerCore"]
        )
    ]
)
