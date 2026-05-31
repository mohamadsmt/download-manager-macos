# Safari Web Extension Wrapper

The shared WebExtension in `BrowserExtensions/shared` is Safari-compatible, but
Safari requires an Xcode Safari Web Extension target to wrap and sign it.

Once full Xcode is installed, create a macOS Safari Web Extension target named
`DownloadManagerSafariExtension`, point its extension resources at the shared
manifest/background assets, and keep the containing app bundle identifier under
`com.mohamadsmt.DownloadManager`.

The app already handles `downloadmanager://add?url=...` URLs and the browser
inbox file used by the Chrome native messaging host.
