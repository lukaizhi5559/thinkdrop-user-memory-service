// td-screen-helper.swift — one-shot ScreenCaptureKit PNG capture → stdout/--out.
//
// Why a binary and not `screencapture` CLI: that tool inherits TCC
// responsibility from whatever launched the service (terminal/Electron dev
// binary, Warp, etc.) — there is no grantable "ThinkDrop" process to toggle.
// This helper is its own TCC client ("ThinkDrop Screen Helper" in System
// Settings → Privacy & Security → Screen Recording): one grant, stable
// forever regardless of launcher. Same pattern as td-audio-helper (audio).
//
// Build:  swiftc -O td-screen-helper.swift -o bin/td-screen-helper
//         wrap bin/td-screen-helper.app (Info.plist + Contents/MacOS) then
//         codesign -s - --force --deep bin/td-screen-helper.app
//
// Usage:  td-screen-helper [--out PATH] [--display INDEX]
//         stdout: PNG bytes (when --out omitted)
//         stderr: {"event":"starting"|"requesting-access"|"granted"|
//                  "captured"|"denied"|"error","msg":...}
//         exit 0 ok / 2 denied / 1 error

import Foundation
import ScreenCaptureKit
import CoreGraphics
import ImageIO
import UniformTypeIdentifiers

func emit(_ event: String, _ msg: String = "") {
    var dict: [String: Any] = ["event": event]
    if !msg.isEmpty { dict["msg"] = msg }
    if let d = try? JSONSerialization.data(withJSONObject: dict),
       let s = String(data: d, encoding: .utf8) {
        FileHandle.standardError.write((s + "\n").data(using: .utf8)!)
    }
}

func fail(_ event: String, _ msg: String, _ code: Int32 = 1) -> Never {
    emit(event, msg)
    exit(code)
}

// ── Args ──────────────────────────────────────────────────────────────────
var outPath: String? = nil
var displayIndex = 0
var i = 1
while i < CommandLine.arguments.count {
    switch CommandLine.arguments[i] {
    case "--out":
        i += 1
        if i < CommandLine.arguments.count { outPath = CommandLine.arguments[i] }
    case "--display":
        i += 1
        if i < CommandLine.arguments.count { displayIndex = Int(CommandLine.arguments[i]) ?? 0 }
    default:
        break
    }
    i += 1
}

emit("starting")

// Watchdog — macOS replayd wedged (crash-looping on a malformed TCC record)
// makes SCShareableContent/captureImage hang FOREVER (same failure the
// td-audio-helper ACTIVE watchdog guards). A single-frame capture is
// sub-second healthy; 20s means the subsystem is dead — exit 'stalled' so
// the caller surfaces "restart the Mac / toggle the grant" instead of the
// execFile timeout reading as a generic failure.
let deadline = DispatchTime.now() + .seconds(20)
DispatchQueue.global().asyncAfter(deadline: deadline) {
    fail("stalled", "ScreenCaptureKit unresponsive 20s — replayd may be wedged; toggle the Screen Recording grant or restart the Mac", 3)
}

// SCK does NOT prompt for Screen Recording on behalf of a bare binary — the
// .app wrapper + explicit request below are what register "ThinkDrop Screen
// Helper" as a TCC client. Preflight; trigger the system prompt once; if it
// stays denied emit a classified 'denied' so callers can name the helper in
// their user-action hint.
if !CGPreflightScreenCaptureAccess() {
    emit("requesting-access")
    _ = CGRequestScreenCaptureAccess()
}
if !CGPreflightScreenCaptureAccess() {
    fail("denied", "Screen Recording not granted to ThinkDrop Screen Helper — enable it in System Settings → Privacy & Security → Screen Recording", 2)
}
emit("granted")

if #available(macOS 14.0, *) {
    Task {
        do {
            let content = try await SCShareableContent.excludingDesktopWindows(false,
                                                                               onScreenWindowsOnly: false)
            guard displayIndex < content.displays.count else {
                fail("denied", "no display \(displayIndex) — grant Screen Recording to ThinkDrop Screen Helper in System Settings → Privacy & Security", 2)
            }
            let display = content.displays[displayIndex]
            let filter = SCContentFilter(display: display,
                                         excludingApplications: [],
                                         exceptingWindows: [])
            let config = SCStreamConfiguration()
            config.showsCursor = true
            let image = try await SCScreenshotManager.captureImage(contentFilter: filter,
                                                                   configuration: config)

            let data = NSMutableData()
            guard let dest = CGImageDestinationCreateWithData(data as CFMutableData,
                                                              UTType.png.identifier as CFString, 1, nil) else {
                fail("error", "png destination unavailable")
            }
            CGImageDestinationAddImage(dest, image, nil)
            guard CGImageDestinationFinalize(dest) else {
                fail("error", "png finalize failed")
            }

            if let out = outPath {
                do {
                    try data.write(toFile: out, options: .atomic)
                } catch {
                    fail("error", "write \(out): \(error.localizedDescription)")
                }
            } else {
                FileHandle.standardOutput.write(data as Data)
            }
            emit("captured", "\(image.width)x\(image.height) \(data.length) bytes")
            exit(0)
        } catch {
            let ns = error as NSError
            // TCC denial surfaces as a generic SCK failure — classify so the
            // caller can point at the right Settings toggle.
            if ns.localizedDescription.lowercased().contains("denied")
                || ns.localizedDescription.lowercased().contains("declin")
                || ns.domain.localizedCaseInsensitiveContains("ScreenCapture") {
                fail("denied", ns.localizedDescription, 2)
            }
            fail("error", ns.localizedDescription)
        }
    }
    dispatchMain()
} else {
    fail("error", "td-screen-helper requires macOS 14+ (SCScreenshotManager)")
}
