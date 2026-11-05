import Vision
import ImageIO
import Foundation

// vision-ocr — macOS Vision text recognition for the screen monitor.
// ~0.3-0.8s on a retina screenshot vs ~6s for tesseract.js.
//
// Usage: vision-ocr <image-path>
// stdout: {"text":"line1\nline2","confidence":0.93}
// exit 1 + stderr on failure — caller falls back to tesseract.

func fail(_ msg: String) -> Never {
    FileHandle.standardError.write((msg + "\n").data(using: .utf8)!)
    exit(1)
}

guard CommandLine.arguments.count > 1 else {
    fail("usage: vision-ocr <image-path>")
}

let url = URL(fileURLWithPath: CommandLine.arguments[1])
guard let src = CGImageSourceCreateWithURL(url as CFURL, nil),
      let cgImage = CGImageSourceCreateImageAtIndex(src, 0, nil) else {
    fail("cannot read image: \(url.path)")
}

let request = VNRecognizeTextRequest()
request.recognitionLevel = .accurate
request.usesLanguageCorrection = true
request.recognitionLanguages = ["en-US"]

let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])
do {
    try handler.perform([request])
} catch {
    fail("vision request failed: \(error.localizedDescription)")
}

var lines: [String] = []
var confSum: Float = 0
var confN = 0
for obs in request.results ?? [] {
    guard let cand = obs.topCandidates(1).first else { continue }
    let t = cand.string.trimmingCharacters(in: .whitespaces)
    if !t.isEmpty {
        lines.append(t)
        confSum += cand.confidence
        confN += 1
    }
}

let payload: [String: Any] = [
    "text": lines.joined(separator: "\n"),
    "confidence": confN > 0 ? Double(confSum) / Double(confN) : 0,
]
guard let data = try? JSONSerialization.data(withJSONObject: payload),
      let json = String(data: data, encoding: .utf8) else {
    fail("json encode failed")
}
print(json)
