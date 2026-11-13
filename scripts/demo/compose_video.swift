// Compose the demo walkthrough video from captured frames (CEO demo).
//
//   swiftc -O scripts/demo/compose_video.swift -o /tmp/compose && /tmp/compose FRAMES_DIR OUT.mp4
//
// FRAMES_DIR holds manifest.json from record_walkthrough.mjs: frames with their time in seconds,
// and the Hebrew caption shown with them; title cards are drawn here. Each frame is fitted into a
// 1920x1080 picture above a caption bar in the features deck's style, and written as H.264 with
// macOS's own AVFoundation: nothing to install. The output may show real names: keep it in the
// git-ignored demo-data folder.
import AVFoundation
import AppKit
import CoreText
import Foundation

struct Entry: Decodable {
    let file: String?
    let t: Double
    let eyebrow: String?
    let caption: String?
    let title: [String]?
    let sub: String?
}

struct Manifest: Decodable {
    let frames: [Entry]
    let end: Double
}

let width = 1920
let height = 1080
let barHeight: CGFloat = 150

func rgb(_ hex: UInt32, _ alpha: CGFloat = 1) -> CGColor {
    CGColor(
        red: CGFloat((hex >> 16) & 0xFF) / 255, green: CGFloat((hex >> 8) & 0xFF) / 255,
        blue: CGFloat(hex & 0xFF) / 255, alpha: alpha)
}

let canvas = rgb(0xDDE1DD)
let ink = rgb(0x141813)
let inkText = rgb(0xF5F3ED)
let accent = rgb(0xD7A57E)
let border = rgb(0x899585)

let args = CommandLine.arguments
guard args.count == 3 else {
    FileHandle.standardError.write("usage: compose FRAMES_DIR OUT.mp4\n".data(using: .utf8)!)
    exit(2)
}
let dir = URL(fileURLWithPath: args[1])
let outURL = URL(fileURLWithPath: args[2])
let manifest = try JSONDecoder().decode(
    Manifest.self, from: Data(contentsOf: dir.appendingPathComponent("manifest.json")))

// IBM Plex Sans Hebrew, the app's own font, from the client's dependencies when present.
let fontDir = URL(fileURLWithPath: CommandLine.arguments[0]).deletingLastPathComponent()
for weight in ["400Regular", "500Medium", "600SemiBold"] {
    let candidates = [
        ProcessInfo.processInfo.environment["HDS_FONT_DIR"].map { "\($0)/\(weight)/IBMPlexSansHebrew_\(weight).ttf" },
    ].compactMap { $0 }
    for path in candidates where FileManager.default.fileExists(atPath: path) {
        CTFontManagerRegisterFontsForURL(URL(fileURLWithPath: path) as CFURL, .process, nil)
    }
}
_ = fontDir

func font(_ size: CGFloat, _ weight: String) -> CTFont {
    let name = "IBMPlexSansHebrew-\(weight)"
    let f = CTFontCreateWithName(name as CFString, size, nil)
    if (CTFontCopyPostScriptName(f) as String) == name { return f }
    return CTFontCreateUIFontForLanguage(.system, size, "he" as CFString) ?? f
}

/// Draw right-to-left Hebrew text, right-aligned, inside ``rect``.
func drawText(_ ctx: CGContext, _ text: String, _ f: CTFont, _ color: CGColor, _ rect: CGRect,
              align: CTTextAlignment = .right) {
    let style = NSMutableParagraphStyle()
    style.alignment = align == .left ? .left : .right
    style.baseWritingDirection = .rightToLeft
    let attrs: [NSAttributedString.Key: Any] = [
        .font: f as NSFont,
        .foregroundColor: NSColor(cgColor: color) ?? NSColor.white,
        .paragraphStyle: style,
    ]
    let s = NSAttributedString(string: text, attributes: attrs)
    let setter = CTFramesetterCreateWithAttributedString(s)
    let path = CGPath(rect: rect, transform: nil)
    let frame = CTFramesetterCreateFrame(setter, CFRange(location: 0, length: s.length), path, nil)
    CTFrameDraw(frame, ctx)
}

/// The deck's panel: a dark rectangle with its top-left corner cut.
func panel(_ ctx: CGContext, _ rect: CGRect, cut: CGFloat) {
    ctx.setFillColor(ink)
    ctx.beginPath()
    ctx.move(to: CGPoint(x: rect.minX, y: rect.minY))
    ctx.addLine(to: CGPoint(x: rect.maxX, y: rect.minY))
    ctx.addLine(to: CGPoint(x: rect.maxX, y: rect.maxY))
    ctx.addLine(to: CGPoint(x: rect.minX + cut, y: rect.maxY))
    ctx.addLine(to: CGPoint(x: rect.minX, y: rect.maxY - cut))
    ctx.closePath()
    ctx.fillPath()
}

var imageCache: (String, CGImage)?
func image(_ file: String) -> CGImage? {
    if let cached = imageCache, cached.0 == file { return cached.1 }
    guard let src = CGImageSourceCreateWithURL(dir.appendingPathComponent(file) as CFURL, nil),
          let img = CGImageSourceCreateImageAtIndex(src, 0, nil) else { return nil }
    imageCache = (file, img)
    return img
}

func render(_ e: Entry, into buffer: CVPixelBuffer) {
    CVPixelBufferLockBaseAddress(buffer, [])
    defer { CVPixelBufferUnlockBaseAddress(buffer, []) }
    let ctx = CGContext(
        data: CVPixelBufferGetBaseAddress(buffer), width: width, height: height, bitsPerComponent: 8,
        bytesPerRow: CVPixelBufferGetBytesPerRow(buffer), space: CGColorSpaceCreateDeviceRGB(),
        bitmapInfo: CGImageAlphaInfo.premultipliedFirst.rawValue | CGBitmapInfo.byteOrder32Little.rawValue)!
    let W = CGFloat(width), H = CGFloat(height)
    if let title = e.title {
        // A title card, like the deck's first slide.
        ctx.setFillColor(ink)
        ctx.fill(CGRect(x: 0, y: 0, width: W, height: H))
        drawText(ctx, e.eyebrow ?? "HYBRID DEFENSE SYSTEMS", font(26, "Medium"), accent,
                 CGRect(x: 160, y: H / 2 + 150, width: W - 320, height: 50))
        var y = H / 2 + 40
        for line in title {
            drawText(ctx, line, font(92, "Medium"), inkText, CGRect(x: 160, y: y, width: W - 320, height: 120))
            y -= 112
        }
        ctx.setFillColor(rgb(0x3A403C))
        ctx.fill(CGRect(x: 160, y: y + 70, width: W - 320, height: 2))
        if let sub = e.sub {
            drawText(ctx, sub, font(34, "Regular"), rgb(0xC9CCC4), CGRect(x: 160, y: y - 10, width: W - 320, height: 70))
        }
        return
    }
    ctx.setFillColor(canvas)
    ctx.fill(CGRect(x: 0, y: 0, width: W, height: H))
    if let file = e.file, let img = image(file) {
        let area = CGRect(x: 0, y: barHeight, width: W, height: H - barHeight).insetBy(dx: 24, dy: 20)
        let iw = CGFloat(img.width), ih = CGFloat(img.height)
        let scale = min(area.width / iw, area.height / ih)
        let size = CGSize(width: iw * scale, height: ih * scale)
        let rect = CGRect(x: area.midX - size.width / 2, y: area.midY - size.height / 2,
                          width: size.width, height: size.height)
        ctx.interpolationQuality = .high
        ctx.draw(img, in: rect)
        ctx.setStrokeColor(border)
        ctx.setLineWidth(1.5)
        ctx.stroke(rect.insetBy(dx: -1, dy: -1))
    }
    if let caption = e.caption, !caption.isEmpty {
        panel(ctx, CGRect(x: 0, y: 0, width: W, height: barHeight), cut: 22)
        drawText(ctx, e.eyebrow ?? "", font(24, "Medium"), accent, CGRect(x: 80, y: barHeight - 62, width: W - 160, height: 40))
        drawText(ctx, caption, font(36, "Regular"), inkText, CGRect(x: 80, y: 18, width: W - 160, height: 80))
    }
}

try? FileManager.default.removeItem(at: outURL)
let writer = try AVAssetWriter(outputURL: outURL, fileType: .mp4)
let input = AVAssetWriterInput(mediaType: .video, outputSettings: [
    AVVideoCodecKey: AVVideoCodecType.h264,
    AVVideoWidthKey: width,
    AVVideoHeightKey: height,
    AVVideoCompressionPropertiesKey: [AVVideoAverageBitRateKey: 8_000_000],
])
input.expectsMediaDataInRealTime = false
let adaptor = AVAssetWriterInputPixelBufferAdaptor(assetWriterInput: input, sourcePixelBufferAttributes: [
    kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_32BGRA,
    kCVPixelBufferWidthKey as String: width,
    kCVPixelBufferHeightKey as String: height,
])
writer.add(input)
writer.startWriting()
writer.startSession(atSourceTime: .zero)

var last = -1.0
var written = 0
for e in manifest.frames + [Entry(file: manifest.frames.last?.file, t: manifest.end,
                                  eyebrow: manifest.frames.last?.eyebrow,
                                  caption: manifest.frames.last?.caption,
                                  title: manifest.frames.last?.title, sub: manifest.frames.last?.sub)] {
    let t = max(e.t, last + 0.001)
    last = t
    while !input.isReadyForMoreMediaData { Thread.sleep(forTimeInterval: 0.005) }
    var buffer: CVPixelBuffer?
    CVPixelBufferPoolCreatePixelBuffer(nil, adaptor.pixelBufferPool!, &buffer)
    guard let pb = buffer else { continue }
    render(e, into: pb)
    adaptor.append(pb, withPresentationTime: CMTime(seconds: t, preferredTimescale: 600))
    written += 1
}
input.markAsFinished()
let done = DispatchSemaphore(value: 0)
writer.finishWriting { done.signal() }
done.wait()
if writer.status != .completed {
    FileHandle.standardError.write("compose: \(writer.error?.localizedDescription ?? "failed")\n".data(using: .utf8)!)
    exit(1)
}
print("compose: \(written) frames, \(String(format: "%.1f", manifest.end)) s → \(outURL.path)")
