// Compose the demo walkthrough video from captured frames (CEO demo).
//
//   swiftc -O scripts/demo/compose_video.swift -o demo-data/compose
//   HDS_FONT_DIR=… demo-data/compose FRAMES_DIR OUT.mp4 [WIDTH HEIGHT]   (1920 1080 by default;
//                                                                     1080 1920 for phones)
//
// FRAMES_DIR holds manifest.json from record_walkthrough.mjs: frames with their time in seconds and
// the Hebrew caption shown with them; title cards are drawn here. The frames are resampled to a
// steady 30 fps, each fitted above a caption bar in the features deck's style, with a short
// crossfade whenever the scene or caption changes, and written as H.264 with macOS's own
// AVFoundation: nothing to install, no voice. The output may show real names: keep it in the
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

let args = CommandLine.arguments
guard args.count == 3 || args.count == 5 else {
    FileHandle.standardError.write("usage: compose FRAMES_DIR OUT.mp4 [WIDTH HEIGHT]\n".data(using: .utf8)!)
    exit(2)
}
let dir = URL(fileURLWithPath: args[1])
let outURL = URL(fileURLWithPath: args[2])
let width = args.count == 5 ? Int(args[3])! : 1920
let height = args.count == 5 ? Int(args[4])! : 1080
let W = CGFloat(width), H = CGFloat(height)
let portrait = height > width
let unit = min(W, H) / 1080  // type scales with the short side
let fps = 30.0
let fade = 0.28  // seconds of crossfade at each change
let barHeight: CGFloat = portrait ? 300 * unit : 150 * unit
let margin: CGFloat = (portrait ? 64 : 80) * unit

let manifest = try JSONDecoder().decode(
    Manifest.self, from: Data(contentsOf: dir.appendingPathComponent("manifest.json")))

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

// IBM Plex Sans Hebrew, the app's own font, from the client's dependencies (HDS_FONT_DIR).
if let fonts = ProcessInfo.processInfo.environment["HDS_FONT_DIR"] {
    for weight in ["400Regular", "500Medium", "600SemiBold"] {
        let path = "\(fonts)/\(weight)/IBMPlexSansHebrew_\(weight).ttf"
        if FileManager.default.fileExists(atPath: path) {
            CTFontManagerRegisterFontsForURL(URL(fileURLWithPath: path) as CFURL, .process, nil)
        }
    }
}

func font(_ size: CGFloat, _ weight: String) -> CTFont {
    let name = "IBMPlexSansHebrew-\(weight)"
    let f = CTFontCreateWithName(name as CFString, size * unit, nil)
    if (CTFontCopyPostScriptName(f) as String) == name { return f }
    return CTFontCreateUIFontForLanguage(.system, size * unit, "he" as CFString) ?? f
}

/// Right-to-left Hebrew text, right-aligned and wrapped inside ``rect``.
func drawText(_ ctx: CGContext, _ text: String, _ f: CTFont, _ color: CGColor, _ rect: CGRect) {
    let style = NSMutableParagraphStyle()
    style.alignment = .right
    style.baseWritingDirection = .rightToLeft
    style.lineSpacing = 4 * unit
    let s = NSAttributedString(string: text, attributes: [
        .font: f as NSFont, .foregroundColor: NSColor(cgColor: color) ?? .white, .paragraphStyle: style,
    ])
    let setter = CTFramesetterCreateWithAttributedString(s)
    let frame = CTFramesetterCreateFrame(setter, CFRange(location: 0, length: s.length), CGPath(rect: rect, transform: nil), nil)
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

var cache: [String: CGImage] = [:]
func image(_ file: String) -> CGImage? {
    if let hit = cache[file] { return hit }
    guard let src = CGImageSourceCreateWithURL(dir.appendingPathComponent(file) as CFURL, nil),
          let img = CGImageSourceCreateImageAtIndex(src, 0, nil) else { return nil }
    if cache.count > 8 { cache.removeAll() }
    cache[file] = img
    return img
}

func makeContext() -> CGContext {
    CGContext(data: nil, width: width, height: height, bitsPerComponent: 8, bytesPerRow: 0,
              space: CGColorSpaceCreateDeviceRGB(),
              bitmapInfo: CGImageAlphaInfo.premultipliedFirst.rawValue | CGBitmapInfo.byteOrder32Little.rawValue)!
}

/// One composed picture for ``e``.
func compose(_ e: Entry) -> CGImage {
    let ctx = makeContext()
    if let title = e.title {
        ctx.setFillColor(ink)
        ctx.fill(CGRect(x: 0, y: 0, width: W, height: H))
        let big: CGFloat = portrait ? 104 : 92
        let x = (portrait ? 90 : 160) * unit
        let lines = CGFloat(title.count)
        var y = H / 2 + (lines - 1) * big * 0.62 * unit
        drawText(ctx, e.eyebrow ?? "HYBRID DEFENSE SYSTEMS", font(26, "Medium"), accent,
                 CGRect(x: x, y: y + big * 1.25 * unit, width: W - 2 * x, height: 50 * unit))
        for line in title {
            drawText(ctx, line, font(big, "Medium"), inkText, CGRect(x: x, y: y, width: W - 2 * x, height: big * 1.3 * unit))
            y -= big * 1.22 * unit
        }
        ctx.setFillColor(rgb(0x3A403C))
        ctx.fill(CGRect(x: x, y: y + big * 0.55 * unit, width: W - 2 * x, height: 2))
        if let sub = e.sub {
            drawText(ctx, sub, font(portrait ? 36 : 34, "Regular"), rgb(0xC9CCC4),
                     CGRect(x: x, y: y - 50 * unit, width: W - 2 * x, height: 120 * unit))
        }
        return ctx.makeImage()!
    }
    ctx.setFillColor(canvas)
    ctx.fill(CGRect(x: 0, y: 0, width: W, height: H))
    if let file = e.file, let img = image(file) {
        let area = CGRect(x: 0, y: barHeight, width: W, height: H - barHeight)
            .insetBy(dx: (portrait ? 40 : 24) * unit, dy: (portrait ? 36 : 20) * unit)
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
        panel(ctx, CGRect(x: 0, y: 0, width: W, height: barHeight), cut: 22 * unit)
        let eyebrowSize: CGFloat = portrait ? 30 : 24
        let captionSize: CGFloat = portrait ? 40 : 36
        drawText(ctx, e.eyebrow ?? "", font(eyebrowSize, "Medium"), accent,
                 CGRect(x: margin, y: barHeight - (portrait ? 92 : 62) * unit, width: W - 2 * margin, height: 50 * unit))
        drawText(ctx, caption, font(captionSize, "Regular"), inkText,
                 CGRect(x: margin, y: (portrait ? 26 : 18) * unit, width: W - 2 * margin,
                        height: barHeight - (portrait ? 116 : 70) * unit))
    }
    return ctx.makeImage()!
}

/// The entry showing at ``t``: the last one at or before it.
let entries = manifest.frames
func entry(at t: Double, from start: inout Int) -> Entry {
    while start + 1 < entries.count && entries[start + 1].t <= t { start += 1 }
    return entries[start]
}

func key(_ e: Entry) -> String { (e.title?.joined() ?? "") + "|" + (e.eyebrow ?? "") + "|" + (e.caption ?? "") }

try? FileManager.default.removeItem(at: outURL)
let writer = try AVAssetWriter(outputURL: outURL, fileType: .mp4)
let input = AVAssetWriterInput(mediaType: .video, outputSettings: [
    AVVideoCodecKey: AVVideoCodecType.h264, AVVideoWidthKey: width, AVVideoHeightKey: height,
    AVVideoCompressionPropertiesKey: [AVVideoAverageBitRateKey: portrait ? 7_000_000 : 9_000_000,
                                      AVVideoExpectedSourceFrameRateKey: 30],
])
input.expectsMediaDataInRealTime = false
let adaptor = AVAssetWriterInputPixelBufferAdaptor(assetWriterInput: input, sourcePixelBufferAttributes: [
    kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_32BGRA,
    kCVPixelBufferWidthKey as String: width, kCVPixelBufferHeightKey as String: height,
])
writer.add(input)
writer.startWriting()
writer.startSession(atSourceTime: .zero)

let total = Int((manifest.end * fps).rounded(.up))
var cursor = 0
var currentKey = ""
var previous: CGImage? = nil  // the last picture of the scene before, for the crossfade
var lastPicture: CGImage? = nil
var changedAt = -1000
var lastEntryKey = ""
for i in 0..<total {
    let t = Double(i) / fps
    let e = entry(at: t, from: &cursor)
    let k = key(e)
    if k != currentKey {
        previous = lastPicture
        changedAt = i
        currentKey = k
    }
    let picture: CGImage
    let thisKey = (e.file ?? "") + k
    if thisKey == lastEntryKey, let same = lastPicture {
        picture = same  // the same frame and caption as last time: no need to draw it again
    } else {
        picture = compose(e)
        lastEntryKey = thisKey
    }
    lastPicture = picture
    while !input.isReadyForMoreMediaData { Thread.sleep(forTimeInterval: 0.002) }
    var buffer: CVPixelBuffer?
    CVPixelBufferPoolCreatePixelBuffer(nil, adaptor.pixelBufferPool!, &buffer)
    guard let pb = buffer else { continue }
    CVPixelBufferLockBaseAddress(pb, [])
    let out = CGContext(data: CVPixelBufferGetBaseAddress(pb), width: width, height: height, bitsPerComponent: 8,
                        bytesPerRow: CVPixelBufferGetBytesPerRow(pb), space: CGColorSpaceCreateDeviceRGB(),
                        bitmapInfo: CGImageAlphaInfo.premultipliedFirst.rawValue | CGBitmapInfo.byteOrder32Little.rawValue)!
    out.draw(picture, in: CGRect(x: 0, y: 0, width: W, height: H))
    let into = Double(i - changedAt) / fps
    if let before = previous, into < fade {
        out.setAlpha(CGFloat(1 - into / fade))
        out.draw(before, in: CGRect(x: 0, y: 0, width: W, height: H))
    }
    CVPixelBufferUnlockBaseAddress(pb, [])
    adaptor.append(pb, withPresentationTime: CMTime(value: CMTimeValue(i), timescale: 30))
}
input.markAsFinished()
let done = DispatchSemaphore(value: 0)
writer.finishWriting { done.signal() }
done.wait()
if writer.status != .completed {
    FileHandle.standardError.write("compose: \(writer.error?.localizedDescription ?? "failed")\n".data(using: .utf8)!)
    exit(1)
}
print("compose: \(total) frames at 30 fps, \(String(format: "%.1f", manifest.end)) s, \(width)x\(height) → \(outURL.path)")
