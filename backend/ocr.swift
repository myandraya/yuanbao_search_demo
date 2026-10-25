import Foundation
import Vision
import ImageIO
let url = URL(fileURLWithPath: CommandLine.arguments[1])
let request = VNRecognizeTextRequest()
request.recognitionLevel = .accurate
request.recognitionLanguages = ["zh-Hans", "en-US"]
request.usesLanguageCorrection = true
do {
    try VNImageRequestHandler(url: url).perform([request])
    print((request.results ?? []).compactMap { $0.topCandidates(1).first?.string }.joined(separator: "\n"))
} catch {
    fputs("OCR failed\n", stderr)
    exit(1)
}
