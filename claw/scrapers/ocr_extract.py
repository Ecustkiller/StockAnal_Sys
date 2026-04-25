#!/usr/bin/env python3
"""
OCR批量提取退学炒股图片中的文字
使用macOS自带的Vision框架进行中文OCR，无需安装额外依赖
"""
import subprocess
import os
import json
import sys

IMAGE_DIR = "/Users/ecustkiller/WorkBuddy/Claw/txcg_images"
OUTPUT_DIR = "/Users/ecustkiller/WorkBuddy/Claw/txcg_text"

# 已读完的模块（不需要重新提取）
DONE_PREFIXES = [
    "yulu57_",      # 语录57条(7页) ✅
    "xiaoming_",    # 小明解读(1页) ✅
    "bingdian_",    # 冰点(1页) ✅
    "shangying_",   # 上影线(1页) ✅
    "liaoliao1_",   # 闲聊1(2页) ✅
    "liaoliao2_",   # 闲聊2(1页) ✅
    "yizhouqi_",    # 一周期含义(8页) ✅
    "dachangtui_",  # 大长腿(4页) ✅
]

def ocr_with_vision(image_path):
    """使用macOS Vision框架进行OCR"""
    swift_code = '''
import Foundation
import Vision

let imagePath = CommandLine.arguments[1]
guard let image = NSImage(contentsOfFile: imagePath),
      let tiffData = image.tiffRepresentation,
      let bitmap = NSBitmapImageRep(data: tiffData),
      let cgImage = bitmap.cgImage(forProposedRect: nil, context: nil, hints: nil) else {
    print("ERROR: Cannot load image")
    exit(1)
}

let request = VNRecognizeTextRequest()
request.recognitionLanguages = ["zh-Hans", "zh-Hant", "en"]
request.recognitionLevel = .accurate
request.usesLanguageCorrection = true

let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])
try handler.perform([request])

guard let observations = request.results else {
    print("")
    exit(0)
}

for observation in observations {
    if let candidate = observation.topCandidates(1).first {
        print(candidate.string)
    }
}
'''
    # Write swift code to temp file
    swift_file = "/tmp/ocr_vision.swift"
    with open(swift_file, "w") as f:
        f.write(swift_code)
    
    try:
        result = subprocess.run(
            ["swift", swift_file, image_path],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            return f"[OCR ERROR: {result.stderr.strip()[:200]}]"
    except subprocess.TimeoutExpired:
        return "[OCR TIMEOUT]"
    except Exception as e:
        return f"[OCR EXCEPTION: {str(e)[:200]}]"

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Get all jpg files
    all_files = sorted([f for f in os.listdir(IMAGE_DIR) if f.endswith(".jpg")])
    
    # Filter out already-done files
    todo_files = []
    for f in all_files:
        skip = False
        for prefix in DONE_PREFIXES:
            if f.startswith(prefix):
                skip = True
                break
        if not skip:
            todo_files.append(f)
    
    print(f"Total images: {len(all_files)}, Already done: {len(all_files)-len(todo_files)}, To process: {len(todo_files)}")
    
    # Group by prefix (module)
    modules = {}
    for f in todo_files:
        prefix = f.rsplit("_p", 1)[0] if "_p" in f else f.rsplit(".", 1)[0]
        if prefix not in modules:
            modules[prefix] = []
        modules[prefix].append(f)
    
    print(f"Modules to process: {list(modules.keys())}")
    
    # Process each module
    for module_name, files in sorted(modules.items()):
        output_file = os.path.join(OUTPUT_DIR, f"{module_name}.txt")
        if os.path.exists(output_file):
            existing_size = os.path.getsize(output_file)
            if existing_size > 100:
                print(f"  [SKIP] {module_name} ({len(files)} pages) - already extracted ({existing_size} bytes)")
                continue
        
        print(f"  Processing {module_name} ({len(files)} pages)...")
        all_text = []
        for i, fname in enumerate(sorted(files)):
            image_path = os.path.join(IMAGE_DIR, fname)
            text = ocr_with_vision(image_path)
            page_num = i + 1
            all_text.append(f"=== 第{page_num}页 ({fname}) ===\n{text}\n")
            print(f"    Page {page_num}/{len(files)}: {len(text)} chars")
        
        # Save
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(f"# {module_name}\n\n")
            f.write("\n".join(all_text))
        
        total_chars = sum(len(t) for t in all_text)
        print(f"  Done: {module_name} -> {total_chars} chars total")
    
    print("\n=== All done! ===")
    # Print summary
    for txt_file in sorted(os.listdir(OUTPUT_DIR)):
        if txt_file.endswith(".txt"):
            size = os.path.getsize(os.path.join(OUTPUT_DIR, txt_file))
            print(f"  {txt_file}: {size} bytes")

if __name__ == "__main__":
    main()
