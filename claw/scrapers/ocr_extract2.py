#!/usr/bin/env python3
"""
OCR批量提取退学炒股图片中的文字
使用macOS Vision框架 (通过pyobjc)
"""
import os
import sys
import Quartz
import Vision
from Foundation import NSURL

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

def ocr_image(image_path):
    """使用macOS Vision框架进行OCR"""
    # Load image
    url = NSURL.fileURLWithPath_(image_path)
    source = Quartz.CGImageSourceCreateWithURL(url, None)
    if source is None:
        return "[ERROR: Cannot load image]"
    
    cg_image = Quartz.CGImageSourceCreateImageAtIndex(source, 0, None)
    if cg_image is None:
        return "[ERROR: Cannot create CGImage]"
    
    # Create OCR request
    request = Vision.VNRecognizeTextRequest.alloc().init()
    request.setRecognitionLanguages_(["zh-Hans", "zh-Hant", "en"])
    request.setRecognitionLevel_(Vision.VNRequestTextRecognitionLevelAccurate)
    request.setUsesLanguageCorrection_(True)
    
    # Process
    handler = Vision.VNImageRequestHandler.alloc().initWithCGImage_options_(cg_image, None)
    success = handler.performRequests_error_([request], None)
    
    if not success[0]:
        return f"[ERROR: OCR failed - {success[1]}]"
    
    results = request.results()
    if not results:
        return ""
    
    lines = []
    for obs in results:
        candidates = obs.topCandidates_(1)
        if candidates:
            lines.append(candidates[0].string())
    
    return "\n".join(lines)

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
    
    print(f"Total: {len(all_files)}, Skip: {len(all_files)-len(todo_files)}, Todo: {len(todo_files)}")
    
    # Group by prefix (module)
    modules = {}
    for f in todo_files:
        prefix = f.rsplit("_p", 1)[0] if "_p" in f else f.rsplit(".", 1)[0]
        if prefix not in modules:
            modules[prefix] = []
        modules[prefix].append(f)
    
    print(f"Modules: {list(modules.keys())}")
    
    for module_name, files in sorted(modules.items()):
        output_file = os.path.join(OUTPUT_DIR, f"{module_name}.txt")
        
        # Check if already extracted with valid content
        if os.path.exists(output_file):
            with open(output_file, "r") as f:
                content = f.read()
            if "OCR ERROR" not in content and len(content) > len(files) * 100:
                print(f"  [SKIP] {module_name} - already done")
                continue
        
        print(f"  {module_name} ({len(files)} pages)...", flush=True)
        all_text = []
        for i, fname in enumerate(sorted(files)):
            image_path = os.path.join(IMAGE_DIR, fname)
            text = ocr_image(image_path)
            page_num = i + 1
            all_text.append(f"=== 第{page_num}页 ({fname}) ===\n{text}\n")
            char_count = len(text)
            print(f"    p{page_num}/{len(files)}: {char_count}字", flush=True)
        
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(f"# {module_name}\n\n")
            f.write("\n".join(all_text))
        
        total = sum(len(t) for t in all_text)
        print(f"  => {total} chars")
    
    print("\n=== Summary ===")
    for txt_file in sorted(os.listdir(OUTPUT_DIR)):
        if txt_file.endswith(".txt"):
            path = os.path.join(OUTPUT_DIR, txt_file)
            size = os.path.getsize(path)
            with open(path, "r") as f:
                content = f.read()
            # Count actual text (excluding headers)
            lines = [l for l in content.split("\n") if l.strip() and not l.startswith("===") and not l.startswith("#")]
            print(f"  {txt_file}: {size} bytes, {len(lines)} text lines")

if __name__ == "__main__":
    main()
