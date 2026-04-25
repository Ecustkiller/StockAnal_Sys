#!/usr/bin/env python3
"""正确分页格式抓取全部17页"""
import requests, re, json, time
from bs4 import BeautifulSoup

COOKIE = "JSESSIONID=OTZjNmM3ZWMtNTk5OC00ZjZhLThhZjctZmM3NGNjZGI1ZDYx; tgbuser=13880750; loginStatus=qrcode; agree=enter; _c_WBKFRo=D9rJR6ecn7RagPYqLESGs4RWJfEhqy0nvbPMuNG8; creatorStatus13880750=true; showStatus13880750=true; Actionshow2=true"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Cookie": COOKIE, "Referer": "https://www.tgb.cn/"
}

all_posts = []
seen_floors = set()

for page in range(1, 18):
    # 关键：分页格式是 /a/2jEzkjWRJit-{页码}
    if page == 1:
        url = "https://www.tgb.cn/a/2jEzkjWRJit"
    else:
        url = f"https://www.tgb.cn/a/2jEzkjWRJit-{page}"
    
    print(f"第{page}/17页: {url}")
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.encoding = "utf-8"
    
    if "登录可查看全文" in r.text:
        print(f"  ⚠️ 需登录")
        continue
    
    soup = BeautifulSoup(r.text, "html.parser")
    page_count = 0
    
    for div in soup.find_all(attrs={"id": re.compile(r"reply", re.I)}):
        text = div.get_text(strip=True)
        if "老弟来当韭菜" not in text:
            continue
        if "楼主" not in text:
            continue
            
        date_match = re.search(r"(202[56]-\d{2}-\d{2})", text)
        floor_match = re.search(r"第(\d+)楼", text)
        floor_num = int(floor_match.group(1)) if floor_match else 0
        
        if floor_num in seen_floors:
            continue
        seen_floors.add(floor_num)
        
        # 提取正文
        body = re.sub(r"老弟来当韭菜楼主.*?只看TA", "", text)
        body = re.sub(r"第\d+楼.*?· 淘股吧.*", "", body)
        body = re.sub(r"· 淘股吧打赏点赞\(\d+\)Ta回复.*", "", body)
        body = body.strip()
        
        # 提取图片（实际交易截图）
        imgs = []
        for img in div.find_all("img"):
            src = img.get("data-original","") or img.get("src","")
            if "image.tgb.cn" in src and "user_icon" not in src:
                # 去掉缩略图后缀
                src = re.sub(r"_\d+wh\.png$", "", src)
                if src not in imgs:
                    imgs.append(src)
        
        if body and len(body) > 3:
            all_posts.append({
                "date": date_match.group(1) if date_match else "未知",
                "floor": floor_num,
                "text": body[:1000],
                "imgs": imgs[:5],
                "page": page
            })
            page_count += 1
    
    print(f"  ✅ 新增{page_count}条 (累计{len(all_posts)}条, 最大楼层#{max(seen_floors) if seen_floors else 0})")
    time.sleep(2)

# 排序
all_posts.sort(key=lambda x: (x["date"], x["floor"]))

# 保存
with open("/Users/ecustkiller/WorkBuddy/Claw/tgb_all_posts.json", "w", encoding="utf-8") as f:
    json.dump(all_posts, f, ensure_ascii=False, indent=2)

print(f"\n{'='*80}")
print(f"✅ 抓取完成！共{len(all_posts)}条楼主追帖")
print(f"楼层: #{min(p['floor'] for p in all_posts)} ~ #{max(p['floor'] for p in all_posts)}")
print(f"时间: {all_posts[0]['date']} ~ {all_posts[-1]['date']}")

# 打印全部
for p in all_posts:
    txt = p["text"][:180].replace("\n"," ")
    img_flag = f" 📷{len(p['imgs'])}" if p["imgs"] else ""
    print(f"[{p['date']}] #{p['floor']:>3d}  {txt}{img_flag}")
