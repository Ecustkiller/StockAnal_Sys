#!/usr/bin/env python3
"""批量抓取淘股吧帖子所有页面，提取楼主追帖"""
import requests, time, re, json
from bs4 import BeautifulSoup

COOKIE = "JSESSIONID=OTZjNmM3ZWMtNTk5OC00ZjZhLThhZjctZmM3NGNjZGI1ZDYx; tgbuser=13880750; loginStatus=qrcode; agree=enter; _c_WBKFRo=D9rJR6ecn7RagPYqLESGs4RWJfEhqy0nvbPMuNG8; creatorStatus13880750=true; showStatus13880750=true; Actionshow2=true"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Cookie": COOKIE,
    "Referer": "https://www.tgb.cn/"
}

BASE_URL = "https://www.tgb.cn/a/2jEzkjWRJit"

all_posts = []

for page in range(1, 18):  # 17页
    url = f"{BASE_URL}?p={page}" if page > 1 else BASE_URL
    print(f"抓取第{page}/17页: {url}")
    
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.encoding = "utf-8"
        html = r.text
        
        if "登录可查看全文" in html:
            print(f"  ⚠️ 第{page}页需要登录，跳过")
            continue
        
        soup = BeautifulSoup(html, "html.parser")
        
        # 提取楼主追帖
        page_posts = []
        for div in soup.find_all(attrs={"id": re.compile(r"reply", re.I)}):
            text = div.get_text(strip=True)
            if "老弟来当韭菜" in text and "楼主" in text:
                date_match = re.search(r"(202[56]-\d{2}-\d{2})", text)
                floor_match = re.search(r"第(\d+)楼", text)
                
                # 提取正文
                body = re.sub(r"老弟来当韭菜楼主.*?只看TA", "", text)
                body = re.sub(r"第\d+楼.*", "", body)
                body = body.strip()
                
                # 提取图片
                imgs = [img.get("src","") or img.get("data-original","") for img in div.find_all("img")]
                imgs = [u for u in imgs if "image.tgb" in u and "user_icon" not in u and "_60wh" not in u]
                # 也取_60wh的（可能是缩略图）
                if not imgs:
                    imgs = [u for u in [img.get("src","") or img.get("data-original","") for img in div.find_all("img")] if "image.tgb" in u and "user_icon" not in u]
                
                if body and len(body) > 5:
                    floor_num = int(floor_match.group(1)) if floor_match else 0
                    page_posts.append({
                        "date": date_match.group(1) if date_match else "未知",
                        "floor": floor_num,
                        "text": body[:800],
                        "imgs": imgs[:5],
                        "page": page
                    })
        
        # 去重
        seen_floors = set()
        for p in page_posts:
            if p["floor"] not in seen_floors:
                seen_floors.add(p["floor"])
                all_posts.append(p)
        
        print(f"  ✅ 第{page}页: {len(page_posts)}条楼主帖 (累计{len(all_posts)}条)")
        
    except Exception as e:
        print(f"  ❌ 第{page}页异常: {e}")
    
    time.sleep(2)  # 礼貌间隔

# 排序
all_posts.sort(key=lambda x: (x["date"], x["floor"]))

# 保存
with open("/Users/ecustkiller/WorkBuddy/Claw/tgb_all_posts.json", "w", encoding="utf-8") as f:
    json.dump(all_posts, f, ensure_ascii=False, indent=2)

print(f"\n{'='*80}")
print(f"抓取完成！共{len(all_posts)}条楼主追帖")
print(f"时间范围: {all_posts[0]['date'] if all_posts else 'N/A'} ~ {all_posts[-1]['date'] if all_posts else 'N/A'}")
print(f"已保存到 tgb_all_posts.json")

# 输出所有帖子摘要
print(f"\n{'='*80}")
print("全部追帖摘要")
print(f"{'='*80}")
for p in all_posts:
    txt = p["text"][:200].replace("\n"," ")
    img_flag = f" 📷{len(p['imgs'])}张" if p["imgs"] else ""
    print(f"[{p['date']}] #{p['floor']:>3d} {txt}{img_flag}")
