#!/usr/bin/env python3
"""
淘股吧博客自动抓取器 v2 - 使用Playwright
抓取山茶花Camellia(10926360)博客的所有帖子正文
"""
from playwright.sync_api import sync_playwright
import json, re, time

BLOG_URL = "https://www.tgb.cn/blog/10926360"
COOKIE_STR = "JSESSIONID=OTZjNmM3ZWMtNTk5OC00ZjZhLThhZjctZmM3NGNjZGI1ZDYx; tgbuser=13880750; loginStatus=qrcode; agree=enter; _c_WBKFRo=D9rJR6ecn7RagPYqLESGs4RWJfEhqy0nvbPMuNG8; creatorStatus13880750=true; showStatus13880750=true; Actionshow2=true"

# 从第一次运行中找到的帖子短链（可点击元素中提取到的）
BLOG_POST_URLS = [
    ("56天 落袋888", "2026-03-26", "a/2qtLAp19Lys"),
    ("55天 满仓出击必胜", "2026-03-25", "a/2qs6FSvuWU8"),
    ("54天 出击！", "2026-03-24", "a/2qqs3mcdPli"),
    ("52天 继续防守", "2026-03-23", "a/2qoN45X172i"),
    ("52天 无聊", "2026-03-21", "a/2qjO6cEjvnz"),  # 注意这个可能和上面日期不完全匹配
    ("51天 当了一天守门员", "2026-03-19", "a/2qi9CTxE4cr"),
    ("50天 启动失败", "2026-03-18", "a/2qgumt6CQuU"),
    ("49天 拐点出击", "2026-03-17", "a/2qePktS8a3R"),
    ("48天 无聊的一天", "2026-03-16", "a/2qdarOMCMtF"),
    ("47天 预期太满终吃面", "2026-03-13", "a/2q8c5u1qJMk"),
]

def parse_cookies(cookie_str):
    cookies = []
    for item in cookie_str.split("; "):
        if "=" in item:
            name, value = item.split("=", 1)
            cookies.append({"name": name.strip(), "value": value.strip(), "domain": ".tgb.cn", "path": "/"})
    return cookies

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
        )
        context.add_cookies(parse_cookies(COOKIE_STR))
        page = context.new_page()
        
        # ==================== 第1步：从博客获取完整帖子列表 ====================
        print("="*80)
        print("[1/2] 打开博客主页，获取全部帖子链接...")
        page.goto(BLOG_URL, wait_until="networkidle", timeout=30000)
        time.sleep(4)
        
        # 滚动加载更多
        for _ in range(10):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1)
        
        # 提取所有帖子链接
        all_links = page.evaluate("""() => {
            const results = [];
            document.querySelectorAll('a').forEach(a => {
                const href = a.getAttribute('href') || '';
                const text = a.innerText.trim();
                // 博客帖子链接格式: a/xxxxx（无前导斜杠）或 /a/xxxxx
                if ((href.startsWith('a/') || href.startsWith('/a/')) && /\\d{1,3}天/.test(text)) {
                    const fullHref = href.startsWith('/') ? href : '/' + href;
                    results.push({title: text.substring(0, 60), href: fullHref});
                }
            });
            return results;
        }""")
        
        print(f"  找到帖子链接: {len(all_links)}个")
        for al in all_links:
            print(f"    [{al['title'][:40]}] -> {al['href']}")
        
        # 补充：也获取可能漏掉的帖子（通过文本+日期匹配）
        page_text = page.inner_text("body")
        text_posts = re.findall(
            r'(\d{1,3}天\s*[\u4e00-\u9fff][^\n]{2,30})\s*(\d{2,5})/(\d{1,3})\s*(202[56]-\d{2}-\d{2})',
            page_text
        )
        print(f"\n  文本提取帖子标题: {len(text_posts)}条")
        
        # ==================== 第2步：逐个抓取帖子内容 ====================
        # 用Playwright提取到的链接
        post_urls = []
        for al in all_links:
            url = f"https://www.tgb.cn{al['href']}"
            post_urls.append((al['title'], url))
        
        # 如果Playwright没找到足够链接，用预设的
        if len(post_urls) < 10:
            print("\n  补充预设链接...")
            for title, date, path in BLOG_POST_URLS:
                url = f"https://www.tgb.cn/{path}"
                if not any(url == u for _, u in post_urls):
                    post_urls.append((title, url))
        
        # 去重
        seen = set()
        unique_urls = []
        for title, url in post_urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append((title, url))
        
        print(f"\n{'='*80}")
        print(f"[2/2] 批量抓取 {len(unique_urls)} 篇帖子内容...")
        
        all_posts = []
        for i, (title, url) in enumerate(unique_urls):
            print(f"\n  [{i+1}/{len(unique_urls)}] {title}")
            try:
                page.goto(url, wait_until="networkidle", timeout=25000)
                time.sleep(3)
                
                actual_title = page.title()
                
                # 检查是否需要登录
                body_text = page.inner_text("body")
                if "登录可查看全文" in body_text:
                    print(f"    ⚠️ 需要登录查看")
                    # 但我们有Cookie，应该已经登录了
                
                # 提取帖子正文内容
                # 淘股吧帖子正文通常在 class=p_cot 或某个特定div里
                content = page.evaluate("""() => {
                    // 方法1: 找class=p_cot
                    let el = document.querySelector('.p_cot');
                    if (el) return el.innerText;
                    // 方法2: 找class=font4
                    el = document.querySelector('.font4');
                    if (el) return el.innerText;
                    // 方法3: 找article
                    el = document.querySelector('article');
                    if (el) return el.innerText;
                    // 方法4: 用body
                    return document.body.innerText;
                }""")
                
                # 提取帖子中所有追帖/楼层（博主自己的）
                floors = page.evaluate("""() => {
                    const floors = [];
                    // 找所有楼层内容
                    document.querySelectorAll('[id*="reply"], .p_cot, .font4').forEach(el => {
                        const text = el.innerText.trim();
                        if (text.length > 20 && text.length < 5000) {
                            floors.push(text);
                        }
                    });
                    return floors;
                }""")
                
                # 提取图片URL
                images = page.evaluate("""() => {
                    return Array.from(document.querySelectorAll('img')).map(img => {
                        return img.getAttribute('data-original') || img.getAttribute('src') || '';
                    }).filter(s => s.includes('image.tgb') && !s.includes('user_icon'));
                }""")
                
                post = {
                    "title": title,
                    "actual_title": actual_title,
                    "url": url,
                    "content": content[:8000] if content else "",
                    "floors": floors[:50],  # 最多50层
                    "images": images,
                    "body_text": body_text[:10000],
                    "content_len": len(content) if content else 0,
                }
                all_posts.append(post)
                
                print(f"    ✅ 内容{len(content)}字, {len(floors)}层, {len(images)}图")
                
            except Exception as e:
                print(f"    ❌ 失败: {e}")
                all_posts.append({"title": title, "url": url, "error": str(e)})
            
            time.sleep(2)
        
        # 保存
        with open("/Users/ecustkiller/WorkBuddy/Claw/tgb_blog_all.json", "w", encoding="utf-8") as f:
            json.dump(all_posts, f, ensure_ascii=False, indent=2)
        
        print(f"\n{'='*80}")
        print(f"完成！共抓取 {len(all_posts)} 篇帖子")
        print(f"保存到: tgb_blog_all.json\n")
        
        # 打印摘要
        for i, p in enumerate(all_posts):
            if "error" in p:
                print(f"  [{i+1}] ❌ {p['title']} - {p['error'][:50]}")
            else:
                clen = p.get("content_len", 0)
                imgs = len(p.get("images", []))
                floors = len(p.get("floors", []))
                print(f"  [{i+1}] ✅ {p['title'][:40]} ({clen}字, {floors}层, {imgs}图)")
        
        browser.close()

if __name__ == "__main__":
    main()
