import asyncio
import httpx
import json
import re
import pandas as pd
from pathlib import Path
from selectolax.parser import HTMLParser
from datetime import datetime

# ============ تنظیمات ============
import os
BALE_TOKEN = os.environ.get("BALE_TOKEN", "")
CHAT_ID = int(os.environ.get("CHAT_ID", "0"))
SOURCE_FILE = "approved_prices.csv"      # 32 محصول در سایتت
STATE_FILE = "last_known_prices.json"    # قیمت‌های آخرین دفعه
LOG_FILE = "monitor.log"
CONCURRENCY = 8
TIMEOUT = 20
MIN_VALID_PRICE = 100_000
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# ============ اسکرپ قیمت ============
def parse_price(html: str):
    """قیمت را از HTML صفحه استخراج می‌کند."""
    # روش ۱: JSON-LD
    try:
        tree = HTMLParser(html)
        for node in tree.css('script[type="application/ld+json"]'):
            try:
                data = json.loads(node.text())
                items = data if isinstance(data, list) else [data]
                for item in items:
                    offers = item.get("offers") if isinstance(item, dict) else None
                    if offers:
                        offers_list = offers if isinstance(offers, list) else [offers]
                        for o in offers_list:
                            p = o.get("price")
                            if p:
                                p = int(float(str(p).replace(",", "")))
                                if p >= MIN_VALID_PRICE:
                                    return p, "jsonld"
            except Exception:
                continue
    except Exception:
        pass
    
    # روش ۲: Regex
    m = re.search(r'"price"\s*:\s*"?(\d{6,})"?', html)
    if m:
        p = int(m.group(1))
        if p >= MIN_VALID_PRICE:
            return p, "regex"
    
    # روش ۳: بررسی ناموجود
    if "ناموجود" in html or "اتمام موجودی" in html:
        return None, "out_of_stock"
    
    return None, "not_found"

async def fetch_price(client, product):
    """قیمت یک محصول را می‌گیرد."""
    url = product["noornegar_url"]
    try:
        r = await client.get(url, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
        if r.status_code == 404:
            return {**product, "price": None, "http_status": 404, "method": "404"}
        if r.status_code != 200:
            return {**product, "price": None, "http_status": r.status_code, "method": "http_error"}
        price, method = parse_price(r.text)
        return {**product, "price": price, "http_status": 200, "method": method}
    except Exception as e:
        return {**product, "price": None, "http_status": 0, "method": f"error:{type(e).__name__}"}

async def scrape_all(products):
    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient(follow_redirects=True) as client:
        async def bound(p):
            async with sem:
                return await fetch_price(client, p)
        return await asyncio.gather(*[bound(p) for p in products])

# ============ ارسال پیام به بله ============
def send_bale(text: str):
    url = f"https://tapi.bale.ai/bot{BALE_TOKEN}/sendMessage"
    try:
        r = httpx.post(url, json={"chat_id": CHAT_ID, "text": text}, timeout=15)
        if r.status_code == 200:
            return True
        log(f"❌ خطای بله: {r.status_code} - {r.text[:200]}")
        return False
    except Exception as e:
        log(f"❌ استثنای بله: {e}")
        return False

# ============ لاگ ============
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

# ============ خواندن/نوشتن state ============
def load_state():
    if Path(STATE_FILE).exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

# ============ منطق اصلی ============
def format_price(n):
    return f"{n:,}" if n else "—"

def check_changes():
    if not Path(SOURCE_FILE).exists():
        log(f"❌ فایل {SOURCE_FILE} پیدا نشد.")
        return
    
    df = pd.read_csv(SOURCE_FILE)
    products = df.to_dict("records")
    log(f"🔍 شروع بررسی {len(products)} محصول...")


# فقط دوربین‌های Sony و Canon (نه سه پایه، نه لنز)
df = df[
    df['title'].str.contains('Sony|Canon', case=False, na=False) &
    ~df['title'].str.contains('Tripod|سه پایه|Lens|لنز|Flash|فلاش|Bag|کیف|Battery|باتری|Charger|شارژر|Filter|فیلتر|Strap|Card|کارت', case=False, na=False)
]
print(f"تعداد محصولات بعد از فیلتر: {len(df)}")

    
    results = asyncio.run(scrape_all(products))
    state = load_state()
    
    changes = []
    ok_count = 0
    for r in results:
        pid = str(r["product_id"])
        new_price = r["price"]
        if new_price is None:
            continue
        ok_count += 1
        old_price = state.get(pid, {}).get("price")
        if old_price is None:
            # اولین بار — فقط ذخیره کن، پیام نده
            state[pid] = {"price": new_price, "title": r["title"], "url": r["noornegar_url"]}
        elif int(old_price) != int(new_price):
            changes.append({
                "title": r["title"],
                "url": r["noornegar_url"],
                "old": int(old_price),
                "new": int(new_price),
            })
            state[pid] = {"price": new_price, "title": r["title"], "url": r["noornegar_url"]}
    
    save_state(state)
    log(f"✅ {ok_count}/{len(products)} موفق اسکرپ شد. تغییرات: {len(changes)}")
    
    if not changes:
        return
    
    # ارسال پیام برای هر تغییر
    for c in changes:
        diff = c["new"] - c["old"]
        pct = (diff / c["old"]) * 100 if c["old"] else 0
        arrow = "📈" if diff > 0 else "📉"
        sign = "+" if diff > 0 else ""
        text = (
            f"🔔 تغییر قیمت نورنگار\n"
            f"📷 {c['title']}\n"
            f"💰 قبلی: {format_price(c['old'])}\n"
            f"💰 جدید: {format_price(c['new'])}\n"
            f"{arrow} اختلاف: {sign}{format_price(diff)} ({sign}{pct:.2f}%)\n"
            f"🔗 {c['url']}\n"
            f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )
        if send_bale(text):
            log(f"📤 پیام ارسال شد: {c['title']}")

if __name__ == "__main__":
    check_changes()
