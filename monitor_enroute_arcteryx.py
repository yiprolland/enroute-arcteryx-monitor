import asyncio
import json
import os
import re
import random
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ================== 配置 ==================
BASE = "https://enroute.run"
COLLECTION = "https://enroute.run/collections/arcteryx"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120 Safari/537.36"
)

SNAPSHOT = Path("snapshot.json")
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
# 默认不发“无变更也通知”；如需开启，设置环境变量 NOTIFY_ON_NO_CHANGE=true
NOTIFY_ON_NO_CHANGE = os.environ.get("NOTIFY_ON_NO_CHANGE", "false").lower() in ("1", "true", "yes", "on")

REQUEST_TIMEOUT = 20000   # 单次 HTTP 超时(ms)
MAX_PAGES = 20            # 集合页最多翻页数（HTTP & Playwright）
SCROLL_PAUSE = 700
MAX_CONCURRENCY = 8
HTTP_RETRIES = 3
TRY_VARIANT_QTY = True    # 尝试 /variants/<id>.json
# =================================================

def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def cents_to_str(cents: int | None, currency: str | None) -> str:
    if cents is None:
        return "-"
    cur = (currency or "USD").upper()
    sym = "$" if cur in ("USD", "CAD", "AUD", "NZD", "SGD") else f"{cur} "
    return f"{sym}{cents/100:.2f}"

def get_handle_from_url(url: str) -> str:
    path = urlparse(url).path.split("/")
    try:
        i = path.index("products")
        return path[i+1] if len(path) > i+1 else ""
    except ValueError:
        return ""

def parse_price_to_cents(v) -> int | None:
    if v is None:
        return None
    try:
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            return int(round(v * 100))
        s = str(v).strip().replace(",", "").replace("$", "")
        if re.match(r"^\d+(\.\d{1,2})?$", s):
            return int(round(float(s) * 100))
        if s.isdigit():
            return int(s)
    except Exception:
        return None
    return None

# ----------------- HTTP 客户端 -----------------
DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": COLLECTION,
}

class HttpClient:
    def __init__(self, timeout_ms=REQUEST_TIMEOUT):
        self.timeout = aiohttp.ClientTimeout(total=timeout_ms/1000)

    async def get_text(self, session: aiohttp.ClientSession, url: str, retries=HTTP_RETRIES):
        last_err = None
        for i in range(1, retries+1):
            try:
                async with session.get(url, headers=DEFAULT_HEADERS, timeout=self.timeout) as r:
                    if r.status == 200:
                        return await r.text()
                    elif r.status in (403, 404):
                        return None
                    last_err = f"HTTP {r.status}"
            except Exception as e:
                last_err = str(e)
            await asyncio.sleep(0.4 * i)
        if last_err:
            print(f"GET {url} text failed: {last_err}")
        return None

    async def get_json(self, session: aiohttp.ClientSession, url: str, retries=HTTP_RETRIES):
        last_err = None
        for i in range(1, retries+1):
            try:
                async with session.get(url, headers=DEFAULT_HEADERS | {"Accept": "application/json"}, timeout=self.timeout) as r:
                    if r.status == 200:
                        return await r.json()
                    elif r.status in (403, 404):
                        return None
                    last_err = f"HTTP {r.status}"
            except Exception as e:
                last_err = str(e)
            await asyncio.sleep(0.4 * i)
        if last_err:
            print(f"GET {url} json failed: {last_err}")
        return None

http = HttpClient()

# ----------------- 集合页：handles 获取（HTTP 分页优先 + Playwright 回退） -----------------
async def get_handles_via_playwright() -> list[str]:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
        )
        ctx = await browser.new_context(
            user_agent=USER_AGENT, viewport={"width": 1400, "height": 1000}, locale="en-US"
        )

        async def _route_filter(route):
            rt = route.request.resource_type
            if rt in ("image", "media", "font"):
                return await route.abort()
            return await route.continue_()
        await ctx.route("**/*", _route_filter)

        page = await ctx.new_page()
        handles = set()

        def norm_path(href: str) -> str:
            parts = href.split("?")[0].split("/")
            if len(parts) >= 3 and parts[1] == "products":
                return "/".join(parts[:3])
            return href.split("?")[0]

        async def collect():
            cards = await page.locator('a[href^="/products/"]').all()
            for a in cards:
                href = await a.get_attribute("href")
                if href and href.startswith("/products/"):
                    h = get_handle_from_url(norm_path(href))
                    if h:
                        handles.add(h)

        try:
            await page.goto(COLLECTION, wait_until="domcontentloaded", timeout=60000)
        except PWTimeout:
            await page.goto(COLLECTION, wait_until="commit")

        last_h = 0
        for _ in range(20):  # 更长滚动
            await collect()
            await page.mouse.wheel(0, 4000)
            await asyncio.sleep(SCROLL_PAUSE/1000)
            h = await page.evaluate("document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h

        # 兜底分页
        for p in range(2, MAX_PAGES + 1):
            try:
                resp = await page.goto(f"{COLLECTION}?page={p}", wait_until="domcontentloaded", timeout=25000)
                if not resp or resp.status != 200:
                    break
            except PWTimeout:
                break
            before = len(handles)
            await collect()
            if len(handles) == before:
                break

        await browser.close()
        return sorted(handles)

async def get_handles_via_http(session: aiohttp.ClientSession, max_pages: int = MAX_PAGES) -> list[str]:
    """
    纯 HTTP 解析集合页，并按 ?page=N 循环翻页，直到没有新增或到达 max_pages。
    """
    handles: set[str] = set()

    async def fetch_one(page_no: int) -> int:
        url = COLLECTION if page_no == 1 else f"{COLLECTION}?page={page_no}"
        html = await http.get_text(session, url)
        if not html:
            print(f"[HTTP] page {page_no}: 请求失败或无内容")
            return 0
        # 兼容大小写与不同结尾（引号/斜杠/参数）
        found = set(re.findall(r'href=["\'](?:https?://[^"\']+)?/products/([a-z0-9\-]+)(?:[/"\']|\?)', html, flags=re.I))
        before = len(handles)
        handles.update(found)
        added = len(handles) - before
        print(f"[HTTP] page {page_no}: 新增 {added} 个（累计 {len(handles)}）")
        return added

    page = 1
    while page <= max_pages:
        added = await fetch_one(page)
        if added == 0:
            break
        page += 1

    return sorted(handles)

async def get_all_product_handles() -> list[str]:
    # 先尝试 HTTP 分页抓取
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT/1000)) as s:
        handles = await get_handles_via_http(s, max_pages=MAX_PAGES)
        print(f"[HTTP] 集合页累计抓到 {len(handles)} 个 handle")
    # 如果 HTTP 抓取仍然很少（可能被风控/结构变化），回退到 Playwright
    if len(handles) < 30:
        print("[HTTP] 抓取数偏少，回退 Playwright 补抓…")
        handles = await get_handles_via_playwright()
        print(f"[PW] 补抓后共 {len(handles)} 个 handle")
    return handles

# ----------------- 产品详情抓取：三段式回退 -----------------
async def fetch_product_via_js(handle: str, session: aiohttp.ClientSession):
    return await http.get_json(session, f"{BASE}/products/{handle}.js")

async def fetch_product_via_json(handle: str, session: aiohttp.ClientSession):
    # 少数站点提供 .json（不是通用，但可一试）
    data = await http.get_json(session, f"{BASE}/products/{handle}.json")
    if isinstance(data, dict):
        return data.get("product") or data
    return None

def extract_variants_from_html(html: str):
    """
    从 HTML 中提取 variants 数组（常见于主题内嵌的 JSON）。
    尝试顺序：
      1) 任意 <script> 文本里出现 "variants":[ {...} ] 的数组
      2) script[type=application/ld+json] 的 Product/Offer（价格可得，变体可能缺少）
    """
    # 1) 粗略抓取 "variants": [...] 数组（Shopify 常见）
    m = re.search(r'"variants"\s*:\s*(\[\s*\{.*?\}\s*\])', html, flags=re.S | re.I)
    if m:
        try:
            arr = json.loads(m.group(1))
            return arr
        except Exception:
            pass
    # 2) 从 ld+json 抓价格/可售（可作为兜底）
    ld_blocks = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, flags=re.S | re.I)
    for block in ld_blocks:
        try:
            data = json.loads(block.strip())
        except Exception:
            continue
        obj = data[0] if isinstance(data, list) and data else data
        if not isinstance(obj, dict):
            continue
        offers = obj.get("offers")
        var_list = []
        if isinstance(offers, list):
            for off in offers:
                price = parse_price_to_cents(off.get("price"))
                available = str(off.get("availability", "")).lower().endswith("instock")
                var_list.append({"id": None, "option1": "", "option2": "", "price": price, "available": available})
        elif isinstance(offers, dict):
            price = parse_price_to_cents(offers.get("price"))
            available = str(offers.get("availability", "")).lower().endswith("instock")
            var_list.append({"id": None, "option1": "", "option2": "", "price": price, "available": available})
        if var_list:
            return var_list
    return None

async def fetch_product_via_html(handle: str, session: aiohttp.ClientSession):
    html = await http.get_text(session, f"{BASE}/products/{handle}")
    if not html:
        return None
    title = None
    m = re.search(r"<title>(.*?)</title>", html, flags=re.S | re.I)
    if m:
        # 简单清洗，避免站点后缀
        title = normalize_space(re.sub(r"-\s*Enroute.*$", "", m.group(1)))
    variants_arr = extract_variants_from_html(html) or []
    product = {
        "title": title or handle.replace("-", " "),
        "variants": variants_arr
    }
    return product

async def fetch_product(handle: str, session: aiohttp.ClientSession):
    """
    三段式回退：
      1) /products/<handle>.js
      2) /products/<handle>.json
      3) 抓 HTML 提取内嵌 JSON
    并尽量补齐 color/size/price/available/inventory_qty/sku
    """
    # 1) .js
    data = await fetch_product_via_js(handle, session)
    source = "js"
    if not data:
        # 2) .json
        data = await fetch_product_via_json(handle, session)
        source = "json"
    if not data:
        # 3) HTML
        data = await fetch_product_via_html(handle, session)
        source = "html"
    if not data:
        return None

    title = normalize_space((data.get("title") if isinstance(data, dict) else None) or handle.replace("-", " "))
    variants = []
    for v in (data.get("variants") or []):
        vid = v.get("id")
        price_cents = parse_price_to_cents(v.get("price"))
        available = bool(v.get("available", False))
        color = v.get("option2") or ""
        size  = v.get("option1") or ""
        sku   = v.get("sku") or ""
        if not color and isinstance(v.get("options"), list) and len(v["options"]) >= 2:
            color, size = v["options"][0], v["options"][1]

        inv_qty = None
        if TRY_VARIANT_QTY and vid and source in ("js", "json"):
            vi = await http.get_json(session, f"{BASE}/variants/{vid}.json")
            if vi and isinstance(vi.get("variant"), dict):
                q = vi["variant"].get("inventory_quantity")
                if isinstance(q, int):
                    inv_qty = q

        variants.append({
            "variant_id": str(vid) if vid else "",
            "sku": normalize_space(str(sku)),
            "color": normalize_space(str(color)),
            "size": normalize_space(str(size)),
            "available": available,
            "price_cents": price_cents,
            "inventory_qty": inv_qty
        })

    return {
        "handle": handle,
        "title": title or handle.replace("-", " "),
        "currency": "USD",
        "variants": variants,
        "url": f"{BASE}/products/{handle}",
    }

# ----------------- 快照 & Diff -----------------
def to_variant_key(entry: dict) -> str:
    if entry.get("variant_id"):
        return f"vid:{entry['variant_id']}"
    return f"name:{entry.get('title','')}|{entry.get('color','')}|{entry.get('size','')}"

def build_snapshot(products: dict[str, str], variants_map: dict[str, dict]) -> dict:
    return {"version": 2, "products": products, "variants": variants_map}

def read_snapshot() -> dict:
    if not SNAPSHOT.exists():
        return build_snapshot({}, {})
    try:
        data = json.loads(SNAPSHOT.read_text("utf-8"))
        if isinstance(data, dict) and "variants" not in data:
            return build_snapshot({}, data)
        if isinstance(data, dict):
            data.setdefault("products", {})
            data.setdefault("variants", {})
            return data
    except Exception:
        pass
    return build_snapshot({}, {})

def diff_events(old_snap: dict, new_snap: dict, currency: str):
    """
    事件类型：
      NEW_PRODUCT / NEW_VARIANT / PRICE_CHANGE / INVENTORY_INCREASE / INVENTORY_INCREASE_PRODUCT
    仅“库存增加”类事件会通知（减少不通知）；上新、价格变化也通知。
    """
    events = []
    old_p, new_p = old_snap.get("products", {}), new_snap.get("products", {})
    old_v, new_v = old_snap.get("variants", {}), new_snap.get("variants", {})

    # NEW_PRODUCT
    for h in sorted(set(new_p) - set(old_p)):
        events.append({"type": "NEW_PRODUCT", "handle": h, "title": new_p[h]})

    # 变体对比
    for k, nv in new_v.items():
        ov = old_v.get(k)
        if ov is None:
            events.append({
                "type": "NEW_VARIANT",
                "key": k, "handle": nv.get("handle"),
                "title": nv.get("title"),
                "color": nv.get("color"),
                "size": nv.get("size"),
                "sku": nv.get("sku"),
                "price_cents": nv.get("price_cents"),
                "url": nv.get("url")
            })
            continue
        np, op = nv.get("price_cents"), ov.get("price_cents")
        if np is not None and op is not None and np != op:
            events.append({
                "type": "PRICE_CHANGE",
                "key": k, "handle": nv.get("handle"),
                "title": nv.get("title"),
                "color": nv.get("color"),
                "size": nv.get("size"),
                "sku": nv.get("sku"),
                "old_price": op,
                "new_price": np,
                "currency": currency,
                "url": nv.get("url")
            })
        n_q, o_q = nv.get("inventory_qty"), ov.get("inventory_qty")
        if isinstance(n_q, int) and isinstance(o_q, int) and n_q > o_q:
            events.append({
                "type": "INVENTORY_INCREASE",
                "key": k, "handle": nv.get("handle"),
                "title": nv.get("title"),
                "color": nv.get("color"),
                "size": nv.get("size"),
                "sku": nv.get("sku"),
                "old_qty": o_q,
                "new_qty": n_q,
                "price_cents": nv.get("price_cents"),
                "url": nv.get("url")
            })
        else:
            if ov.get("available") is False and nv.get("available") is True:
                events.append({
                    "type": "INVENTORY_INCREASE",
                    "key": k, "handle": nv.get("handle"),
                    "title": nv.get("title"),
                    "color": nv.get("color"),
                    "size": nv.get("size"),
                    "sku": nv.get("sku"),
                    "old_qty": None,
                    "new_qty": None,
                    "price_cents": nv.get("price_cents"),
                    "url": nv.get("url")
                })

    # 产品维度：可购变体数增加
    def avail_count(variants: dict[str, dict]) -> dict[str, int]:
        cnt = {}
        for v in variants.values():
            h = v.get("handle")
            if h and v.get("available") is True:
                cnt[h] = cnt.get(h, 0) + 1
        return cnt

    oc, nc = avail_count(old_v), avail_count(new_v)
    for h, val in nc.items():
        if val > oc.get(h, 0):
            events.append({
                "type": "INVENTORY_INCREASE_PRODUCT",
                "handle": h,
                "title": new_p.get(h, h),
                "old_count": oc.get(h, 0),
                "new_count": val
            })
    return events

# ----------------- 文本格式化 & 发送 -----------------
SIZE_ORDER = ["XXS", "XS", "S", "M", "L", "XL", "XXL", "2XL", "3XL"]
def sort_size_key(s: str) -> int:
    s = s.upper()
    if s in SIZE_ORDER: return SIZE_ORDER.index(s)
    # 数字码（如 28/30/...）
    m = re.match(r"^(\d+)", s)
    if m: return 100 + int(m.group(1))
    return 999

def build_inventory_index(new_vars: dict[str, dict]):
    """
    生成当前可用库存索引：handle -> color -> { size: qty_or_flag, ... }
    qty_or_flag: int 数量 或 "有货"
    同时也返回一个 (handle,color,size) -> sku 的索引，方便取货号
    以及 (handle,color,size) -> price_cents 的索引
    """
    inv = {}
    sku_idx = {}
    price_idx = {}

    for v in new_vars.values():
        h = v.get("handle"); c = v.get("color") or ""; s = v.get("size") or ""
        if not h or not c or not s:
            continue
        if v.get("available") is not True:
            continue
        q = v.get("inventory_qty")
        val = q if isinstance(q, int) and q > 0 else "有货"
        inv.setdefault(h, {}).setdefault(c, {})
        inv[h][c][s] = val

        sk = v.get("sku") or ""
        if sk:
            sku_idx[(h, c, s)] = sk
        price_idx[(h, c, s)] = v.get("price_cents")
    return inv, sku_idx, price_idx

def format_sizes_line(size_map: dict[str, object]) -> str:
    if not size_map:
        return "—"
    items = []
    for sz in sorted(size_map.keys(), key=sort_size_key):
        v = size_map[sz]
        if isinstance(v, int):
            items.append(f"{sz}: {v}")
        else:
            items.append(f"{sz}: 有货")
    return " | ".join(items)

def find_sku_for_event(e, sku_idx) -> str:
    # 优先精确 (handle,color,size)，否则尝试任意该 handle 的 sku
    h, c, s = e.get("handle"), e.get("color"), e.get("size")
    if h and c and s and (h, c, s) in sku_idx:
        return sku_idx[(h, c, s)]
    # 退化：找该 handle 的任意 sku
    for (hh, _, _), sk in sku_idx.items():
        if hh == h and sk:
            return sk
    return "-"

def format_price_line(e) -> str:
    if e.get("type") == "PRICE_CHANGE":
        return f"$ {cents_to_str(e['old_price'], e.get('currency')).replace('$','').strip()} → $ {cents_to_str(e['new_price'], e.get('currency')).replace('$','').strip()}"
    pc = e.get("price_cents")
    if pc is None:
        return "-"
    return f"$ {cents_to_str(pc, e.get('currency')).replace('$','').strip()}"

def format_event_text(e: dict, inv_index, sku_idx, price_idx) -> str:
    h = e.get("handle")
    title = e.get("title") or h or "-"
    color = e.get("color") or "-"
    sizes_line = "—"
    if h and color != "-":
        sizes_line = format_sizes_line(inv_index.get(h, {}).get(color, {}))
    sku = find_sku_for_event(e, sku_idx)
    price_line = format_price_line(e)
    url = e.get("url") or f"{BASE}/products/{h}"

    lines = [
        f"• 名称：{title}",
        f"• 货号：{sku}",
        f"• 颜色：{color}",
        f"• 价格：{price_line}",
        f"📊 库存信息：{sizes_line}",
        f"🔗 直达链接\n{url}"
    ]
    return "\n".join(lines)

async def send_text(msg: str):
    if not DISCORD_WEBHOOK:
        print("WARN: 未设置 DISCORD_WEBHOOK_URL，跳过通知。")
        return
    async with aiohttp.ClientSession() as session:
        async with session.post(DISCORD_WEBHOOK, json={"content": msg}, timeout=30) as resp:
            body = await resp.text()
            print(f"Discord text status={resp.status}")
            if resp.status >= 300:
                print("Discord 文本推送失败:", resp.status, body)

async def send_texts_individually(msgs: list[str], pause_sec: float = 0.5):
    """逐条发送，避免 2000 字限制与 embeds 限制。"""
    for i, m in enumerate(msgs, 1):
        await send_text(m)
        await asyncio.sleep(pause_sec)

# ----------------- 主流程 -----------------
def build_snapshot(products: dict[str, str], variants_map: dict[str, dict]) -> dict:
    return {"version": 2, "products": products, "variants": variants_map}

async def run_once():
    print("收集商品 handle ...")
    handles = await get_all_product_handles()
    print(f"共发现 {len(handles)} 个商品 handle")

    old_snap = read_snapshot()

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    products: dict[str, dict] = {}

    async def worker(handle: str, session: aiohttp.ClientSession):
        async with semaphore:
            for t in range(1, HTTP_RETRIES+1):
                try:
                    prod = await fetch_product(handle, session)
                    if prod:
                        products[handle] = prod
                    return
                except Exception as e:
                    if t == HTTP_RETRIES:
                        print(f"产品抓取失败: {handle} -> {e}")
                    await asyncio.sleep(0.6 * t + random.random() * 0.3)

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT/1000)) as session:
        tasks = [asyncio.create_task(worker(h, session)) for h in handles]
        done = 0
        for f in asyncio.as_completed(tasks):
            await f
            done += 1
            if done % 20 == 0:
                print(f"已完成 {done}/{len(tasks)}")

    succ_products = len(products)
    succ_variants = sum(len(p.get("variants") or []) for p in products.values())
    print(f"成功解析：商品 {succ_products} 个，变体 {succ_variants} 条")

    new_products: dict[str, str] = {}
    new_variants: dict[str, dict] = {}
    currency_seen = "USD"

    for handle, prod in products.items():
        title = prod["title"]
        new_products[handle] = title
        currency_seen = prod.get("currency") or currency_seen
        url = prod.get("url")
        for v in prod.get("variants", []):
            entry = {
                "handle": handle,
                "title": title,
                "color": v.get("color", ""),
                "size": v.get("size", ""),
                "available": bool(v.get("available")),
                "price_cents": v.get("price_cents"),
                "inventory_qty": v.get("inventory_qty"),
                "variant_id": v.get("variant_id"),
                "sku": v.get("sku", ""),
                "url": url,
                "currency": currency_seen,
            }
            k = to_variant_key(entry)
            new_variants[k] = entry

    new_snap = build_snapshot(new_products, new_variants)

    events = diff_events(old_snap, new_snap, currency_seen)
    print(f"事件条目：{len(events)}")

    SNAPSHOT.write_text(json.dumps(new_snap, ensure_ascii=False, indent=2), "utf-8")

    if events:
        inv_index, sku_idx, price_idx = build_inventory_index(new_snap.get("variants", {}))
        msgs = [format_event_text(e, inv_index, sku_idx, price_idx) for e in events]
        await send_texts_individually(msgs, pause_sec=0.4)
    elif NOTIFY_ON_NO_CHANGE:
        await send_text("运行成功：本次无上新、无价格变化、无库存增加。")

# 支持单品调试：DEBUG_ONE_HANDLE=arcteryx-mantis-2-waist-pack
if __name__ == "__main__":
    dbg = os.environ.get("DEBUG_ONE_HANDLE", "").strip()
    if dbg:
        async def _single():
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT/1000)) as s:
                prod = await fetch_product(dbg, s)
                print(json.dumps(prod, ensure_ascii=False, indent=2))
        asyncio.run(_single())
    else:
        asyncio.run(run_once())
