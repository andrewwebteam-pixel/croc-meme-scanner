import re
import asyncio
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from dotenv import load_dotenv
import aiohttp
import base64

# === Config ===
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_KEY = os.getenv("ADMIN_KEY", "ADMIN-ROOT-ACCESS")
DB_PATH = os.getenv("DB_PATH", "/opt/crocbrains/keys.db")
PRODUCT = os.getenv("PRODUCT", "meme_scanner")
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "").strip()
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "").strip()  # future use
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "").strip()
HELIUS_RPC_URL = os.getenv("HELIUS_RPC_URL", "").strip() or (f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else "")

# cooldown seconds per user for /scan
SCAN_COOLDOWN_SEC = int(os.getenv("SCAN_COOLDOWN_SEC", "30"))

assert BOT_TOKEN, "BOT_TOKEN is required"

BIRDEYE_BASE = "https://public-api.birdeye.so"

# === Simple in-memory cache for /scan results ===
SCAN_CACHE_TTL = 15  # seconds
_scan_cache: Dict[str, Any] = {"ts": 0.0, "pairs": []}

# === In-chat pagination sessions for /scan ===
_scan_sessions: Dict[str, Dict[str, Any]] = {}
SCAN_SESSION_TTL = 900  # 15 минут

def _scan_sid() -> str:
    return f"s{int(time.time()*1000)}"

def _scan_gc():
    now = time.time()
    dead = [sid for sid, v in _scan_sessions.items() if v.get("ts", 0) + SCAN_SESSION_TTL < now]
    for sid in dead:
        _scan_sessions.pop(sid, None)


# === Global API rate limiter ===
_last_api_call_ts = 0.0
_api_lock = asyncio.Lock()

async def api_rate_limit(min_interval_sec: float = 1.1):
    """Ensure ~1 RPS (Birdeye free). For Helius RPC we’ll call with smaller interval."""
    global _last_api_call_ts
    async with _api_lock:
        now = time.time()
        wait = (_last_api_call_ts + min_interval_sec) - now
        if wait > 0:
            await asyncio.sleep(wait)
        _last_api_call_ts = time.time()

# === DB helpers ===
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS access_keys (
            access_key TEXT PRIMARY KEY,
            product TEXT NOT NULL,
            expires_at TEXT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_access (
            user_id INTEGER PRIMARY KEY,
            access_key TEXT NOT NULL,
            FOREIGN KEY(access_key) REFERENCES access_keys(access_key)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_throttle (
            user_id INTEGER PRIMARY KEY,
            last_scan_ts INTEGER NOT NULL
        )
    """)
    return conn

def seed_initial_keys():
    conn = db()
    conn.execute("INSERT OR IGNORE INTO access_keys VALUES (?, ?, NULL)", (ADMIN_KEY, PRODUCT))
    conn.execute("INSERT OR IGNORE INTO access_keys VALUES (?, ?, ?)", ("TEST-1234", PRODUCT, "2099-12-31"))
    conn.commit()
    conn.close()

def key_info(access_key: str) -> Optional[tuple]:
    conn = db()
    cur = conn.execute("SELECT access_key, product, expires_at FROM access_keys WHERE access_key = ?", (access_key,))
    row = cur.fetchone()
    conn.close()
    return row

def bind_user(user_id: int, access_key: str):
    conn = db()
    conn.execute("INSERT OR REPLACE INTO user_access(user_id, access_key) VALUES (?, ?)", (user_id, access_key))
    conn.commit()
    conn.close()

def get_user_key(user_id: int) -> Optional[str]:
    conn = db()
    cur = conn.execute("SELECT access_key FROM user_access WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

def is_key_valid_for_product(access_key: str) -> tuple[bool, str]:
    info = key_info(access_key)
    if not info:
        return False, "Invalid key."
    _, product, expires_at = info
    if product != PRODUCT:
        return False, "This key is for a different product."
    if expires_at is None:
        return True, "Lifetime access (admin/NFT)."
    try:
        if datetime.utcnow().date() <= datetime.fromisoformat(expires_at).date():
            return True, f"Access valid until {expires_at}."
        else:
            return False, "Key has expired."
    except Exception:
        return False, "Invalid key expiry format."

# === Throttle helpers ===
def get_last_scan_ts(user_id: int) -> int:
    conn = db()
    cur = conn.execute("SELECT last_scan_ts FROM user_throttle WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else 0

def set_last_scan_ts(user_id: int, ts: int):
    conn = db()
    conn.execute("INSERT OR REPLACE INTO user_throttle(user_id, last_scan_ts) VALUES (?, ?)", (user_id, ts))
    conn.commit()
    conn.close()

# === Utils / formatting ===
def format_usd(v: Optional[float]) -> str:
    if v is None:
        return "—"
    try:
        v = float(v)
    except Exception:
        return "—"
    if v >= 1_000_000: return f"${v/1_000_000:.2f}M"
    if v >= 1_000:     return f"${v/1_000:.2f}k"
    if v >= 1:         return f"${v:.2f}"
    return f"${v:.6f}"

def from_unix_ms(ms: Optional[int]) -> Optional[datetime]:
    if not ms: return None
    if ms > 10_000_000_000:  # millis
        ms = ms / 1000.0
    try:
        return datetime.fromtimestamp(ms, tz=timezone.utc)
    except Exception:
        return None

def human_age(dt: Optional[datetime]) -> str:
    if not dt: return "—"
    delta = datetime.now(tz=timezone.utc) - dt
    hours = int(delta.total_seconds() // 3600)
    if hours < 24: return f"{hours}h"
    days = hours // 24
    return f"{days}d"
    
def md_escape(s: str) -> str:
    """
    Экранирует спецсимволы Markdown в строке, чтобы символ/имя токена
    не «ломали» разметку при ParseMode.MARKDOWN.
    """
    if not s:
        return ""
    # Экранируем базовый набор для Markdown v1: _ * [ ] ( )
    return re.sub(r'([_*[\]()])', r'\\\1', str(s))


# === Normalizer for /token argument ===
_mint_re = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")  # base58 32..44 chars

def normalize_mint_arg(raw: str) -> Optional[str]:
    s = (raw or "").strip()
    if not s:
        return None
    # 1) full URL Birdeye/Solscan
    m = re.search(r"/token/([1-9A-HJ-NP-Za-km-z]{32,44})", s)
    if m:
        return m.group(1)
    # 2) “SYMBOL (MINT)”
    m = re.search(r"\(([1-9A-HJ-NP-Za-km-z]{32,44})\)", s)
    if m:
        return m.group(1)
    # 3) bare mint
    m = _mint_re.search(s)
    return m.group(0) if m else None

# === Jupiter price fallback (no key required) ===
async def jupiter_price(session: aiohttp.ClientSession, mint: str) -> Optional[float]:
    try:
        url = "https://price.jup.ag/v6/price"
        params = {"ids": mint}
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return None
            j = await r.json()
            data = (j or {}).get("data") or {}
            rec = data.get(mint) or {}
            price = rec.get("price")
            if price is None:
                return None
            return float(price)
    except Exception:
        return None

# === Birdeye fetchers ===
async def fetch_latest_sol_pairs(limit: int = 8) -> List[Dict[str, Any]]:
    if (_scan_cache["ts"] + SCAN_CACHE_TTL) > time.time() and _scan_cache["pairs"]:
        return _scan_cache["pairs"][:limit]
    if not BIRDEYE_API_KEY:
        return []
    url = f"{BIRDEYE_BASE}/defi/markets"
    headers = {"accept": "application/json", "X-API-KEY": BIRDEYE_API_KEY}
    params = {"chain": "solana", "sort_by": "liquidity", "sort_type": "desc", "offset": 0, "limit": 50}
    try:
        await api_rate_limit()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(url, headers=headers, params=params) as r:
                if r.status != 200:
                    return []
                j = await r.json()
                if not j or not j.get("success"):
                    return []
                data = j.get("data") or []
                pairs = []
                for m in data:
                    try:
                        base = {
                            "symbol": m.get("symbol") or "",
                            "name": m.get("name") or "",
                            "address": m.get("address") or m.get("baseMint") or ""
                        }
                        pairs.append({
                            "baseToken": base,
                            "priceUsd": m.get("price"),
                            "liquidity": {"usd": m.get("liquidity") or m.get("liquidityUsd")},
                            "fdv": m.get("marketCap"),
                            "volume": {"h24": m.get("v24") or m.get("volume24h")},
                            "pairCreatedAt": m.get("createdAt") or m.get("firstTradeAt"),
                            "chainId": "solana",
                        })
                    except Exception:
                        continue
                _scan_cache["ts"] = time.time()
                _scan_cache["pairs"] = pairs
                return pairs[:limit]
    except Exception:
        return []

async def birdeye_overview(session: aiohttp.ClientSession, mint: str) -> Optional[Dict[str, Any]]:
    if not BIRDEYE_API_KEY:
        return None
    url = f"{BIRDEYE_BASE}/defi/token_overview"
    headers = {"accept": "application/json", "X-API-KEY": BIRDEYE_API_KEY}
    params = {"address": mint, "chain": "solana"}
    try:
        await api_rate_limit()
        async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return None
            j = await r.json()
            if not j or not j.get("success"):
                return None
            return j.get("data") or j
    except Exception:
        return None

async def birdeye_markets(session: aiohttp.ClientSession, mint: str) -> Optional[List[Dict[str, Any]]]:
    if not BIRDEYE_API_KEY:
        return None
    url = f"{BIRDEYE_BASE}/defi/markets"
    headers = {"accept": "application/json", "X-API-KEY": BIRDEYE_API_KEY}
    params = {"address": mint, "chain": "solana", "sort_by": "liquidity", "sort_type": "desc"}
    try:
        await api_rate_limit()
        async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return None
            j = await r.json()
            if not j or not j.get("success"):
                return None
            data = j.get("data") or []
            return data if isinstance(data, list) else None
    except Exception:
        return None

def extract_holders(data: Dict[str, Any]) -> Optional[int]:
    for k in ("holders", "holder", "holder_count", "holdersCount"):
        v = data.get(k)
        if isinstance(v, (int, float)) and v >= 0:
            return int(v)
    return None

def extract_lp_lock_ratio(data: Dict[str, Any]) -> Optional[float]:
    for k in ("lp_lock_ratio", "lpLockRatio", "lp_locked", "lpLockedRatio"):
        v = data.get(k)
        try:
            if v is None: continue
            v = float(v)
            return v*100 if 0 <= v <= 1 else v
        except Exception:
            continue
    return None

def extract_created_at(data: Dict[str, Any]) -> Optional[datetime]:
    for k in ("createdAt", "firstTradeAt", "first_trade_at", "first_trade_unix"):
        v = data.get(k)
        if v is None: continue
        try:
            v = int(v)
        except Exception:
            try:
                return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            except Exception:
                continue
        return from_unix_ms(v)
    return None

def exchanges_block(markets: Optional[List[Dict[str, Any]]]) -> str:
    if not markets:
        return "Exchanges: —"
    cleaned = []
    for m in markets:
        dex = m.get("dex") or m.get("market") or m.get("name")
        liq = m.get("liquidity") or m.get("liquidityUsd") or (m.get("liquidity", {}) or {}).get("usd")
        try:
            liq = float(liq) if liq is not None else None
        except Exception:
            liq = None
        if dex and liq is not None:
            cleaned.append((dex, liq))
    if not cleaned:
        return "Exchanges: —"
    cleaned.sort(key=lambda x: x[1], reverse=True)
    top = cleaned[:2]
    lines = ["Exchanges:"]
    for dex, liq in top:
        lines.append(f"- {dex}: {format_usd(liq)} liquidity")
    return "\n".join(lines)

# === Risk flags helper ===
def risk_flags(mint_active: bool, freeze_active: bool, top10_share: Optional[float]) -> List[str]:
    flags = []
    if mint_active:   flags.append("Mint authority active")
    if freeze_active: flags.append("Freeze authority active")
    try:
        if top10_share is not None and top10_share >= 70.0:
            flags.append(f"Top-10 concentration {top10_share:.1f}%")
    except Exception:
        pass
    return flags

def token_card(p: Dict[str, Any], extra: Optional[Dict[str, Any]], extra_flags: Optional[List[str]] = None) -> str:
    base = p.get("baseToken", {}) or {}
    symbol = base.get("symbol") or "?"
    name   = base.get("name") or "Unknown"
    price  = p.get("priceUsd")
    price_txt = format_usd(price)

    liq_usd = (p.get("liquidity") or {}).get("usd")
    fdv     = p.get("fdv")
    vol24   = (p.get("volume") or {}).get("h24")

    age_dt = extract_created_at(extra) if extra else None
    if not age_dt:
        age_dt = from_unix_ms(p.get("pairCreatedAt"))
    age_txt = human_age(age_dt)

    holders = extract_holders(extra or {}) if extra else None
    lp_lock = extract_lp_lock_ratio(extra or {}) if extra else None

    risk = []
    if liq_usd is not None and liq_usd < 10_000: risk.append("Low liquidity")
    if vol24  is not None and vol24  <  5_000:  risk.append("Low volume")
    if extra_flags:
        risk.extend(extra_flags)

    lines = [
        f"🐊 *{md_escape(symbol)}* — _{md_escape(name)}_",
        f"Price: {price_txt}",
        f"Liquidity: {format_usd(liq_usd)}",
        f"Market Cap: {format_usd(fdv)}",
        f"Volume 24h: {format_usd(vol24)}",
        f"Age: {age_txt}",
    ]
    if holders is not None:
        lines.append(f"Holders: {holders:,}")
    else:
        lines.append("Holders: Hidden on basic plan")
    if lp_lock is not None:
        lines.append(f"LP Locked: {lp_lock:.1f}%")
    else:
        lines.append("LP Locked: Hidden on basic plan")

    if risk:
        lines.append(f"⚠️ {', '.join(risk)}")

    return "\n".join(lines)

def token_keyboard(p: Dict[str, Any], mode: str = "summary") -> InlineKeyboardMarkup:
    """
    mode: "summary" -> показываем кнопку ℹ️ Details (ведёт в details)
          "details" -> показываем кнопку ◀ Back (ведёт в summary)
    """
    base = p.get("baseToken", {}) or {}
    mint = base.get("address", "") or ""

    be_link = f"https://birdeye.so/token/{mint}?chain=solana" if mint else "https://birdeye.so/solana"
    solscan_link = f"https://solscan.io/token/{mint}" if mint else "https://solscan.io"
    jup_link = f"https://jup.ag/swap?outputMint={mint}" if mint else "https://jup.ag"
    # вкладка графика на Birdeye (если нет — обычная ссылка сработает так же)
    chart_link = be_link + "&t=chart" if mint else be_link

    if mode == "details":
        toggle_btn = InlineKeyboardButton(text="◀ Back", callback_data=f"token:{mint}:summary")
    else:
        toggle_btn = InlineKeyboardButton(text="ℹ️ Details", callback_data=f"token:{mint}:details")

    # Кнопка Holders открывает details (там уже есть блок on-chain/top-10)
    holders_btn = InlineKeyboardButton(text="Holders", callback_data=f"token:{mint}:details")

    return InlineKeyboardMarkup(inline_keyboard=[
        [toggle_btn],
        [InlineKeyboardButton(text="Buy (Jupiter)", url=jup_link)],
        [InlineKeyboardButton(text="Chart", url=chart_link), holders_btn],
        [InlineKeyboardButton(text="Open on Birdeye", url=be_link)],
        [InlineKeyboardButton(text="Open on Solscan", url=solscan_link)],
        [InlineKeyboardButton(text="Share", switch_inline_query=f"{mint}")]
    ])

def scan_keyboard(sid: str, idx: int, total: int, mint: str) -> InlineKeyboardMarkup:
    prev_i = (idx - 1) % total
    next_i = (idx + 1) % total
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀ Prev", callback_data=f"scan:session:{sid}:idx:{prev_i}"),
         InlineKeyboardButton(text=f"{idx+1}/{total}", callback_data="noop"),
         InlineKeyboardButton(text="Next ▶", callback_data=f"scan:session:{sid}:idx:{next_i}")],
        [InlineKeyboardButton(text="ℹ️ Details", callback_data=f"scan:session:{sid}:detail:{idx}")],
    ])

# === Callback: summary/details toggle for a single token card ===
@dp.callback_query(F.data.startswith("token:"))
async def token_callback_handler(c: CallbackQuery):
    # 0) Мгновенно закрываем «часики», чтобы не словить timeout «query is too old»
    try:
        await c.answer()
    except Exception:
        pass

    # 1) Проверка доступа (как в /my)
    key = get_user_key(c.from_user.id)
    if not key:
        # покажем alert, если ещё не поздно
        try:
            await c.answer("No access. Use /start", show_alert=True)
        except Exception:
            pass
        return
    ok, _ = is_key_valid_for_product(key)
    if not ok:
        try:
            await c.answer("Access invalid", show_alert=True)
        except Exception:
            pass
        return

    # 2) Разбор callback-data: token:<mint>:details|summary
    parts = (c.data or "").split(":")
    if len(parts) < 2:
        try:
            await c.answer("Mint missing", show_alert=True)
        except Exception:
            pass
        return

    mint = parts[1].strip()
    mode = parts[2].strip() if len(parts) >= 3 else "details"

    # 3) Сборка текста и правка текущего сообщения (edit_message_text)
    try:
        if mode == "details":
            # Детальная карточка
            text, p = await build_details_text(mint)
            kb = token_keyboard(p, mode="details")
        else:
            # Краткая карточка
            text, p = await build_summary_text(mint)
            kb = token_keyboard(p, mode="summary")

        # Правим текущее сообщение in-place
        await bot.edit_message_text(
            chat_id=c.message.chat.id,
            message_id=c.message.message_id,
            text=text,
            reply_markup=kb,
            disable_web_page_preview=True,
        )
    except Exception as e:
        # «message is not modified» игнорируем, остальное показываем алертом, если возможно
        try:
            from aiogram.exceptions import TelegramBadRequest
            if isinstance(e, TelegramBadRequest) and "message is not modified" in str(e).lower():
                return
        except Exception:
            pass
        try:
            await c.answer("Failed to update message", show_alert=True)
        except Exception:
            pass

# === Callback: pagination & details inside /scan session ===
@dp.callback_query(F.data.startswith("scan:"))
async def scan_cb_handler(c: CallbackQuery):
    # Форматы:
    # scan:session:<sid>:idx:<i>     -> показать пару с индексом i (summary)
    # scan:session:<sid>:detail:<i>  -> показать пару с индексом i (details)
    try:
        await c.answer()
    except Exception:
        pass

    parts = (c.data or "").split(":")
    # ожидаем минимум 5 частей: ["scan","session",SID,"idx|detail",INDEX]
    if len(parts) < 5:
        return

    sid = parts[2]
    action = parts[3]
    try:
        idx = int(parts[4])
    except Exception:
        return

    # Берём сессию
    sess = _scan_sessions.get(sid)
    if not sess:
        # Сессия протухла — сообщим мягко
        try:
            await c.answer("Session expired. Run /scan again.", show_alert=True)
        except Exception:
            pass
        return

    pairs = sess.get("pairs") or []
    if not pairs:
        return

    total = len(pairs)
    idx = idx % total
    p = pairs[idx]
    mint = (p.get("baseToken") or {}).get("address", "")

    # Подтягиваем доп-данные (Birdeye по текущему mint)
    extra = None
    mkts = None
    if mint:
        async with aiohttp.ClientSession() as session:
            if BIRDEYE_API_KEY:
                try:
                    extra = await birdeye_overview(session, mint)
                except Exception:
                    extra = None
                try:
                    mkts = await birdeye_markets(session, mint)
                except Exception:
                    mkts = None

    # Готовим «вид» пары для token_card (тот же формат, что и при отправке)
    p_view = {
        "baseToken": p.get("baseToken") or {},
        "priceUsd": p.get("priceUsd"),
        "liquidity": p.get("liquidity"),
        "fdv": p.get("fdv"),
        "volume": p.get("volume"),
        "pairCreatedAt": p.get("pairCreatedAt"),
        "chainId": "solana",
    }

    # summary/details режимы внутри /scan
    if action == "detail":
        # Детальный текст: карточка + биржи (в рамках простого UI без on-chain здесь)
        text = token_card(p_view, extra, extra_flags=None) + "\n\n" + (exchanges_block(mkts) if mkts is not None else "")
    else:
        # Summary текст
        text = token_card(p_view, extra, extra_flags=None) + "\n\n" + (exchanges_block(mkts) if mkts is not None else "")

    kb = scan_keyboard(sid, idx, total, mint)

    try:
        await bot.edit_message_text(
            chat_id=c.message.chat.id,
            message_id=c.message.message_id,
            text=text,
            reply_markup=kb,
            disable_web_page_preview=True,
        )
    except Exception:
        # молча — это внутри своей сессии
        pass



# === Helius RPC helpers ===
async def helius_rpc(session: aiohttp.ClientSession, method: str, params: list) -> Optional[dict]:
    if not HELIUS_RPC_URL:
        return None
    try:
        await api_rate_limit(min_interval_sec=0.12)
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        async with session.post(HELIUS_RPC_URL, json=payload, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status != 200:
                return None
            return await r.json()
    except Exception:
        return None

def _u64_le(buf: bytes, off: int) -> int:
    return int.from_bytes(buf[off:off+8], "little", signed=False)

def _pubkey_hex(buf: bytes, off: int) -> str:
    return buf[off:off+32].hex()

async def helius_get_mint_info(session: aiohttp.ClientSession, mint: str) -> Optional[dict]:
    j = await helius_rpc(session, "getAccountInfo", [mint, {"encoding": "base64", "commitment": "finalized"}])
    if not j or "result" not in j or not j["result"] or not j["result"].get("value"):
        return None
    try:
        data_b64 = j["result"]["value"]["data"][0]
        raw = base64.b64decode(data_b64)
        if len(raw) < 82:
            return None
        mint_opt = int.from_bytes(raw[0:4], "little")
        mint_auth = _pubkey_hex(raw, 4) if mint_opt == 1 else None
        supply   = _u64_le(raw, 36)
        decimals = raw[44]
        freeze_opt = int.from_bytes(raw[46:50], "little")
        freeze_auth = _pubkey_hex(raw, 50) if freeze_opt == 1 else None
        return {
            "mintAuthority": mint_auth,
            "freezeAuthority": freeze_auth,
            "supply": supply,
            "decimals": decimals,
        }
    except Exception:
        return None

async def helius_get_token_supply(session: aiohttp.ClientSession, mint: str) -> Optional[dict]:
    j = await helius_rpc(session, "getTokenSupply", [mint])
    if not j or not j.get("result"):
        return None
    v = j["result"]["value"]
    try:
        amount = int(v["amount"])
        decimals = int(v["decimals"])
        return {"supply": amount, "decimals": decimals}
    except Exception:
        return None

async def helius_top_holders_share(session: aiohttp.ClientSession, mint: str, k: int = 10) -> Optional[float]:
    j = await helius_rpc(session, "getTokenLargestAccounts", [mint, {"commitment": "finalized"}])
    if not j or not j.get("result"):
        return None
    try:
        values = j["result"]["value"] or []
        amounts = []
        for it in values[:k]:
            a = it.get("amount")
            if a is None:
                continue
            amounts.append(int(a))
        sup = None
        info = await helius_get_mint_info(session, mint)
        if info and info.get("supply") is not None:
            sup = int(info["supply"])
        if sup is None:
            ts = await helius_get_token_supply(session, mint)
            if ts and ts.get("supply") is not None:
                sup = int(ts["supply"])
        if not sup or sup == 0:
            return None
        share = (sum(amounts) / sup) * 100.0
        return float(share)
    except Exception:
        return None

def format_authority(pk_hex: Optional[str]) -> str:
    if not pk_hex:
        return "revoked"
    short = pk_hex[:4] + "…" + pk_hex[-4:]
    return f"active ({short})"

def format_topk_share(v: Optional[float]) -> str:
    if v is None:
        return "—"
    return f"{v:.2f}%"

# === Bot ===
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher()

@dp.message(Command("start"))
async def start_handler(m: Message):
    if get_user_key(m.from_user.id):
        await m.answer(
            "✅ Access confirmed.\n\n"
            "Commands:\n"
            "/scan — scan new memes (Birdeye)\n"
            "/token <mint> — show token card\n"
            "/my — my access status\n"
            "/logout — unlink key\n"
            "/help — show help"
        )
    else:
        await m.answer("🔑 Please enter your access key:")

@dp.message(Command("help"))
async def help_handler(m: Message):
    await m.answer(
        "🤖 *CrocBrains Meme Scanner*\n"
        "The meme that thinks for you.\n\n"
        "• /scan — latest Solana pairs (Birdeye only)\n"
        "• /token <mint> — price, MC, liquidity, volume, exchanges\n"
        "• Holders & LP Lock appear automatically after data plan upgrade\n"
        "• On-chain: Mint/Freeze authority, Top-10 holders (Helius)\n"
        "• /my, /logout — manage access",
        parse_mode="Markdown"
    )

@dp.message(Command("my"))
async def my_handler(m: Message):
    key = get_user_key(m.from_user.id)
    if not key:
        await m.answer("⛔ No active access. Send your key or use /start.")
        return
    ok, msg = is_key_valid_for_product(key)
    status = "✅ Active" if ok else "⛔ Inactive"
    await m.answer(f"Your key: `{key}`\nStatus: {status}\n{msg}", parse_mode="Markdown")

@dp.message(Command("logout"))
async def logout_handler(m: Message):
    conn = db()
    conn.execute("DELETE FROM user_access WHERE user_id = ?", (m.from_user.id,))
    conn.commit()
    conn.close()
    await m.answer("🔒 Key unlinked. Send a new key or /start.")

@dp.message(Command("scan"))
async def scan_handler(m: Message):
    # проверка ключа
    key = get_user_key(m.from_user.id)
    if not key:
        await m.answer("⛔ No active access. Send your key or use /start.")
        return

    # проверка кулдауна
    conn = db()
    cur = conn.execute("SELECT last_scan_ts FROM user_throttle WHERE user_id=?", (m.from_user.id,))
    row = cur.fetchone()
    now = int(time.time())
    if row:
        last_ts = row[0]
        if now - last_ts < SCAN_COOLDOWN_SEC:
            await m.answer("⏳ Please wait before next /scan.")
            return
    conn.execute("INSERT OR REPLACE INTO user_throttle VALUES (?, ?)", (m.from_user.id, now))
    conn.commit()

    # создаём сессию и тянем пары
    await m.answer("🔎 Scanning new memes on Solana...")
    async with aiohttp.ClientSession() as session:
        pairs = await fetch_latest_sol_pairs(limit=5)
        if not pairs:
            await m.answer("⚠️ No data from Birdeye API.")
            return
        for p in pairs:
            txt, kb = await format_token_card(session, p["baseToken"]["address"], summary=True)
            await m.answer(txt, reply_markup=kb)


# ======= SHARED RENDER =======
async def send_token_card(chat_id: int, mint: str):
    extra = None
    mkts  = None
    async with aiohttp.ClientSession() as session:
        # Birdeye (soft)
        if BIRDEYE_API_KEY:
            try:
                extra = await birdeye_overview(session, mint)
            except Exception:
                extra = None
            try:
                mkts = await birdeye_markets(session, mint)
            except Exception:
                mkts = None

        # Build pseudo-pair for card reuse
        p = {
            "baseToken": {"symbol": (extra or {}).get("symbol") or "", "name": (extra or {}).get("name") or "", "address": mint},
            "priceUsd": (extra or {}).get("price"),
            "liquidity": {"usd": (extra or {}).get("liquidity")},
            "fdv": (extra or {}).get("marketCap"),
            "volume": {"h24": (extra or {}).get("v24")},
            "pairCreatedAt": (extra or {}).get("createdAt") or (extra or {}).get("firstTradeAt"),
            "chainId": "solana",
        }

        # Jupiter price fallback
        if p.get("priceUsd") is None:
            jp = await jupiter_price(session, mint)
            if jp is not None:
                p["priceUsd"] = jp

    # Helius on-chain add-ons
    helius_info = None
    topk_share = None
    if HELIUS_RPC_URL:
        async with aiohttp.ClientSession() as hs:
            try:
                helius_info = await helius_get_mint_info(hs, mint)
            except Exception:
                helius_info = None
            try:
                topk_share  = await helius_top_holders_share(hs, mint, k=10)
            except Exception:
                topk_share = None

    add_lines = []
    mint_active = False
    freeze_active = False
    if helius_info:
        mint_txt = format_authority(helius_info.get('mintAuthority'))
        freeze_txt = format_authority(helius_info.get('freezeAuthority'))
        mint_active = (helius_info.get('mintAuthority') is not None)
        freeze_active = (helius_info.get('freezeAuthority') is not None)
        add_lines.append(f"Mint authority: {mint_txt}")
        add_lines.append(f"Freeze authority: {freeze_txt}")
    else:
        add_lines.append("Mint authority: —")
        add_lines.append("Freeze authority: —")
    add_lines.append(f"Top-10 holders: {format_topk_share(topk_share)}")

    # Risk flags (on-chain + concentration)
    flags = risk_flags(mint_active, freeze_active, topk_share)

    # Plan hint if Birdeye overview absent
    plan_hint = ""
    if not extra:
        plan_hint = "\n_Birdeye plan: basic — detailed stats hidden_"

    text = token_card(p, extra, extra_flags=flags) + "\n" + "\n".join(add_lines) + plan_hint
    ex_block = exchanges_block(mkts)
    kb = token_keyboard(p)

    await bot.send_message(chat_id, text + "\n\n" + ex_block, reply_markup=kb, disable_web_page_preview=True)

# ======= HANDLERS =======
@dp.message(Command("scan"))
async def scan_handler(m: Message):
    # стартовое сообщение (потом отредактируем)
    msg = await m.answer("🔍 Scanning Solana pairs… (0/8)")

    # получаем свежие пары с Birdeye
    try:
        pairs = await fetch_latest_sol_pairs(limit=8)
    except Exception:
        pairs = []

    if not pairs:
        await bot.edit_message_text(
            chat_id=msg.chat.id,
            message_id=msg.message_id,
            text="😕 No fresh pairs available via Birdeye on the current plan.",
        )
        return

    # создаём сессию
    _scan_gc()
    sid = _scan_sid()
    _scan_sessions[sid] = {"ts": time.time(), "pairs": pairs}

    # показываем первую карточку (idx=0)
    idx = 0
    p = pairs[idx]
    mint = (p.get("baseToken") or {}).get("address", "")

    extra = None
    mkts = None
    async with aiohttp.ClientSession() as session:
        if BIRDEYE_API_KEY:
            try:
                extra = await birdeye_overview(session, mint)
            except Exception:
                extra = None
            try:
                mkts = await birdeye_markets(session, mint)
            except Exception:
                mkts = None

    # p_view в формате token_card (как в send_token_card/build_summary_text)
    p_view = {
        "baseToken": p.get("baseToken") or {},
        "priceUsd": p.get("priceUsd"),
        "liquidity": p.get("liquidity"),
        "fdv": p.get("fdv"),
        "volume": p.get("volume"),
        "pairCreatedAt": p.get("pairCreatedAt"),
        "chainId": "solana",
    }
    text = token_card(p_view, extra, extra_flags=None) + "\n\n" + exchanges_block(mkts)
    kb = scan_keyboard(sid, idx, len(pairs), mint)

    # редактируем стартовое сообщение в карточку с пагинацией
    await bot.edit_message_text(
        chat_id=msg.chat.id,
        message_id=msg.message_id,
        text=text,
        reply_markup=kb,
        disable_web_page_preview=True,
    )

# === UI-02A: text builders for summary/details toggle ===
async def build_summary_text(mint: str) -> (str, Dict[str, Any]):
    """
    Возвращает (text, p) для summary-режима.
    Сейчас повторяет логику send_token_card: карточка + on-chain блок + биржи.
    """
    extra = None
    mkts = None
    async with aiohttp.ClientSession() as session:
        # Birdeye (мягко)
        if BIRDEYE_API_KEY:
            try:
                extra = await birdeye_overview(session, mint)
            except Exception:
                extra = None
            try:
                mkts = await birdeye_markets(session, mint)
            except Exception:
                mkts = None

        # Собираем псевдо-pair (как в send_token_card)
        p = {
            "baseToken": {
                "symbol": (extra or {}).get("symbol") or "",
                "name":   (extra or {}).get("name") or "",
                "address": mint
            },
            "priceUsd": (extra or {}).get("price"),
            "liquidity": {"usd": (extra or {}).get("liquidity")},
            "fdv": (extra or {}).get("marketCap"),
            "volume": {"h24": (extra or {}).get("v24")},
            "pairCreatedAt": (extra or {}).get("createdAt") or (extra or {}).get("firstTradeAt"),
            "chainId": "solana",
        }

        # Jupiter fallback цены
        if p.get("priceUsd") is None:
            try:
                jp = await jupiter_price(session, mint)
                if jp is not None:
                    p["priceUsd"] = jp
            except Exception:
                pass

    # Helius: on-chain
    helius_info = None
    topk_share = None
    if HELIUS_RPC_URL:
        async with aiohttp.ClientSession() as hs:
            try:
                helius_info = await helius_get_mint_info(hs, mint)
            except Exception:
                helius_info = None
            try:
                topk_share = await helius_top_holders_share(hs, mint, k=10)
            except Exception:
                topk_share = None

    add_lines = []
    mint_active = False
    freeze_active = False
    if helius_info:
        mint_txt = format_authority(helius_info.get('mintAuthority'))
        freeze_txt = format_authority(helius_info.get('freezeAuthority'))
        mint_active = (helius_info.get('mintAuthority') is not None)
        freeze_active = (helius_info.get('freezeAuthority') is not None)
        add_lines.append(f"Mint authority: {mint_txt}")
        add_lines.append(f"Freeze authority: {freeze_txt}")
    else:
        add_lines.append("Mint authority: —")
        add_lines.append("Freeze authority: —")

    if topk_share is not None:
        add_lines.append(f"Top-10 concentration: {format_topk_share(topk_share)}")

    # Риски
    flags = risk_flags(mint_active, freeze_active, topk_share)

    # Подсказка тарифа, если overview отсутствует
    plan_hint = ""
    if not extra:
        plan_hint = "\n_Birdeye plan: basic — detailed stats hidden_"

    text_main = token_card(p, extra, extra_flags=flags) + "\n" + "\n".join(add_lines) + plan_hint
    ex_block = exchanges_block(mkts)
    full_text = text_main + "\n\n" + ex_block
    return full_text, p


async def build_details_text(mint: str) -> (str, Dict[str, Any]):
    """
    UI-02B: расширенный details.
    Источники:
      - Birdeye overview/markets (если есть ключ и ответ)
      - Helius mint info + top holders
      - Jupiter Price как фоллбек цены
    Безопасная деградация: при отсутствии данных показываем "—" и/или подсказку тарифа.
    """
    # Локальные форматтеры с фоллбеком (если глобальные хелперы не объявлены)
    def _fmt_usd(x):
        try:
            return format_usd(x)  # type: ignore
        except Exception:
            if x is None:
                return "—"
            try:
                return f"${float(x):,.4f}"
            except Exception:
                return str(x)

    def _fmt_int(x):
        try:
            return format_int(x)  # type: ignore
        except Exception:
            if x is None:
                return "—"
            try:
                return f"{int(float(x)):,}"
            except Exception:
                return str(x)

    def _fmt_pct(x):
        try:
            return format_pct(x)  # type: ignore
        except Exception:
            if x is None:
                return "—"
            try:
                return f"{float(x)*100:.2f}%"
            except Exception:
                return str(x)

    def _fmt_age(ts_ms):
        try:
            return format_age(ts_ms)  # type: ignore
        except Exception:
            if not ts_ms:
                return "—"
            try:
                import time
                now = int(time.time() * 1000)
                dt = max(0, (now - int(ts_ms)) // 1000)
                days = dt // 86400
                if days > 0:
                    return f"{days}d"
                hours = (dt % 86400) // 3600
                mins = (dt % 3600) // 60
                if hours:
                    return f"{hours}h {mins}m"
                return f"{mins}m"
            except Exception:
                return "—"

    def _fmt_auth(a):
        try:
            return format_authority(a)  # type: ignore
        except Exception:
            if a is None:
                return "—"
            if isinstance(a, dict):
                v = a.get("address") or a.get("pubkey") or a.get("value")
                return v or "—"
            return str(a)

    def _fmt_topk(x):
        try:
            return format_topk_share(x)  # type: ignore
        except Exception:
            if x is None:
                return "—"
            try:
                return f"{float(x)*100:.2f}%"
            except Exception:
                return str(x)

    # ---- fetch data
    extra = None   # birdeye overview dict
    mkts = None    # birdeye markets list
    async with aiohttp.ClientSession() as session:
        if BIRDEYE_API_KEY:
            try:
                extra = await birdeye_overview(session, mint)
            except Exception:
                extra = None
            try:
                mkts = await birdeye_markets(session, mint)
            except Exception:
                mkts = None

        # Восстановим p-подобную структуру (как в summary) для клавиатуры/карточки
        p = {
            "baseToken": {
                "symbol": (extra or {}).get("symbol") or "",
                "name":   (extra or {}).get("name") or "",
                "address": mint
            },
            "priceUsd": (extra or {}).get("price"),
            "liquidity": {"usd": (extra or {}).get("liquidity")},
            "fdv": (extra or {}).get("marketCap"),
            "volume": {"h24": (extra or {}).get("v24")},
            "pairCreatedAt": (extra or {}).get("createdAt") or (extra or {}).get("firstTradeAt"),
            "chainId": "solana",
        }

        # Фоллбек цены с Jupiter
        if p.get("priceUsd") is None:
            try:
                jp = await jupiter_price(session, mint)
                if jp is not None:
                    p["priceUsd"] = jp
            except Exception:
                pass

    # ---- on-chain (Helius)
    helius_info = None
    topk_share = None
    if HELIUS_RPC_URL:
        async with aiohttp.ClientSession() as hs:
            try:
                helius_info = await helius_get_mint_info(hs, mint)
            except Exception:
                helius_info = None
            try:
                topk_share = await helius_top_holders_share(hs, mint, k=10)
            except Exception:
                topk_share = None

    # ---- warnings (risk flags)
    mint_active = bool(helius_info and helius_info.get("mintAuthority") is not None)
    freeze_active = bool(helius_info and helius_info.get("freezeAuthority") is not None)
    try:
        flags = risk_flags(mint_active, freeze_active, topk_share)  # type: ignore
    except Exception:
        flags = []

    # === Compose DETAILS text ===
    # 1) Шапка (как summary-стиль, но оставляем лаконичной)
    symbol = (extra or {}).get("symbol") or p["baseToken"]["symbol"]
    name = (extra or {}).get("name") or p["baseToken"]["name"]
    header = f"🐊 *{symbol or '—'}* — _{name or '—'}_"

    # 2) Основные метрики (вынесем в таблицу)
    price_txt = _fmt_usd(p.get("priceUsd"))
    fdv_txt = _fmt_usd((extra or {}).get("marketCap"))
    liq_txt = _fmt_usd((extra or {}).get("liquidity"))
    vol24_txt = _fmt_usd((extra or {}).get("v24"))
    age_txt = _fmt_age(p.get("pairCreatedAt"))

    # Дополнительные поля из Birdeye overview, если есть
    # Популярные ключи (будут пропущены, если None/нет): supply, totalSupply, circulatingSupply, tx24, holders
    def _get_any(d, keys):
        for k in keys:
            if d and d.get(k) not in (None, "", 0):
                return d.get(k)
        return None

    supply_val = _get_any(extra or {}, ["supply", "circulatingSupply", "totalSupply"])
    holders_val = _get_any(extra or {}, ["holders", "holdersCount", "holders_count"])
    tx24_val = _get_any(extra or {}, ["tx24", "transactions24h", "txn24"])

    supply_txt = _fmt_int(supply_val)
    holders_txt = _fmt_int(holders_val)
    tx24_txt = _fmt_int(tx24_val)

    main_block = (
        f"\n"
        f"💰 Price: {price_txt}\n"
        f"📊 Market Cap / FDV: {fdv_txt}\n"
        f"💦 Liquidity: {liq_txt}\n"
        f"📈 24h Volume: {vol24_txt}\n"
        f"⏳ Age: {age_txt}\n"
        f"👥 Holders: {holders_txt}\n"
        f"🔁 24h TX: {tx24_txt}"
    )

    # 3) On-chain блок (Helius)
    decimals_txt = "—"
    supply_onchain_txt = "—"
    if helius_info:
        decimals_txt = str(helius_info.get("decimals")) if helius_info.get("decimals") is not None else "—"
        # Пытаемся взять supply (может быть в helius_info['supply'] или аналоге)
        _s_on = helius_info.get("supply") if isinstance(helius_info, dict) else None
        supply_onchain_txt = _fmt_int(_s_on)

    onchain_block = (
        f"\n\n*On-chain*\n"
        f"• Mint authority: {_fmt_auth(helius_info.get('mintAuthority') if helius_info else None)}\n"
        f"• Freeze authority: {_fmt_auth(helius_info.get('freezeAuthority') if helius_info else None)}\n"
        f"• Decimals: {decimals_txt}\n"
        f"• Supply (on-chain): {supply_onchain_txt}\n"
        f"• Top-10 concentration: {_fmt_topk(topk_share)}"
    )

    # 4) Топ-2 DEX из Birdeye markets
    # Сортируем сначала по ликвидности, если нет — по 24h объёму
    top2_lines = []
    if isinstance(mkts, list) and mkts:
        def _liq(m):
            for k in ("liquidityUSD", "liquidityUsd", "liquidity", "liquidity_usd"):
                v = m.get(k)
                if v not in (None, ""):
                    return float(v)
            return 0.0

        def _v24(m):
            for k in ("v24Usd", "volume24hUsd", "volume24h", "v24"):
                v = m.get(k)
                if v not in (None, ""):
                    return float(v)
            return 0.0

        sorted_mkts = sorted(mkts, key=lambda m: (_liq(m), _v24(m)), reverse=True)
        top2 = sorted_mkts[:2]
        for m in top2:
            name = m.get("dex") or m.get("market") or m.get("name") or "DEX"
            liq_show = _fmt_usd(_liq(m))
            v24_show = _fmt_usd(_v24(m))
            base_sym = (extra or {}).get("symbol") or p["baseToken"]["symbol"] or ""
            quote_sym = m.get("quoteSymbol") or m.get("quote") or ""
            pair = f"{base_sym}/{quote_sym}" if quote_sym else base_sym or "—"
            top2_lines.append(f"• {name}: {pair} — L:{liq_show}, V24:{v24_show}")

    dex_block = ""
    if top2_lines:
        dex_block = "\n\n*DEX (top-2)*\n" + "\n".join(top2_lines)

    # 5) Предупреждения
    warn_block = ""
    if flags:
        # flags — список строк; выводим с ⚠️
        warn_lines = [f"⚠️ {x}" for x in flags if x]
        if warn_lines:
            warn_block = "\n\n" + "\n".join(warn_lines)

    # 6) Подсказка тарифа, если overview недоступен
    plan_hint = ""
    if not extra:
        plan_hint = "\n\n_Birdeye plan: basic — detailed stats may be limited_"

    text = header + main_block + onchain_block + dex_block + warn_block + plan_hint

    # Возвращаем также p для клавиатуры
    return text, p



@dp.message(Command("scan"))
async def scan_handler(m: Message):
    key = get_user_key(m.from_user.id)
    if not key:
        await m.answer("⛔ No access. Please enter your key via /start.")
        return
    ok, msg = is_key_valid_for_product(key)
    if not ok:
        await m.answer(f"⛔ Access invalid: {msg}\nSend a new key.")
        return

    # Per-user cooldown
    now_ts = int(time.time())
    last_ts = get_last_scan_ts(m.from_user.id)
    remaining = SCAN_COOLDOWN_SEC - (now_ts - last_ts)
    if remaining > 0:
        await m.answer(f"⏳ Please wait {remaining}s before using /scan again (anti-spam).")
        return
    set_last_scan_ts(m.from_user.id, now_ts)

    progress_msg = await m.answer("🔎 Scanning Solana pairs (Birdeye)…")
    pairs = await fetch_latest_sol_pairs(limit=8)
    if not pairs:
        await progress_msg.edit_text(
            "😕 No fresh pairs available via Birdeye on the current plan.\n"
            "• Try `/token <mint>` to view a specific coin\n"
            "• Or upgrade data plan to enable full auto-scan",
            parse_mode="Markdown"
        )
        return

    async with aiohttp.ClientSession() as session:
        total = len(pairs)
        for idx, p in enumerate(pairs, start=1):
            mint = (p.get("baseToken") or {}).get("address", "")
            extra = None
            if BIRDEYE_API_KEY and mint:
                extra = await birdeye_overview(session, mint)

            if (p.get("priceUsd") is None) and mint:
                jp = await jupiter_price(session, mint)
                if jp is not None:
                    p["priceUsd"] = jp

            text = token_card(p, extra, extra_flags=None)
            kb = token_keyboard(p)
            try:
                await m.answer(text, reply_markup=kb, disable_web_page_preview=True)
            except Exception:
                await m.answer(text, disable_web_page_preview=True)

            try:
                await progress_msg.edit_text(f"🔎 Scanning Solana pairs (Birdeye)… ({idx}/{total})")
            except Exception:
                pass

        try:
            await progress_msg.edit_text("✅ Scan complete.")
        except Exception:
            pass

@dp.message(Command("token"))
async def token_handler(m: Message):
    key = get_user_key(m.from_user.id)
    if not key:
        await m.answer("⛔ No access. Please enter your key via /start.")
        return
    ok, msg = is_key_valid_for_product(key)
    if not ok:
        await m.answer(f"⛔ Access invalid: {msg}\nSend a new key.")
        return

    args = (m.text or "").split(maxsplit=1)
    if len(args) < 2:
        await m.answer("Usage: `/token <mint | birdeye/solscan link | SYMBOL (MINT)>`", parse_mode="Markdown")
        return

    raw_arg = args[1]
    mint = normalize_mint_arg(raw_arg)
    if not mint:
        await m.answer("❌ Can't detect mint address. Send a Solana mint or a direct link to Birdeye/Solscan.")
        return

    await m.answer(f"🔎 Fetching token data…\n`{mint}`", parse_mode="Markdown")
    await send_token_card(m.chat.id, mint)

# NEW: callback handler for “ℹ️ Details”
@dp.callback_query(F.data.startswith("token:"))
async def token_callback_handler(c: CallbackQuery):
        # 0) Сразу закрываем "часики" чтобы избежать таймаута callback
    try:
        await c.answer()  # без текста, мгновенно
    except Exception:
        pass  # если вдруг уже поздно — просто игнорируем

    # 1) Доступ
    key = get_user_key(c.from_user.id)
    if not key:
        # алерт можно показать отдельно, но если callback уже старый — не критично
        try:
            await c.answer("No access. Use /start", show_alert=True)
        except Exception:
            pass
        return
    ok, _ = is_key_valid_for_product(key)
    if not ok:
        try:
            await c.answer("Access invalid", show_alert=True)
        except Exception:
            pass
        return

@dp.callback_query(F.data.startswith("scan:"))
async def scan_cb_handler(c: CallbackQuery):
    try:
        await c.answer()
    except Exception:
        pass

    parts = (c.data or "").split(":")
    # формат: scan:session:<sid>:idx:<i>  ИЛИ  scan:session:<sid>:detail:<i>
    if len(parts) < 5:
        return
    _, _, sid, action, idx_s = parts
    if sid not in _scan_sessions:
        try:
            await c.answer("Session expired", show_alert=True)
        except Exception:
            pass
        return
    try:
        idx = int(idx_s)
    except Exception:
        return

    sess = _scan_sessions[sid]
    pairs = sess.get("pairs") or []
    if not pairs:
        return
    total = len(pairs)
    if not (0 <= idx < total):
        idx = 0

    p = pairs[idx]
    mint = (p.get("baseToken") or {}).get("address", "")

    if action == "idx":
        extra = None; mkts = None
        async with aiohttp.ClientSession() as s:
            if BIRDEYE_API_KEY:
                try: extra = await birdeye_overview(s, mint)
                except Exception: extra = None
                try: mkts = await birdeye_markets(s, mint)
                except Exception: mkts = None
        p_view = {
            "baseToken": p.get("baseToken") or {},
            "priceUsd": p.get("priceUsd"),
            "liquidity": p.get("liquidity"),
            "fdv": p.get("fdv"),
            "volume": p.get("volume"),
            "pairCreatedAt": p.get("pairCreatedAt"),
            "chainId": "solana",
        }
        text = token_card(p_view, extra, extra_flags=None) + "\n\n" + exchanges_block(mkts)
    else:  # "detail"
        text, _ = await build_details_text(mint)

    kb = scan_keyboard(sid, idx, total, mint)
    if not c.message:
        return
    try:
        await bot.edit_message_text(
            chat_id=c.message.chat.id,
            message_id=c.message.message_id,
            text=text,
            reply_markup=kb,
            disable_web_page_preview=True,
        )
    except Exception:
        pass

@dp.callback_query(F.data == "noop")
async def noop_cb(c: CallbackQuery):
    try:
        await c.answer()
    except Exception:
        pass


    # 2) Разбор данных
    parts = (c.data or "").split(":")
    if len(parts) < 2:
        try:
            await c.answer("Mint missing", show_alert=True)
        except Exception:
            pass
        return

    mint = parts[1].strip()
    mode = parts[2].strip() if len(parts) >= 3 else "details"

    # 3) Сборка текста + правка сообщения
    try:
        if mode == "details":
            text, p = await build_details_text(mint)
            kb = token_keyboard(p, mode="details")
        else:
            text, p = await build_summary_text(mint)
            kb = token_keyboard(p, mode="summary")

        try:
            await bot.edit_message_text(
                chat_id=c.message.chat.id,
                message_id=c.message.message_id,
                text=text,
                reply_markup=kb,
                disable_web_page_preview=True,
            )
        except Exception as e:
            # если "message is not modified" или что-то похожее — просто игнорируем
            from aiogram.exceptions import TelegramBadRequest
            if isinstance(e, TelegramBadRequest) and "message is not modified" in str(e).lower():
                pass
            else:
                raise
    except Exception:
        # финальная попытка сообщить об ошибке (может быть уже поздно — обернули)
        try:
            await c.answer("Failed to update message", show_alert=True)
        except Exception:
            pass



@dp.message(F.text)
async def key_input_handler(m: Message):
    if get_user_key(m.from_user.id):
        return
    candidate = (m.text or "").strip()
    ok, msg = is_key_valid_for_product(candidate)
    if ok:
        bind_user(m.from_user.id, candidate)
        await m.answer(f"✅ Key accepted. {msg}\nYou can now use /scan")
    else:
        await m.answer(f"⛔ {msg}\nPlease try again.")

async def main():
    seed_initial_keys()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())