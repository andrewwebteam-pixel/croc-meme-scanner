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

# === User-facing strings (UI-04) ===
STR = {
    "no_access": "⛔ No access. Please enter your key via /start.",
    "access_invalid": "⛔ Access invalid: {msg}\nSend a new key.",
    "cooldown": "⏳ Please wait {remaining}s before using /scan again (anti-spam).",
    "no_pairs": (
        "😕 No fresh pairs available via Birdeye on the current plan.\n"
        "Try `/token <mint>` or upgrade your data plan."
    ),
    "scan_progress": "🔍 Scanning Solana pairs… ({i}/{n})",
    "start": "Welcome to the {product} bot! Use /help to see commands.",
    "help": (
        "Commands:\n"
        "/token <mint> — get details on a token\n"
        "/scan — scan fresh pairs\n"
        "/my — show your subscription status\n"
        "/logout — remove your key\n"
        "/help — show this help"
    ),
    "logged_out": "✅ Your key has been removed. Goodbye!",
    "no_key": "You have no key saved. Use /start to enter a key.",
    "key_saved": "✅ Access key saved.",
    "key_invalid": "⛔ Invalid key.",
    "token_not_found": "⛔ Token not found. Please try again.",
    "bad_callback": "Bad callback.",
    "session_expired": "Session expired. Please run /scan again.",
    # Добавленные ключи:
    "enter_key": "Please enter your access key:",
    "no_active_access": "⛔ No active access. Send your key or use /start.",
    "key_unlinked": "✅ Key unlinked. Send a new key or /start.",
    "usage_token": "Usage: `/token <mint | birdeye/solscan link | SYMBOL (MINT)>`",
    "cant_detect_mint": "❌ Can't detect mint address. Send a Solana mint or a direct link to Birdeye/Solscan.",
    "fetching_data": "Fetching token data…\n`{mint}`",
    "no_data": "No data",
}

# === Simple in-memory cache for /scan results ===
SCAN_CACHE_TTL = 15  # seconds
_scan_cache: Dict[str, Any] = {"ts": 0.0, "pairs": []}


# === /scan pagination sessions ===
SCAN_SESSION_TTL = 300  # время жизни сессии в секундах
_scan_cache_sessions: Dict[str, Dict[str, Any]] = {}  # sid -> {"ts": float, "pairs": List[dict]}

def _new_sid() -> str:
    return str(int(time.time()*1000)) + "-" + os.urandom(3).hex()

def _cleanup_scan_sessions():
    now = time.time()
    for k in list(_scan_cache_sessions.keys()):
        if _scan_cache_sessions[k].get("ts", 0) + SCAN_SESSION_TTL < now:
            _scan_cache_sessions.pop(k, None)

def scan_nav_kb(sid: str, idx: int, mint: str, mode: str = "summary") -> InlineKeyboardMarkup:
    prev_idx = max(idx - 1, 0)
    next_idx = idx + 1
    row_nav = [
        InlineKeyboardButton(text="◀ Prev", callback_data=f"scan:session:{sid}:idx:{prev_idx}"),
        InlineKeyboardButton(text="▶ Next", callback_data=f"scan:session:{sid}:idx:{next_idx}"),
    ]
    row_toggle = (
        [InlineKeyboardButton(text="ℹ️ Details", callback_data=f"scan:session:{sid}:detail:{idx}")]
        if mode == "summary"
        else [InlineKeyboardButton(text="◀ Back", callback_data=f"scan:session:{sid}:idx:{idx}")]
    )
    be_link = f"https://birdeye.so/token/{mint}?chain=solana"
    solscan_link = f"https://solscan.io/token/{mint}"
    row_links1 = [InlineKeyboardButton(text="Open on Birdeye", url=be_link)]
    row_links2 = [InlineKeyboardButton(text="Open on Solscan", url=solscan_link)]
    return InlineKeyboardMarkup(inline_keyboard=[row_nav, row_toggle, row_links1, row_links2])



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
    # cache hit
    if (_scan_cache["ts"] + SCAN_CACHE_TTL) > time.time() and _scan_cache["pairs"]:
        return _scan_cache["pairs"][:limit]

    if not BIRDEYE_API_KEY:
        print("[SCAN] Birdeye: BIRDEYE_API_KEY is empty -> returning []")
        return []

    url = f"{BIRDEYE_BASE}/defi/markets"
    headers = {"accept": "application/json", "X-API-KEY": BIRDEYE_API_KEY}
    params = {"chain": "solana", "sort_by": "liquidity", "sort_type": "desc", "offset": 0, "limit": 50}
    try:
        await api_rate_limit()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
            async with s.get(url, headers=headers, params=params) as r:
                if r.status != 200:
                    try:
                        txt = await r.text()
                    except Exception:
                        txt = "<no body>"
                    print(f"[SCAN] /defi/markets HTTP {r.status} -> {txt[:300]}")
                    return []
                j = await r.json()
                if not j or not j.get("success"):
                    print(f"[SCAN] /defi/markets success==false or empty: {str(j)[:300]}")
                    return []
                data = j.get("data") or []
                if not isinstance(data, list) or not data:
                    print("[SCAN] /defi/markets returned empty 'data'")
                    return []

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
                    except Exception as e:
                        print(f"[SCAN] pair build error: {e}")
                        continue

                _scan_cache["ts"] = time.time()
                _scan_cache["pairs"] = pairs
                return pairs[:limit]
    except Exception as e:
        print(f"[SCAN] Birdeye fetch exception: {e}")
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
    # Базовые метрики
    if liq_usd is not None and liq_usd < 10_000:
        risk.append("Low liquidity")
    if vol24 is not None and vol24 < 5_000:
        risk.append("Low volume")

    # Дополнительные (UI-02B расширение)
    # 1) LP lock
    if lp_lock is not None:
        try:
            if float(lp_lock) < 20.0:
                risk.append("Low LP lock (<20%)")
        except Exception:
            pass

    # 2) Возраст токена
    # age_dt уже вычислен выше; считаем часы и помечаем совсем свежие
    if age_dt:
        try:
            hrs = int((datetime.now(tz=timezone.utc) - age_dt).total_seconds() // 3600)
            if hrs < 6:
                risk.append("New token (<6h)")
        except Exception:
            pass

    # Рисковые флаги из details (Mint/Freeze/Top-10)
    if extra_flags:
        risk.extend(extra_flags)


    lines = [
        f"🐊 *${symbol}* — {name}",
        f"Price: {price_txt}",
        f"Liquidity: {format_usd(liq_usd)}",
        f"FDV/MC: {format_usd(fdv)}",
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

# === Text builders for summary/details ===
def build_summary_text(p: Dict[str, Any], extra: Optional[Dict[str, Any]], mkts: Optional[List[Dict[str, Any]]]) -> str:
    # Короткая карточка без ончейн-блока и без предупреждений
    return token_card(p, extra, extra_flags=None)

# Отформатировать блок "все доступные поля" из Birdeye (плоские ключи)
def birdeye_kv_block(extra: Optional[Dict[str, Any]]) -> str:
    if not extra or not isinstance(extra, dict):
        return "Birdeye: —"

    # Приоритетные ключи выводим первыми
    preferred = [
        "symbol", "name", "price", "marketCap", "liquidity", "v24",
        "createdAt", "firstTradeAt", "holders", "lp_lock_ratio"
    ]
    simple_items: List[tuple[str, str]] = []
    used = set()

    def _fmt_val(k: str, v: Any) -> str:
        try:
            if v is None:
                return "—"
            if k in ("price", "marketCap", "liquidity", "v24"):
                return format_usd(float(v))
            if isinstance(v, bool):
                return "yes" if v else "no"
            if isinstance(v, (int, float)):
                return f"{v}"
            return str(v)
        except Exception:
            return str(v)

    # 1) Приоритетные
    for k in preferred:
        if k in extra:
            v = extra.get(k)
            if isinstance(v, (dict, list)):
                continue
            simple_items.append((k, _fmt_val(k, v)))
            used.add(k)

    # 2) Прочие простые ключи (в алфавитном порядке для детерминизма)
    for k in sorted(extra.keys()):
        if k in used:
            continue
        v = extra.get(k)
        if isinstance(v, (dict, list)):
            continue
        simple_items.append((k, _fmt_val(k, v)))

    # Финальный текст (имя ключа в бэктиках — безопасно для Markdown)
    lines = ["Birdeye:"]
    for k, v in simple_items:
        lines.append(f"- `{k}`: {v}")
    return "\n".join(lines)


def build_details_text(
    p: Dict[str, Any],
    extra: Optional[Dict[str, Any]],
    mkts: Optional[List[Dict[str, Any]]],
    helius_info: Optional[Dict[str, Any]],
    topk_share: Optional[float]
) -> str:
    # Локальные форматтеры
    def f_pct(v: Optional[float]) -> str:
        try:
            if v is None:
                return "—"
            return f"{float(v):.2f}%"
        except Exception:
            return "—"

    # Ончейн-блок (mint/freeze)
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

    # Топ-10 концентрация
    add_lines.append(f"Top-10 holders: {f_pct(topk_share)}")

    # Флаги риска
    flags = risk_flags(mint_active, freeze_active, topk_share)

    # Подсказка по плану, если нет Birdeye overview
    plan_hint = ""
    if not extra:
        plan_hint = "\n_Birdeye plan: basic — detailed stats hidden_"

    # Детализированный блок Birdeye (все доступные поля, плоский вывод)
    be_block = birdeye_kv_block(extra)

    # DEX (top-2) — как и раньше
    ex_block = exchanges_block(mkts)

    # Базовая карточка + флаги риска
    core = token_card(p, extra, extra_flags=flags)

    # Склейка финального текста (ядро + ончейн + подсказка + birdeye + DEX)
    parts = [
        core,
        "\n".join(add_lines),
        plan_hint.strip(),
        be_block,
        ex_block
    ]
    # Удаляем пустые элементы и объединяем
    parts = [x for x in parts if x and x.strip()]
    return "\n\n".join(parts)



def token_keyboard(p: Dict[str, Any], mode: str = "summary") -> InlineKeyboardMarkup:
    base = p.get("baseToken", {}) or {}
    mint = base.get("address", "")
    be_link = f"https://birdeye.so/token/{mint}?chain=solana" if mint else "https://birdeye.so/solana"
    solscan_link = f"https://solscan.io/token/{mint}" if mint else "https://solscan.io"

    # Тоггл-ряд
    if mode == "summary":
        first_row = [InlineKeyboardButton(text="ℹ️ Details", callback_data=f"token:{mint}:details")]
    else:
        first_row = [InlineKeyboardButton(text="◀ Back", callback_data=f"token:{mint}:summary")]

    return InlineKeyboardMarkup(inline_keyboard=[
        first_row,
        [InlineKeyboardButton(text="Open on Birdeye", url=be_link)],
        [InlineKeyboardButton(text="Open on Solscan", url=solscan_link)],
    ])


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
        await m.answer(STR["start"].format(product=PRODUCT))
    else:
        await m.answer(STR["enter_key"])

@dp.message(Command("help"))
async def help_handler(m: Message):
    await m.answer(STR["help"])

@dp.message(Command("my"))
async def my_handler(m: Message):
    key = get_user_key(m.from_user.id)
    if not key:
    await m.answer(STR["no_active_access"])
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
    await m.answer(STR["key_unlinked"])

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

        # Jupiter price fallback (if Birdeye has no price)
        if p.get("priceUsd") is None:
            jp = await jupiter_price(session, mint)
            if jp is not None:
                p["priceUsd"] = jp

    # SUMMARY only (no on-chain and no exchanges block here)
    text = build_summary_text(p, extra, mkts)
    kb = token_keyboard(p, mode="summary")
    await bot.send_message(chat_id, text, reply_markup=kb, disable_web_page_preview=True)

# ======= HANDLERS =======
@dp.message(Command("scan"))
async def scan_handler(m: Message):
    key = get_user_key(m.from_user.id)
    if not key:
        await m.answer(STR["no_access"])
        return
    ok, msg = is_key_valid_for_product(key)
    if not ok:
        await m.answer(STR["access_invalid"].format(msg=msg))
        return

    now_ts = int(time.time())
    last_ts = get_last_scan_ts(m.from_user.id)
    remaining = SCAN_COOLDOWN_SEC - (now_ts - last_ts)
    if remaining > 0:
        await m.answer(STR["cooldown"].format(remaining=remaining))
        return
    set_last_scan_ts(m.from_user.id, now_ts)

    pairs = await fetch_latest_sol_pairs(limit=8)
    if not pairs:
        await m.answer(STR["no_pairs"])
        return

    # Показать прогресс для уже загруженных pairs
    n_pairs = len(pairs)
    progress_msg = await m.answer(STR["scan_progress"].format(i=0, n=n_pairs))
    for i in range(n_pairs):
        await progress_msg.edit_text(STR["scan_progress"].format(i=i+1, n=n_pairs))

    # Теперь пары готовы — создаём сессию и выводим первую карточку
    _cleanup_scan_sessions()
    sid = _new_sid()
    _scan_cache_sessions[sid] = {"ts": time.time(), "pairs": pairs}
    first_idx = 0
    p0 = pairs[first_idx]
    mint0 = (p0.get("baseToken") or {}).get("address", "")
    extra0 = None
    async with aiohttp.ClientSession() as session:
        if BIRDEYE_API_KEY and mint0:
            try:
                extra0 = await birdeye_overview(session, mint0)
            except Exception:
                extra0 = None
        if (p0.get("priceUsd") is None) and mint0:
            try:
                jp = await jupiter_price(session, mint0)
                if jp is not None:
                    p0["priceUsd"] = jp
            except Exception:
                pass

    text0 = token_card(p0, extra0, extra_flags=None)
    kb0 = scan_nav_kb(sid, first_idx, mint0, mode="summary")
    await progress_msg.edit_text(text0, reply_markup=kb0, disable_web_page_preview=True)


@dp.message(Command("token"))
async def token_handler(m: Message):
    key = get_user_key(m.from_user.id)
    if not key:
        await m.answer(STR["no_access"])
        return
    ok, msg = is_key_valid_for_product(key)
    if not ok:
        await m.answer(STR["access_invalid"].format(msg=msg))
        return

    args = (m.text or "").split(maxsplit=1)
    if len(args) < 2:
        await m.answer(STR["usage_token"], parse_mode="Markdown")
        return

    raw_arg = args[1]
    mint = normalize_mint_arg(raw_arg)
    if not mint:
        await m.answer(STR["cant_detect_mint"])
        return

    await m.answer(STR["fetching_data"].format(mint=mint), parse_mode="Markdown"))
    await send_token_card(m.chat.id, mint)

# NEW: callback handler for “ℹ️ Details”
@dp.callback_query(F.data.startswith("token:"))
async def token_cb_handler(cb: CallbackQuery):
    # Ожидаем формат: token:<mint>:<mode>, где <mode> in {"summary","details"}
    try:
        _, mint, mode = cb.data.split(":", 2)
    except ValueError:
        await cb.answer(STR["bad_callback"])
        return

    extra = None
    mkts = None
    helius_info = None
    topk_share = None

    # Загружаем данные (Birdeye soft; Helius только в режиме details)
    async with aiohttp.ClientSession() as session:
        if BIRDEYE_API_KEY and mint:
            try:
                extra = await birdeye_overview(session, mint)
            except Exception:
                extra = None
            try:
                mkts = await birdeye_markets(session, mint)
            except Exception:
                mkts = None

        # Сбор псевдопары для рендера
        p = {
            "baseToken": {
                "symbol": (extra or {}).get("symbol") or "",
                "name": (extra or {}).get("name") or "",
                "address": mint
            },
            "priceUsd": (extra or {}).get("price"),
            "liquidity": {"usd": (extra or {}).get("liquidity")},
            "fdv": (extra or {}).get("marketCap"),
            "volume": {"h24": (extra or {}).get("v24")},
            "pairCreatedAt": (extra or {}).get("createdAt") or (extra or {}).get("firstTradeAt"),
            "chainId": "solana",
        }

        # Фоллбек цены через Jupiter
        if p.get("priceUsd") is None and mint:
            try:
                jp = await jupiter_price(session, mint)
                if jp is not None:
                    p["priceUsd"] = jp
            except Exception:
                pass

        if mode == "details":
            try:
                helius_info = await helius_get_mint_info(session, mint)
            except Exception:
                helius_info = None
            try:
                topk_share = await helius_top_holders_share(session, mint, k=10)
            except Exception:
                topk_share = None

    # Сбор текста и клавиатуры
    try:
        if mode == "details":
            text = build_details_text(p, extra, mkts, helius_info, topk_share)
            kb = token_keyboard(p, mode="details")
        else:
            text = build_summary_text(p, extra, mkts)
            kb = token_keyboard(p, mode="summary")

        await cb.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        # На случай, если текст не изменился или сообщение устарело
        pass

    await cb.answer()


# === /scan pagination & details callback ===
@dp.callback_query(F.data.startswith("scan:session:"))
async def scan_cb_handler(cb: CallbackQuery):
    # Форматы:
    # scan:session:<sid>:idx:<i>
    # scan:session:<sid>:detail:<i>
    try:
        parts = cb.data.split(":")
        # ["scan","session", sid, "idx"|"detail", i]
        sid = parts[2]
        action = parts[3]
        idx = int(parts[4])
    except Exception:
        await cb.answer(STR["bad_callback"])
        return

    _cleanup_scan_sessions()
    sess = _scan_cache_sessions.get(sid)
    if not sess:
        await cb.answer(STR["session_expired"])
        return

    pairs: List[Dict[str, Any]] = sess.get("pairs") or []
    if not pairs:
        await cb.answer(STR["no_data"])
        return

    # Нормализуем индекс
    if idx < 0: idx = 0
    if idx >= len(pairs): idx = len(pairs) - 1

    p = pairs[idx]
    mint = (p.get("baseToken") or {}).get("address", "")

    text = None
    kb = None

    async with aiohttp.ClientSession() as session:
        # Минимальные данные для summary (overview + цена)
        extra = None
        if BIRDEYE_API_KEY and mint:
            try:
                extra = await birdeye_overview(session, mint)
            except Exception:
                extra = None
        if (p.get("priceUsd") is None) and mint:
            try:
                jp = await jupiter_price(session, mint)
                if jp is not None:
                    p["priceUsd"] = jp
            except Exception:
                pass

        if action == "detail":
            # Полные детали (DEX + ончейн + флаги риска)
            mkts = None
            if BIRDEYE_API_KEY and mint:
                try:
                    mkts = await birdeye_markets(session, mint)
                except Exception:
                    mkts = None
            helius_info = None
            topk_share = None
            if HELIUS_RPC_URL and mint:
                try:
                    helius_info, topk_share = await asyncio.gather(
                        helius_get_mint_info(session, mint),
                        helius_top_holders_share(session, mint),
                    )
                except Exception:
                    helius_info, topk_share = None, None
            text = build_details_text(p, extra, mkts, helius_info, topk_share)
            kb = scan_nav_kb(sid, idx, mint, mode="details")
        else:
            # Перелистывание (summary)
            text = build_summary_text(p, extra, mkts=None)
            kb = scan_nav_kb(sid, idx, mint, mode="summary")

    try:
        await cb.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        await cb.message.answer(text, reply_markup=kb, disable_web_page_preview=True)

    await cb.answer()

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
