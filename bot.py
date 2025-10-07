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
DB_PATH = os.getenv("DB_PATH", "./keys.db")
PRODUCT = os.getenv("PRODUCT", "meme_scanner")
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "").strip()
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "").strip()
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "").strip()
HELIUS_RPC_URL = os.getenv("HELIUS_RPC_URL", "").strip() or (f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else "")

SCAN_COOLDOWN_SEC = int(os.getenv("SCAN_COOLDOWN_SEC", "30"))

assert BOT_TOKEN, "BOT_TOKEN is required"

BIRDEYE_BASE = "https://public-api.birdeye.so"

# === User-facing strings (UI-06) ===
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
        "/fav add <mint> — add token to favorites\n"
        "/fav list — show your favorites\n"
        "/my — show your subscription status\n"
        "/logout — remove your key\n"
        "/help — show this help"
    ),
    "logged_out": "✅ Your key has been removed. Goodbye!",
    "no_key": "You have no key saved. Use /start to enter a key.",
    "key_saved": "✅ Access key saved.",
    "key_invalid": "⛔ Invalid key.",
    "token_not_found": "⛔ Token not found. Please try again.",
    "bad_callback": "⚠️ Invalid action.",
    "session_expired": "⌛ Session expired. Run /scan again.",
    "enter_key": "Please enter your access key:",
    "no_active_access": "⛔ No active access. Send your key or use /start.",
    "key_unlinked": "✅ Key unlinked. Send a new key or /start.",
    "usage_token": "Usage: `/token <mint | birdeye/solscan link | SYMBOL (MINT)>`",
    "cant_detect_mint": "❌ Can't detect mint address. Send a Solana mint or a direct link to Birdeye/Solscan.",
    "fetching_data": "Fetching token data…\n`{mint}`",
    "no_data": "No data",
    "fav_usage": "Usage: `/fav add <mint>` or `/fav list`",
    "fav_add_usage": "Usage: `/fav add <mint>`",
    "fav_added": "✅ {mint} added to favorites.",
    "fav_empty": "Your favorites list is empty.",
    "fav_list_header": "⭐ Your favorites:\n{favs}",
    "unknown_subcommand": "Unknown subcommand. Use `/fav add <mint>` or `/fav list`",
    "key_accepted": "✅ Key accepted. {msg}\nYou can now use /scan",
    "key_rejected": "⛔ {msg}\nPlease try again.",
    "fav_added_callback": "Added to favorites: {mint}",
    "btn_prev": "◀ Prev",
    "btn_next": "▶ Next",
    "btn_details": "ℹ️ Details",
    "btn_back": "◀ Back",
    "btn_birdeye": "Open on Birdeye",
    "btn_solscan": "Open on Solscan",
    "btn_buy": "Buy (Jupiter)",
    "btn_fav_add": "⭐ Add to favorites",
    "btn_share": "Share",
    "card_price": "Price: {price}",
    "card_liquidity": "Liquidity: {liq}",
    "card_fdv": "FDV/MC: {fdv}",
    "card_volume": "Volume 24h: {vol}",
    "card_age": "Age: {age}",
    "card_holders": "Holders: {holders}",
    "card_holders_hidden": "Holders: Hidden on basic plan",
    "card_lp_locked": "LP Locked: {lp}%",
    "card_lp_locked_hidden": "LP Locked: Hidden on basic plan",
    "card_risk": "⚠️ {risks}",
    "risk_low_liquidity": "Low liquidity",
    "risk_low_volume": "Low volume",
    "risk_low_lp_lock": "Low LP lock (<20%)",
    "risk_new_token": "New token (<6h)",
    "risk_mint_authority": "Mint authority active",
    "risk_freeze_authority": "Freeze authority active",
    "risk_top10_concentration": "Top-10 concentration {pct}%",
    "exchanges_header": "Exchanges:",
    "exchanges_empty": "Exchanges: —",
    "exchanges_item": "- {dex}: {liq} liquidity",
    "birdeye_header": "Birdeye:",
    "birdeye_empty": "Birdeye: —",
    "birdeye_item": "- `{key}`: {value}",
    "details_mint_auth": "Mint authority: {auth}",
    "details_freeze_auth": "Freeze authority: {auth}",
    "details_top10": "Top-10 holders: {pct}",
    "details_plan_hint": "_Birdeye plan: basic — detailed stats hidden_",
    "authority_revoked": "revoked",
    "authority_active": "active ({short})",
    "card_header": "🐊 *${symbol}* — {name}",
    "unknown_token_name": "Unknown",
    "unknown_token_symbol": "?",
    "fmt_dash": "—",
    "fmt_yes": "yes",
    "fmt_no": "no",
    "fmt_currency": "$",
    "fmt_million": "M",
    "fmt_kilo": "k",
    "fmt_hours": "h",
    "fmt_days": "d",
}

def T(key: str, **kwargs) -> str:
    return STR.get(key, key).format(**kwargs)

MSG_KW = dict(parse_mode="Markdown", disable_web_page_preview=True)

SCAN_CACHE_TTL = 15
_scan_cache: Dict[str, Any] = {"ts": 0.0, "pairs": []}

SCAN_SESSION_TTL = 300
_scan_cache_sessions: Dict[str, Dict[str, Any]] = {}

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
        InlineKeyboardButton(text=T("btn_prev"), callback_data=f"scan:session:{sid}:idx:{prev_idx}"),
        InlineKeyboardButton(text=T("btn_next"), callback_data=f"scan:session:{sid}:idx:{next_idx}"),
    ]

    row_toggle = (
        [InlineKeyboardButton(text=T("btn_details"), callback_data=f"scan:session:{sid}:detail:{idx}")]
        if mode == "summary"
        else [InlineKeyboardButton(text=T("btn_back"), callback_data=f"scan:session:{sid}:idx:{idx}")]
    )

    be_link = f"https://birdeye.so/token/{mint}?chain=solana"
    solscan_link = f"https://solscan.io/token/{mint}"
    jup_link = f"https://jup.ag/swap?outputMint={mint}"

    row_links1 = [InlineKeyboardButton(text=T("btn_birdeye"), url=be_link)]
    row_links2 = [
        InlineKeyboardButton(text=T("btn_solscan"), url=solscan_link),
        InlineKeyboardButton(text=T("btn_buy"), url=jup_link),
    ]
    row_actions = [
        InlineKeyboardButton(text=T("btn_fav_add"), callback_data=f"fav:add:{mint}"),
        InlineKeyboardButton(text=T("btn_share"), switch_inline_query=mint),
    ]

    return InlineKeyboardMarkup(inline_keyboard=[row_nav, row_toggle, row_links1, row_links2, row_actions])

_last_api_call_ts = 0.0
_api_lock = asyncio.Lock()

async def api_rate_limit(min_interval_sec: float = 1.1):
    global _last_api_call_ts
    async with _api_lock:
        now = time.time()
        wait = (_last_api_call_ts + min_interval_sec) - now
        if wait > 0:
            await asyncio.sleep(wait)
        _last_api_call_ts = time.time()

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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS favorites (
            user_id INTEGER NOT NULL,
            mint TEXT NOT NULL,
            PRIMARY KEY (user_id, mint)
        );
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

def add_favorite(user_id: int, mint: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO favorites(user_id, mint) VALUES (?, ?)",
            (user_id, mint),
        )

def list_favorites(user_id: int) -> list[str]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT mint FROM favorites WHERE user_id = ?",
            (user_id,),
        ).fetchall()
    return [row[0] for row in rows]

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

def format_usd(v: Optional[float]) -> str:
    if v is None:
        return T("fmt_dash")
    try:
        v = float(v)
    except Exception:
        return T("fmt_dash")
    curr = T("fmt_currency")
    if v >= 1_000_000: return f"{curr}{v/1_000_000:.2f}{T('fmt_million')}"
    if v >= 1_000:     return f"{curr}{v/1_000:.2f}{T('fmt_kilo')}"
    if v >= 1:         return f"{curr}{v:.2f}"
    return f"{curr}{v:.6f}"

def from_unix_ms(ms: Optional[int]) -> Optional[datetime]:
    if not ms: return None
    ts = float(ms)
    if ts > 10_000_000_000:
        ts = ts / 1000.0
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return None

def human_age(dt: Optional[datetime]) -> str:
    if not dt: return T("fmt_dash")
    delta = datetime.now(tz=timezone.utc) - dt
    hours = int(delta.total_seconds() // 3600)
    if hours < 24: return f"{hours}{T('fmt_hours')}"
    days = hours // 24
    return f"{days}{T('fmt_days')}"

_mint_re = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")

def normalize_mint_arg(raw: str) -> Optional[str]:
    s = (raw or "").strip()
    if not s:
        return None
    m = re.search(r"/token/([1-9A-HJ-NP-Za-km-z]{32,44})", s)
    if m:
        return m.group(1)
    m = re.search(r"\(([1-9A-HJ-NP-Za-km-z]{32,44})\)", s)
    if m:
        return m.group(1)
    m = _mint_re.search(s)
    return m.group(0) if m else None

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

async def fetch_latest_sol_pairs(limit: int = 8) -> List[Dict[str, Any]]:
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
        return T("exchanges_empty")
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
        return T("exchanges_empty")
    cleaned.sort(key=lambda x: x[1], reverse=True)
    top = cleaned[:2]
    lines = [T("exchanges_header")]
    for dex, liq in top:
        lines.append(T("exchanges_item", dex=dex, liq=format_usd(liq)))
    return "\n".join(lines)

def risk_flags(mint_active: bool, freeze_active: bool, top10_share: Optional[float]) -> List[str]:
    flags = []
    if mint_active:   flags.append(T("risk_mint_authority"))
    if freeze_active: flags.append(T("risk_freeze_authority"))
    try:
        if top10_share is not None and top10_share >= 70.0:
            flags.append(T("risk_top10_concentration", pct=f"{top10_share:.1f}"))
    except Exception:
        pass
    return flags

def token_card(p: Dict[str, Any], extra: Optional[Dict[str, Any]], extra_flags: Optional[List[str]] = None) -> str:
    base = p.get("baseToken", {}) or {}
    symbol = base.get("symbol") or T("unknown_token_symbol")
    name   = base.get("name") or T("unknown_token_name")
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
    if liq_usd is not None and liq_usd < 10_000:
        risk.append(T("risk_low_liquidity"))
    if vol24 is not None and vol24 < 5_000:
        risk.append(T("risk_low_volume"))

    if lp_lock is not None:
        try:
            if float(lp_lock) < 20.0:
                risk.append(T("risk_low_lp_lock"))
        except Exception:
            pass

    if age_dt:
        try:
            hrs = int((datetime.now(tz=timezone.utc) - age_dt).total_seconds() // 3600)
            if hrs < 6:
                risk.append(T("risk_new_token"))
        except Exception:
            pass

    if extra_flags:
        risk.extend(extra_flags)

    lines = [
        T("card_header", symbol=symbol, name=name),
        T("card_price", price=price_txt),
        T("card_liquidity", liq=format_usd(liq_usd)),
        T("card_fdv", fdv=format_usd(fdv)),
        T("card_volume", vol=format_usd(vol24)),
        T("card_age", age=age_txt),
    ]
    if holders is not None:
        lines.append(T("card_holders", holders=f"{holders:,}"))
    else:
        lines.append(T("card_holders_hidden"))
    if lp_lock is not None:
        lines.append(T("card_lp_locked", lp=f"{lp_lock:.1f}"))
    else:
        lines.append(T("card_lp_locked_hidden"))

    if risk:
        lines.append(T("card_risk", risks=", ".join(risk)))

    return "\n".join(lines)

def build_summary_text(p: Dict[str, Any], extra: Optional[Dict[str, Any]], mkts: Optional[List[Dict[str, Any]]]) -> str:
    return token_card(p, extra, extra_flags=None)

def birdeye_kv_block(extra: Optional[Dict[str, Any]]) -> str:
    if not extra:
        return T("birdeye_empty")
    preferred = ["extensions", "decimals", "uniqueHolders24h", "trade24h", "sell24h"]
    simple_items: List[tuple[str, str]] = []
    used = set()

    def _fmt_val(k: str, v: Any) -> str:
        try:
            if v is None:
                return T("fmt_dash")
            if k in ("price", "marketCap", "liquidity", "v24"):
                return format_usd(float(v))
            if isinstance(v, bool):
                return T("fmt_yes") if v else T("fmt_no")
            if isinstance(v, (int, float)):
                return f"{v}"
            return str(v)
        except Exception:
            return str(v)

    for k in preferred:
        if k in extra:
            simple_items.append((k, _fmt_val(k, extra[k])))
            used.add(k)

    for k, v in extra.items():
        if k in used:
            continue
        if isinstance(v, (bool, int, float)):
            simple_items.append((k, _fmt_val(k, v)))

    if not simple_items:
        return T("birdeye_empty")

    lines = [T("birdeye_header")]
    for k, v in simple_items:
        lines.append(T("birdeye_item", key=k, value=v))
    return "\n".join(lines)

def build_details_text(
    p: Dict[str, Any],
    extra: Optional[Dict[str, Any]],
    mkts: Optional[List[Dict[str, Any]]],
    helius_info: Optional[Dict[str, Any]],
    topk_share: Optional[float]
) -> str:
    def f_pct(v: Optional[float]) -> str:
        try:
            if v is None:
                return T("fmt_dash")
            return f"{float(v):.2f}%"
        except Exception:
            return T("fmt_dash")

    add_lines = []
    mint_active = False
    freeze_active = False
    if helius_info:
        mint_txt = format_authority(helius_info.get('mintAuthority'))
        freeze_txt = format_authority(helius_info.get('freezeAuthority'))
        mint_active = (helius_info.get('mintAuthority') is not None)
        freeze_active = (helius_info.get('freezeAuthority') is not None)
        add_lines.append(T("details_mint_auth", auth=mint_txt))
        add_lines.append(T("details_freeze_auth", auth=freeze_txt))
    else:
        add_lines.append(T("details_mint_auth", auth=T("fmt_dash")))
        add_lines.append(T("details_freeze_auth", auth=T("fmt_dash")))

    add_lines.append(T("details_top10", pct=f_pct(topk_share)))

    flags = risk_flags(mint_active, freeze_active, topk_share)

    plan_hint = T("details_plan_hint") if not extra else ""

    be_block = birdeye_kv_block(extra)

    ex_block = exchanges_block(mkts)

    core = token_card(p, extra, extra_flags=flags)

    parts = [
        core,
        "\n".join(add_lines),
        plan_hint,
        be_block,
        ex_block
    ]
    parts = [x.strip() for x in parts if x and x.strip()]
    return "\n\n".join(parts)

def token_keyboard(p: Dict[str, Any], mode: str = "summary") -> InlineKeyboardMarkup:
    mint = (p.get("baseToken") or {}).get("address")
    row_toggle = (
        [InlineKeyboardButton(text=T("btn_details"), callback_data=f"token:{mint}:details")]
        if mode == "summary"
        else [InlineKeyboardButton(text=T("btn_back"), callback_data=f"token:{mint}:summary")]
    )
    be_link = f"https://birdeye.so/token/{mint}?chain=solana"
    solscan_link = f"https://solscan.io/token/{mint}"
    jup_link = f"https://jup.ag/swap?outputMint={mint}"

    row_birdeye = [InlineKeyboardButton(text=T("btn_birdeye"), url=be_link)]
    row_solscan_buy = [
        InlineKeyboardButton(text=T("btn_solscan"), url=solscan_link),
        InlineKeyboardButton(text=T("btn_buy"), url=jup_link),
    ]
    row_actions = [
        InlineKeyboardButton(text=T("btn_fav_add"), callback_data=f"fav:add:{mint}"),
        InlineKeyboardButton(text=T("btn_share"), switch_inline_query=mint),
    ]

    return InlineKeyboardMarkup(
        inline_keyboard=[row_toggle, row_birdeye, row_solscan_buy, row_actions]
    )

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
        data = base64.b64decode(j["result"]["value"]["data"][0])
    except Exception:
        return None
    if len(data) < 82:
        return None
    ma_hex = _pubkey_hex(data, 0)
    fa_hex = _pubkey_hex(data, 32 + 8 + 1 + 1)
    ma = None if ma_hex == "0"*64 else ma_hex
    fa = None if fa_hex == "0"*64 else fa_hex
    return {"mintAuthority": ma, "freezeAuthority": fa}

async def helius_top_holders_share(session: aiohttp.ClientSession, mint: str, k: int = 10) -> Optional[float]:
    j1 = await helius_rpc(session, "getTokenLargestAccounts", [mint])
    if not j1 or "result" not in j1 or not j1["result"].get("value"):
        return None
    j2 = await helius_rpc(session, "getTokenSupply", [mint])
    if not j2 or "result" not in j2 or not j2["result"].get("value"):
        return None
    try:
        total_str = j2["result"]["value"]["amount"]
        total = int(total_str)
        if total <= 0:
            return None
        topk_sum = sum(int(acc["amount"]) for acc in j1["result"]["value"][:k])
        return 100.0 * topk_sum / total
    except Exception:
        return None

def format_authority(val: Optional[str]) -> str:
    if not val:
        return T("authority_revoked")
    short = val[:4] + "..." + val[-4:] if len(val) >= 12 else val
    return T("authority_active", short=short)

async def send_token_card(chat_id: int, mint: str):
    async with aiohttp.ClientSession() as session:
        extra = None
        mkts = None
        if BIRDEYE_API_KEY and mint:
            try:
                extra = await birdeye_overview(session, mint)
            except Exception:
                extra = None
            try:
                mkts = await birdeye_markets(session, mint)
            except Exception:
                mkts = None

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

        if p.get("priceUsd") is None and mint:
            try:
                jp = await jupiter_price(session, mint)
                if jp is not None:
                    p["priceUsd"] = jp
            except Exception:
                pass

        if not p.get("baseToken", {}).get("symbol"):
            await bot.send_message(chat_id, T("token_not_found"), **MSG_KW)
            return

        text = build_summary_text(p, extra, mkts)
        kb = token_keyboard(p, mode="summary")
        await bot.send_message(chat_id, text, reply_markup=kb, **MSG_KW)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher()

@dp.message(Command("start"))
async def start_handler(m: Message):
    if get_user_key(m.from_user.id):
        product_escaped = PRODUCT.replace("_", "\\_")
        await m.answer(T("start", product=product_escaped), **MSG_KW)
    else:
        await m.answer(T("enter_key"), **MSG_KW)

@dp.message(Command("help"))
async def help_handler(m: Message):
    await m.answer(T("help"), **MSG_KW)

@dp.message(Command("my"))
async def my_handler(m: Message):
    key = get_user_key(m.from_user.id)
    if not key:
        await m.answer(T("no_key"), **MSG_KW)
        return
    ok, msg = is_key_valid_for_product(key)
    if not ok:
        await m.answer(T("access_invalid", msg=msg), **MSG_KW)
    else:
        await m.answer(T("key_saved") + f"\n{msg}", **MSG_KW)

@dp.message(Command("logout"))
async def logout_handler(m: Message):
    conn = db()
    conn.execute("DELETE FROM user_access WHERE user_id = ?", (m.from_user.id,))
    conn.commit()
    conn.close()
    await m.answer(T("logged_out"), **MSG_KW)

@dp.message(Command("scan"))
async def scan_handler(m: Message):
    key = get_user_key(m.from_user.id)
    if not key:
        await m.answer(T("no_access"), **MSG_KW)
        return
    ok, msg = is_key_valid_for_product(key)
    if not ok:
        await m.answer(T("access_invalid", msg=msg), **MSG_KW)
        return

    now_ts = int(time.time())
    last_ts = get_last_scan_ts(m.from_user.id)
    if (now_ts - last_ts) < SCAN_COOLDOWN_SEC:
        remaining = SCAN_COOLDOWN_SEC - (now_ts - last_ts)
        await m.answer(T("cooldown", remaining=remaining), **MSG_KW)
        return

    pairs = await fetch_latest_sol_pairs(limit=8)
    if not pairs:
        await m.answer(T("no_pairs"), **MSG_KW)
        return

    set_last_scan_ts(m.from_user.id, now_ts)

    first_idx = 0
    p0 = pairs[first_idx]
    mint0 = (p0.get("baseToken") or {}).get("address") or ""

    sid = _new_sid()
    _cleanup_scan_sessions()
    _scan_cache_sessions[sid] = {"ts": time.time(), "pairs": pairs}

    progress_msg = await m.answer(T("scan_progress", i=1, n=len(pairs)), **MSG_KW)

    async with aiohttp.ClientSession() as session:
        extra0 = None
        if BIRDEYE_API_KEY and mint0:
            try:
                extra0 = await birdeye_overview(session, mint0)
            except Exception:
                extra0 = None

        if p0.get("priceUsd") is None and mint0:
            try:
                jp = await jupiter_price(session, mint0)
                if jp is not None:
                    p0["priceUsd"] = jp
            except Exception:
                pass

    text0 = token_card(p0, extra0, extra_flags=None)
    kb0 = scan_nav_kb(sid, first_idx, mint0, mode="summary")
    await progress_msg.edit_text(text0, reply_markup=kb0, **MSG_KW)

@dp.message(Command("token"))
async def token_handler(m: Message):
    key = get_user_key(m.from_user.id)
    if not key:
        await m.answer(T("no_access"), **MSG_KW)
        return
    ok, msg = is_key_valid_for_product(key)
    if not ok:
        await m.answer(T("access_invalid", msg=msg), **MSG_KW)
        return

    args = (m.text or "").split(maxsplit=1)
    if len(args) < 2:
        await m.answer(T("usage_token"), **MSG_KW)
        return

    raw_arg = args[1]
    mint = normalize_mint_arg(raw_arg)
    if not mint:
        await m.answer(T("cant_detect_mint"), **MSG_KW)
        return

    await m.answer(T("fetching_data", mint=mint), **MSG_KW)
    await send_token_card(m.chat.id, mint)

@dp.message(Command("fav"))
async def fav_handler(m: Message):
    key = get_user_key(m.from_user.id)
    if not key:
        await m.answer(T("no_active_access"), **MSG_KW)
        return

    parts = m.text.strip().split()
    if len(parts) < 2:
        await m.answer(T("fav_usage"), **MSG_KW)
        return

    action = parts[1].lower()
    if action == "add":
        if len(parts) < 3:
            await m.answer(T("fav_add_usage"), **MSG_KW)
            return
        mint = parts[2]
        add_favorite(m.from_user.id, mint)
        await m.answer(T("fav_added", mint=mint), **MSG_KW)
    elif action == "list":
        favs = list_favorites(m.from_user.id)
        if not favs:
            await m.answer(T("fav_empty"), **MSG_KW)
        else:
            await m.answer(T("fav_list_header", favs="\n".join(favs)), **MSG_KW)
    else:
        await m.answer(T("unknown_subcommand"), **MSG_KW)

@dp.callback_query(F.data.startswith("token:"))
async def token_cb_handler(cb: CallbackQuery):
    try:
        _, mint, mode = cb.data.split(":", 2)
    except ValueError:
        await cb.answer(T("bad_callback"))
        return

    extra = None
    mkts = None
    helius_info = None
    topk_share = None

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

    try:
        if mode == "details":
            text = build_details_text(p, extra, mkts, helius_info, topk_share)
            kb = token_keyboard(p, mode="details")
        else:
            text = build_summary_text(p, extra, mkts)
            kb = token_keyboard(p, mode="summary")

        await cb.message.edit_text(text, reply_markup=kb, **MSG_KW)
    except Exception:
        pass

    await cb.answer()

@dp.callback_query(F.data.startswith("scan:session:"))
async def scan_cb_handler(cb: CallbackQuery):
    try:
        parts = cb.data.split(":")
        sid = parts[2]
        action = parts[3]
        idx = int(parts[4])
    except Exception:
        await cb.answer(T("bad_callback"))
        return

    _cleanup_scan_sessions()
    sess = _scan_cache_sessions.get(sid)
    if not sess:
        await cb.answer(T("session_expired"))
        return

    pairs: List[Dict[str, Any]] = sess.get("pairs") or []
    if not pairs:
        await cb.answer(T("no_data"))
        return

    if idx < 0: idx = 0
    if idx >= len(pairs): idx = len(pairs) - 1

    p = pairs[idx]
    mint = (p.get("baseToken") or {}).get("address", "")

    text = None
    kb = None

    async with aiohttp.ClientSession() as session:
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
            text = build_summary_text(p, extra, mkts=None)
            kb = scan_nav_kb(sid, idx, mint, mode="summary")

    try:
        await cb.message.edit_text(text, reply_markup=kb, **MSG_KW)
    except Exception:
        await cb.message.answer(text, reply_markup=kb, **MSG_KW)

    await cb.answer()

@dp.callback_query(F.data.startswith("fav:add:"))
async def fav_add_callback(cb: CallbackQuery):
    parts = cb.data.split(":")
    if len(parts) < 3:
        await cb.answer(T("bad_callback"))
        return
    mint = parts[2]
    user_id = cb.from_user.id

    if not get_user_key(user_id):
        await cb.answer(T("no_active_access"))
        return

    add_favorite(user_id, mint)
    await cb.answer(T("fav_added_callback", mint=mint))

@dp.message(F.text)
async def key_input_handler(m: Message):
    if get_user_key(m.from_user.id):
        return
    candidate = (m.text or "").strip()
    ok, msg = is_key_valid_for_product(candidate)
    if ok:
        bind_user(m.from_user.id, candidate)
        await m.answer(T("key_accepted", msg=msg), **MSG_KW)
    else:
        await m.answer(T("key_rejected", msg=msg), **MSG_KW)

async def main():
    seed_initial_keys()
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
