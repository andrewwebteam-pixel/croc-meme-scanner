import re
import asyncio
import os
import sqlite3
import time
import json
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
SCAN_COOLDOWN_PRO_SEC = int(os.getenv("SCAN_COOLDOWN_PRO_SEC", "10"))

assert BOT_TOKEN, "BOT_TOKEN is required"

BIRDEYE_BASE = "https://public-api.birdeye.so"

# === User-facing strings (UI-06) ===
STR = {
    "no_access": "⛔ No access. Please enter your key via /start.",
    "access_invalid": "⛔ Access invalid: {msg}\nSend a new key.",
    "cooldown": "⏳ Please wait {remaining}s before using /scan again (anti-spam).",
    "no_pairs": "😕 No fresh pairs available via Birdeye on the current plan.\nTry `/token <mint>` or upgrade your data plan.",
    "scan_progress": "🔍 Scanning Solana pairs… ({i}/{n})",
    "start": "Welcome to the {product} bot! Use /help to see commands.",
    "help": (
        "Commands:\n"
        "/token <mint> — get details on a token\n"
        "/scan — scan fresh pairs\n"
        "/fav add <mint> — add token to favorites\n"
        "/fav list — show your favorites\n"
        "/alerts — manage price alerts (coming soon)\n"
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
    "btn_chart": "📈 Chart",
    "btn_fav_add": "⭐ Add to favorites",
    "btn_share": "Share",
    "info_fdv": "FDV (Fully Diluted Valuation) = token price × total supply. Shows potential market cap if all tokens were in circulation.",
    "info_lp": "LP (Liquidity Pool) = funds locked in DEX pairs. Higher LP = easier to trade without slippage. Locked LP means devs can't rug pull.",
    "card_price": "Price: {price}",
    "card_liquidity": "Liquidity: {liq}",
    "card_fdv": "FDV/MC: {fdv}",
    "card_volume": "Volume 24h: {vol}",
    "card_age": "Age: {age}",
    "card_holders": "Holders: {holders}",
    "card_holders_hidden": "Holders: Hidden on Free plan",
    "card_lp_locked": "LP Locked: {lp}%",
    "card_lp_locked_hidden": "LP Locked: Hidden on Free plan",
    "card_risk": "⚠️ Risk: {risks}",
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
    "exchanges_hidden": "Exchanges: Hidden on Free plan",
    "birdeye_header": "Birdeye:",
    "birdeye_empty": "Birdeye: —",
    "birdeye_item": "- `{key}`: {value}",
    "details_mint_auth": "Mint authority: {auth}",
    "details_freeze_auth": "Freeze authority: {auth}",
    "details_top10": "Top-10 holders: {pct}",
    "details_top10_hidden": "Top-10 holders: Hidden on Free plan",
    "details_plan_hint": "_Upgrade to PRO for full data access_",
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
    "card_risk_score": "⚠️ Risk: {score}/100",
    "details_risk_why": "Why: {reasons}",
    "alerts_soon": "🔔 Alerts are coming soon. Stay tuned!",
    "my_status_valid": "✅ {msg}\nTier: {tier}",
    "my_status_invalid": "⛔ {msg}",
    "alert_set_usage": "Usage: `/alerts set <mint> <price>`",
    "alert_set_success": "✅ Alert set for {mint} at ${price}",
    "alert_list_empty": "No alerts set. Use `/alerts set <mint> <price>`",
    "alert_list_header": "🔔 Your alerts:\n{alerts}",
    "alert_invalid_price": "❌ Invalid price. Please use a number.",
    "no_pairs_all_sources": "😕 No fresh pairs available from any source.\nTry `/token <mint>` instead.",
    "chain_current": "Current chain: {chain}",
    "chain_set": "✅ Chain set to: {chain}",
    "chain_invalid": "❌ Invalid chain. Use: sol, eth, or bsc",
    "chain_usage": "Usage: `/chain <sol|eth|bsc>` or `/chain` to see current",
    "chain_not_supported": "⚠️ {chain} support coming soon. Only Solana (sol) is currently available.\nUse `/chain sol` to switch back.",
}

def T(key: str, **kwargs) -> str:
    return STR.get(key, key).format(**kwargs)

MSG_KW = dict(parse_mode="Markdown", disable_web_page_preview=True)

SCAN_CACHE_TTL = 15
_scan_cache: Dict[str, Any] = {"ts": 0.0, "pairs": []}

SCAN_SESSION_TTL = 300
_scan_cache_sessions: Dict[str, Dict[str, Any]] = {}

TOKEN_SESSION_TTL = 600
_token_sessions: Dict[str, Dict[str, Any]] = {}

def _new_sid() -> str:
    return str(int(time.time()*1000)) + "-" + os.urandom(3).hex()

def _cleanup_scan_sessions():
    now = time.time()
    for k in list(_scan_cache_sessions.keys()):
        if _scan_cache_sessions[k].get("ts", 0) + SCAN_SESSION_TTL < now:
            _scan_cache_sessions.pop(k, None)

def _cleanup_token_sessions():
    now = time.time()
    for k in list(_token_sessions.keys()):
        if _token_sessions[k].get("ts", 0) + TOKEN_SESSION_TTL < now:
            _token_sessions.pop(k, None)

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
            expires_at TEXT NULL,
            tier TEXT DEFAULT 'free'
        )
    """)
    
    try:
        conn.execute("ALTER TABLE access_keys ADD COLUMN tier TEXT DEFAULT 'free'")
        print("[DB] Added tier column to access_keys table")
    except sqlite3.OperationalError:
        pass
    
    try:
        conn.execute("ALTER TABLE favorites ADD COLUMN added_at INTEGER NOT NULL DEFAULT 0")
        print("[DB] Added added_at column to favorites table")
    except sqlite3.OperationalError:
        pass
    
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
            added_at INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (user_id, mint)
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            user_id INTEGER,
            cmd TEXT NOT NULL,
            args TEXT,
            ok INTEGER NOT NULL,
            ms INTEGER,
            err TEXT
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            user_id INTEGER PRIMARY KEY,
            thresholds TEXT,
            allowlist TEXT,
            blocklist TEXT,
            created_at INTEGER NOT NULL
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_chain (
            user_id INTEGER PRIMARY KEY,
            chain TEXT NOT NULL DEFAULT 'sol'
        );
    """)
    return conn

def seed_initial_keys():
    conn = db()
    conn.execute("INSERT OR REPLACE INTO access_keys (access_key, product, expires_at, tier) VALUES (?, ?, NULL, ?)", (ADMIN_KEY, PRODUCT, "pro"))
    conn.execute("INSERT OR IGNORE INTO access_keys (access_key, product, expires_at, tier) VALUES (?, ?, ?, ?)", ("TEST-1234", PRODUCT, "2099-12-31", "free"))
    print(f"[DB] {ADMIN_KEY} set to PRO tier")
    conn.commit()
    conn.close()

def key_info(access_key: str) -> Optional[tuple]:
    conn = db()
    cur = conn.execute("SELECT access_key, product, expires_at, tier FROM access_keys WHERE access_key = ?", (access_key,))
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

def log_command(user_id: int, cmd: str, args: str = "", ok: bool = True, ms: int = 0, err: str = ""):
    """Log command execution to database (non-blocking)"""
    try:
        conn = sqlite3.connect(DB_PATH)
        ts = int(time.time())
        conn.execute(
            "INSERT INTO logs(ts, user_id, cmd, args, ok, ms, err) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ts, user_id, cmd, args, 1 if ok else 0, ms, err)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[LOG] Failed to log command: {e}")

def is_key_valid_for_product(access_key: str) -> tuple[bool, str]:
    info = key_info(access_key)
    if not info:
        return False, "Invalid key."
    _, product, expires_at, _ = info
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

def is_pro_user(user_id: int) -> bool:
    """Check if user has PRO tier access"""
    key = get_user_key(user_id)
    if not key:
        return False
    info = key_info(key)
    if not info:
        return False
    tier = info[3] if len(info) > 3 else "free"
    return tier == "pro"

def add_favorite(user_id: int, mint: str):
    with sqlite3.connect(DB_PATH) as conn:
        ts = int(time.time())
        conn.execute(
            "INSERT OR REPLACE INTO favorites(user_id, mint, added_at) VALUES (?, ?, ?)",
            (user_id, mint, ts),
        )

def list_favorites(user_id: int) -> list[str]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT mint FROM favorites WHERE user_id = ? ORDER BY added_at DESC",
            (user_id,),
        ).fetchall()
    return [row[0] for row in rows]

def get_user_chain(user_id: int) -> str:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT chain FROM user_chain WHERE user_id = ?", (user_id,)).fetchone()
    return row[0] if row else "sol"

def set_user_chain(user_id: int, chain: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("INSERT OR REPLACE INTO user_chain (user_id, chain) VALUES (?, ?)", (user_id, chain))

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
    """
    Fetch fresh Solana pairs with fallback strategy:
    1. Try Birdeye /defi/v2/markets (sorted by liquidity)
    2. If that fails or returns empty, try Birdeye /defi/recently_added
    3. If both Birdeye endpoints fail, try DexScreener /latest/dex/tokens/solana
    4. Deduplicate by mint, sort by pairCreatedAt, limit to 8 pairs
    """
    if (_scan_cache["ts"] + SCAN_CACHE_TTL) > time.time() and _scan_cache["pairs"]:
        return _scan_cache["pairs"][:limit]

    pairs = []

    if not BIRDEYE_API_KEY:
        print("[SCAN] Birdeye: BIRDEYE_API_KEY is empty -> skipping Birdeye, will try DexScreener")
    else:
        headers = {
            "accept": "application/json",
            "X-API-KEY": BIRDEYE_API_KEY,
            "x-chain": "solana"
        }
    
        url_markets = f"{BIRDEYE_BASE}/defi/v2/markets"
        params_markets = {"sort_by": "liquidity", "sort_type": "desc", "offset": 0, "limit": 50}
        
        try:
            await api_rate_limit()
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
                async with s.get(url_markets, headers=headers, params=params_markets) as r:
                    status = r.status
                    if status == 403:
                        print("[SCAN] /defi/v2/markets returned 403 (plan limit) - falling back to recently_added")
                    elif status == 429:
                        print("[SCAN] /defi/v2/markets returned 429 (rate limit) - falling back to recently_added")
                    elif status == 200:
                        try:
                            j = await r.json()
                            if j and j.get("success"):
                                data = j.get("data") or {}
                                items = data.get("items") if isinstance(data, dict) else data
                                if items and isinstance(items, list):
                                    for m in items[:50]:
                                        try:
                                            base = {
                                                "symbol": m.get("symbol") or "",
                                                "name": m.get("name") or "",
                                                "address": m.get("address") or m.get("baseAddress") or ""
                                            }
                                            pairs.append({
                                                "baseToken": base,
                                                "priceUsd": m.get("price"),
                                                "liquidity": {"usd": m.get("liquidity") or m.get("liquidityUsd")},
                                                "fdv": m.get("marketCap") or m.get("fdv"),
                                                "volume": {"h24": m.get("v24hUSD") or m.get("v24h") or m.get("volume24h")},
                                                "pairCreatedAt": m.get("createdAt") or m.get("firstTradeAt"),
                                                "chainId": "solana",
                                            })
                                        except Exception as e:
                                            print(f"[SCAN] pair build error: {e}")
                                            continue
                                    print(f"[SCAN] /defi/v2/markets returned {len(pairs)} pairs")
                            else:
                                print(f"[SCAN] /defi/v2/markets success==false: {str(j)[:200]}")
                        except Exception as e:
                            print(f"[SCAN] /defi/v2/markets parse error: {e}")
                    else:
                        try:
                            txt = await r.text()
                        except Exception:
                            txt = "<no body>"
                        print(f"[SCAN] /defi/v2/markets HTTP {status} -> {txt[:200]}")
        except Exception as e:
            print(f"[SCAN] /defi/v2/markets exception: {e}")
        
        if not pairs:
            print("[SCAN] Trying fallback: /defi/recently_added")
            url_recent = f"{BIRDEYE_BASE}/defi/recently_added"
            params_recent = {"chain": "solana", "limit": 20}
            
            try:
                await api_rate_limit()
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
                    async with s.get(url_recent, headers=headers, params=params_recent) as r:
                        if r.status == 200:
                            try:
                                j = await r.json()
                                if j and j.get("success"):
                                    data = j.get("data") or {}
                                    items = data.get("items") if isinstance(data, dict) else (data if isinstance(data, list) else [])
                                    for token in (items or []):
                                        try:
                                            mint = token.get("address") or token.get("mint") or ""
                                            if not mint:
                                                continue
                                            base = {
                                                "symbol": token.get("symbol") or "",
                                                "name": token.get("name") or "",
                                                "address": mint
                                            }
                                            pairs.append({
                                                "baseToken": base,
                                                "priceUsd": token.get("price"),
                                                "liquidity": {"usd": token.get("liquidity")},
                                                "fdv": token.get("mc") or token.get("marketCap"),
                                                "volume": {"h24": token.get("v24hUSD") or token.get("v24h")},
                                                "pairCreatedAt": token.get("createdAt") or token.get("firstTradeUnixTime"),
                                                "chainId": "solana",
                                            })
                                        except Exception as e:
                                            print(f"[SCAN] recently_added build error: {e}")
                                            continue
                                    print(f"[SCAN] /defi/recently_added returned {len(pairs)} pairs")
                                else:
                                    print(f"[SCAN] /defi/recently_added success==false: {str(j)[:200]}")
                            except Exception as e:
                                print(f"[SCAN] /defi/recently_added parse error: {e}")
                        else:
                            try:
                                txt = await r.text()
                            except Exception:
                                txt = "<no body>"
                            print(f"[SCAN] /defi/recently_added HTTP {r.status} -> {txt[:200]}")
            except Exception as e:
                print(f"[SCAN] /defi/recently_added exception: {e}")
    
    if not pairs:
        print("[SCAN] Trying external fallback: GeckoTerminal")
        try:
            await api_rate_limit()
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
                headers = {"Accept": "application/json"}
                async with s.get("https://api.geckoterminal.com/api/v2/networks/solana/new_pools", headers=headers) as r:
                    if r.status == 200:
                        try:
                            j = await r.json()
                            if j and isinstance(j.get("data"), list):
                                seen_mints = set()
                                for pool_data in j["data"]:
                                    try:
                                        pool = pool_data.get("attributes", {})
                                        base_token = pool.get("base_token", {})
                                        mint = base_token.get("address", "")
                                        if not mint or mint in seen_mints:
                                            continue
                                        seen_mints.add(mint)
                                        
                                        created_at = pool.get("pool_created_at")
                                        if created_at:
                                            from datetime import datetime
                                            try:
                                                created_ts = int(datetime.fromisoformat(created_at.replace('Z', '+00:00')).timestamp())
                                            except:
                                                created_ts = 0
                                        else:
                                            created_ts = 0
                                        
                                        pairs.append({
                                            "baseToken": {
                                                "symbol": base_token.get("symbol", ""),
                                                "name": base_token.get("name", ""),
                                                "address": mint
                                            },
                                            "priceUsd": float(pool.get("base_token_price_usd", 0)) if pool.get("base_token_price_usd") else None,
                                            "liquidity": {"usd": float(pool.get("reserve_in_usd", 0)) if pool.get("reserve_in_usd") else None},
                                            "fdv": float(pool.get("fdv_usd", 0)) if pool.get("fdv_usd") else None,
                                            "volume": {"h24": float(pool.get("volume_usd", {}).get("h24", 0)) if pool.get("volume_usd") else None},
                                            "pairCreatedAt": created_ts,
                                            "chainId": "solana",
                                        })
                                    except Exception as e:
                                        print(f"[SCAN] GeckoTerminal pool error: {e}")
                                        continue
                                
                                pairs.sort(key=lambda x: x.get("pairCreatedAt", 0), reverse=True)
                                pairs = pairs[:8]
                                print(f"[SCAN] GeckoTerminal returned {len(pairs)} pairs")
                            else:
                                print(f"[SCAN] GeckoTerminal unexpected format: {str(j)[:200]}")
                        except Exception as e:
                            print(f"[SCAN] GeckoTerminal parse error: {e}")
                    else:
                        print(f"[SCAN] GeckoTerminal HTTP {r.status}")
        except Exception as e:
            print(f"[SCAN] GeckoTerminal exception: {e}")
    
    if pairs:
        _scan_cache["ts"] = time.time()
        _scan_cache["pairs"] = pairs
        print(f"[SCAN] Successfully cached {len(pairs)} pairs")
        return pairs[:limit]
    
    return []

async def birdeye_overview(session: aiohttp.ClientSession, mint: str) -> Optional[Dict[str, Any]]:
    if not BIRDEYE_API_KEY:
        return None
    url = f"{BIRDEYE_BASE}/defi/token_overview"
    headers = {
        "accept": "application/json",
        "X-API-KEY": BIRDEYE_API_KEY,
        "x-chain": "solana"
    }
    params = {"address": mint}
    try:
        await api_rate_limit()
        async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 403:
                print(f"[BIRDEYE] token_overview 403 (plan limit) for {mint[:8]}...")
                return None
            if r.status == 429:
                print(f"[BIRDEYE] token_overview 429 (rate limit) for {mint[:8]}...")
                return None
            if r.status != 200:
                try:
                    txt = await r.text()
                except Exception:
                    txt = "<no body>"
                print(f"[BIRDEYE] token_overview HTTP {r.status} for {mint[:8]}... -> {txt[:200]}")
                return None
            j = await r.json()
            if not j or not j.get("success"):
                print(f"[BIRDEYE] token_overview failed for {mint[:8]}...: {str(j)[:200]}")
                return None
            return j.get("data") or j
    except Exception as e:
        print(f"[BIRDEYE] token_overview exception for {mint[:8]}...: {e}")
        return None

async def birdeye_token_security(session: aiohttp.ClientSession, mint: str) -> Optional[Dict[str, Any]]:
    """Fetch token security info (mint authority, freeze authority, etc.)"""
    if not BIRDEYE_API_KEY:
        return None
    url = f"{BIRDEYE_BASE}/defi/token_security"
    headers = {
        "accept": "application/json",
        "X-API-KEY": BIRDEYE_API_KEY,
        "x-chain": "solana"
    }
    params = {"address": mint}
    try:
        await api_rate_limit()
        async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                j = await r.json()
                if j and j.get("success"):
                    return j.get("data")
            else:
                print(f"[BIRDEYE] token_security HTTP {r.status} for {mint[:8]}...")
            return None
    except Exception as e:
        print(f"[BIRDEYE] token_security exception for {mint[:8]}...: {e}")
        return None

async def birdeye_price(session: aiohttp.ClientSession, mint: str) -> Optional[float]:
    """Fetch live price from Birdeye"""
    if not BIRDEYE_API_KEY:
        return None
    url = f"{BIRDEYE_BASE}/defi/price"
    headers = {
        "accept": "application/json",
        "X-API-KEY": BIRDEYE_API_KEY,
        "x-chain": "solana"
    }
    params = {"address": mint}
    try:
        await api_rate_limit()
        async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                j = await r.json()
                if j and j.get("success"):
                    data = j.get("data") or {}
                    price = data.get("value")
                    if price is not None:
                        return float(price)
            return None
    except Exception:
        return None

async def dexscreener_token(session: aiohttp.ClientSession, mint: str) -> Optional[Dict[str, Any]]:
    """Fetch token info from DexScreener as fallback"""
    try:
        await api_rate_limit(min_interval_sec=0.5)
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                print(f"[DEXSCREENER] token HTTP {r.status} for {mint[:8]}...")
                return None
            j = await r.json()
            if not j or not isinstance(j.get("pairs"), list) or len(j["pairs"]) == 0:
                print(f"[DEXSCREENER] no pairs for {mint[:8]}...")
                return None
            pair = j["pairs"][0]
            base_token = pair.get("baseToken", {})
            return {
                "symbol": base_token.get("symbol", ""),
                "name": base_token.get("name", ""),
                "address": mint,
                "price": float(pair.get("priceUsd", 0)) if pair.get("priceUsd") else None,
                "liquidity": float(pair.get("liquidity", {}).get("usd", 0)) if pair.get("liquidity") else None,
                "fdv": float(pair.get("fdv", 0)) if pair.get("fdv") else None,
                "marketCap": float(pair.get("marketCap", 0)) if pair.get("marketCap") else None,
                "v24hUSD": float(pair.get("volume", {}).get("h24", 0)) if pair.get("volume") else None,
                "pairCreatedAt": pair.get("pairCreatedAt"),
            }
    except Exception as e:
        print(f"[DEXSCREENER] token exception for {mint[:8]}...: {e}")
        return None

async def birdeye_markets(session: aiohttp.ClientSession, mint: str) -> Optional[List[Dict[str, Any]]]:
    if not BIRDEYE_API_KEY:
        return None
    url = f"{BIRDEYE_BASE}/defi/v2/markets"
    headers = {
        "accept": "application/json",
        "X-API-KEY": BIRDEYE_API_KEY,
        "x-chain": "solana"
    }
    params = {"address": mint, "sort_by": "liquidity", "sort_type": "desc"}
    try:
        await api_rate_limit()
        async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 403 or r.status == 429:
                print(f"[BIRDEYE] markets {r.status} for {mint[:8]}...")
                return None
            if r.status != 200:
                try:
                    txt = await r.text()
                except Exception:
                    txt = "<no body>"
                print(f"[BIRDEYE] markets HTTP {r.status} for {mint[:8]}... -> {txt[:200]}")
                return None
            j = await r.json()
            if not j or not j.get("success"):
                print(f"[BIRDEYE] markets failed for {mint[:8]}...: {str(j)[:200]}")
                return None
            data = j.get("data") or {}
            items = data.get("items") if isinstance(data, dict) else data
            return items if isinstance(items, list) else None
    except Exception as e:
        print(f"[BIRDEYE] markets exception for {mint[:8]}...: {e}")
        return None

def extract_holders(data: Dict[str, Any]) -> Optional[int]:
    for k in ("holders", "holder", "holder_count", "holdersCount", "uniqueHolders"):
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
    for k in ("createdAt", "firstTradeAt", "first_trade_at", "first_trade_unix", "firstTradeUnixTime"):
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

def exchanges_block(markets: Optional[List[Dict[str, Any]]], is_pro: bool = False) -> str:
    if not is_pro:
        return T("exchanges_hidden")
    if not markets:
        return T("exchanges_empty")
    cleaned = []
    for m in markets:
        dex = m.get("source") or m.get("dex") or m.get("market") or m.get("name")
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

def calc_risk_score(
    liquidity: Optional[float],
    volume: Optional[float],
    lp_lock_pct: Optional[float],
    token_age_hours: float,
    mint_auth_active: bool,
    freeze_auth_active: bool,
    top10_pct: Optional[float]
) -> tuple[int, List[str]]:
    """Calculate risk score (0-100) where 100 = low risk. Returns (score, reasons)."""
    reasons = []
    score = 100
    
    if liquidity is not None and liquidity < 10_000:
        score -= 15
        reasons.append(T("risk_low_liquidity"))
    
    if volume is not None and volume < 10_000:
        score -= 10
        reasons.append(T("risk_low_volume"))
    
    if lp_lock_pct is not None and lp_lock_pct < 20:
        score -= 15
        reasons.append(T("risk_low_lp_lock"))
    
    if token_age_hours < 6:
        score -= 20
        reasons.append(T("risk_new_token"))
    
    if mint_auth_active:
        score -= 15
        reasons.append(T("risk_mint_authority"))
    
    if freeze_auth_active:
        score -= 15
        reasons.append(T("risk_freeze_authority"))
    
    if top10_pct is not None and top10_pct > 50:
        score -= 10
        reasons.append(T("risk_top10_concentration", pct=f"{top10_pct:.0f}"))
    
    score = max(0, min(100, score))
    return score, reasons

def token_card(
    p: Dict[str, Any],
    extra: Optional[Dict[str, Any]],
    is_pro: bool,
    risk_list: Optional[List[str]] = None
) -> str:
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

    lines = [
        T("card_header", symbol=symbol, name=name),
        T("card_price", price=price_txt),
        T("card_liquidity", liq=format_usd(liq_usd)),
        T("card_fdv", fdv=format_usd(fdv)),
        T("card_volume", vol=format_usd(vol24)),
        T("card_age", age=age_txt),
    ]
    
    if is_pro:
        if holders is not None:
            lines.append(T("card_holders", holders=f"{holders:,}"))
        else:
            lines.append(T("card_holders", holders=T("fmt_dash")))
        if lp_lock is not None:
            lines.append(T("card_lp_locked", lp=f"{lp_lock:.1f}"))
        else:
            lines.append(T("card_lp_locked", lp=T("fmt_dash")))
    else:
        lines.append(T("card_holders_hidden"))
        lines.append(T("card_lp_locked_hidden"))

    if risk_list:
        lines.append(T("card_risk", risks=", ".join(risk_list)))

    return "\n".join(lines)

def build_summary_text(
    p: Dict[str, Any],
    extra: Optional[Dict[str, Any]],
    mkts: Optional[List[Dict[str, Any]]],
    is_pro: bool,
    mint_active: bool = False,
    freeze_active: bool = False,
    top10_share: Optional[float] = None
) -> str:
    liq_usd = (p.get("liquidity") or {}).get("usd")
    vol24   = (p.get("volume") or {}).get("h24")
    lp_lock = extract_lp_lock_ratio(extra or {}) if extra else None
    age_dt = extract_created_at(extra) if extra else None
    if not age_dt:
        age_dt = from_unix_ms(p.get("pairCreatedAt"))
    
    age_hours = (datetime.now(tz=timezone.utc) - age_dt).total_seconds() / 3600 if age_dt else 0
    risk_score, risk_reasons = calc_risk_score(
        liq_usd, vol24, lp_lock, age_hours, 
        mint_active, freeze_active, top10_share
    )
    
    base = p.get("baseToken", {}) or {}
    symbol = base.get("symbol") or T("unknown_token_symbol")
    name   = base.get("name") or T("unknown_token_name")
    price  = p.get("priceUsd")
    price_txt = format_usd(price)
    
    liq_txt = format_usd(liq_usd)
    fdv = p.get("fdv")
    age_txt = human_age(age_dt)
    
    holders = extract_holders(extra or {}) if extra else None
    
    lines = [
        T("card_header", symbol=symbol, name=name),
        T("card_price", price=price_txt),
        T("card_liquidity", liq=liq_txt),
        T("card_fdv", fdv=format_usd(fdv)),
        T("card_volume", vol=format_usd(vol24)),
        T("card_age", age=age_txt),
    ]
    
    if is_pro:
        if holders is not None:
            lines.append(T("card_holders", holders=f"{holders:,}"))
        else:
            lines.append(T("card_holders", holders=T("fmt_dash")))
        if lp_lock is not None:
            lines.append(T("card_lp_locked", lp=f"{lp_lock:.1f}"))
        else:
            lines.append(T("card_lp_locked", lp=T("fmt_dash")))
    else:
        lines.append(T("card_holders_hidden"))
        lines.append(T("card_lp_locked_hidden"))
    
    if risk_score < 100:
        lines.append(T("card_risk_score", score=risk_score))
    
    return "\n".join(lines)

def birdeye_kv_block(extra: Optional[Dict[str, Any]]) -> str:
    if not extra:
        return T("birdeye_empty")
    preferred = ["extensions", "decimals", "uniqueHolders24h", "uniqueHolders", "trade24h", "sell24h"]
    simple_items: List[tuple[str, str]] = []
    used = set()

    def _fmt_val(k: str, v: Any) -> str:
        try:
            if v is None:
                return T("fmt_dash")
            if k in ("price", "marketCap", "mc", "liquidity", "v24hUSD", "v24h"):
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
    for item_key, item_val in simple_items[:6]:
        lines.append(T("birdeye_item", key=item_key, value=item_val))
    return "\n".join(lines)

def format_authority(auth: Optional[str]) -> str:
    if not auth:
        return T("authority_revoked")
    short = auth[:4] + "..." + auth[-4:] if len(auth) > 12 else auth
    return T("authority_active", short=short)

def build_details_text(
    p: Dict[str, Any],
    extra: Optional[Dict[str, Any]],
    mkts: Optional[List[Dict[str, Any]]],
    helius_info: Optional[Dict[str, Any]],
    topk_share: Optional[float],
    is_pro: bool
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

    if is_pro:
        if topk_share is not None:
            add_lines.append(T("details_top10", pct=f_pct(topk_share)))
        else:
            add_lines.append(T("details_top10", pct=T("fmt_dash")))
    else:
        add_lines.append(T("details_top10_hidden"))

    liq_usd = (p.get("liquidity") or {}).get("usd")
    vol24   = (p.get("volume") or {}).get("h24")
    lp_lock = extract_lp_lock_ratio(extra or {}) if extra else None
    age_dt = extract_created_at(extra) if extra else None
    if not age_dt:
        age_dt = from_unix_ms(p.get("pairCreatedAt"))
    
    age_hours = (datetime.now(tz=timezone.utc) - age_dt).total_seconds() / 3600 if age_dt else 0
    risk_score, risk_reasons = calc_risk_score(
        liq_usd, vol24, lp_lock, age_hours,
        mint_active, freeze_active, topk_share
    )

    plan_hint = "" if is_pro else T("details_plan_hint")

    be_block = birdeye_kv_block(extra)

    ex_block = exchanges_block(mkts, is_pro)

    base = p.get("baseToken", {}) or {}
    symbol = base.get("symbol") or T("unknown_token_symbol")
    name = base.get("name") or T("unknown_token_name")
    price_txt = format_usd(p.get("priceUsd"))
    liq_txt = format_usd(liq_usd)
    fdv_txt = format_usd(p.get("fdv"))
    vol_txt = format_usd(vol24)
    age_txt = human_age(age_dt)
    holders = extract_holders(extra or {}) if extra else None

    core_lines = [
        T("card_header", symbol=symbol, name=name),
        T("card_price", price=price_txt),
        T("card_liquidity", liq=liq_txt),
        T("card_fdv", fdv=fdv_txt),
        T("card_volume", vol=vol_txt),
        T("card_age", age=age_txt),
    ]
    
    if is_pro:
        if holders is not None:
            core_lines.append(T("card_holders", holders=f"{holders:,}"))
        else:
            core_lines.append(T("card_holders", holders=T("fmt_dash")))
        if lp_lock is not None:
            core_lines.append(T("card_lp_locked", lp=f"{lp_lock:.1f}"))
        else:
            core_lines.append(T("card_lp_locked", lp=T("fmt_dash")))
    else:
        core_lines.append(T("card_holders_hidden"))
        core_lines.append(T("card_lp_locked_hidden"))
    
    if risk_score < 100:
        core_lines.append(T("card_risk_score", score=risk_score))
        if risk_reasons:
            core_lines.append(T("details_risk_why", reasons=", ".join(risk_reasons)))

    core = "\n".join(core_lines)

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
    chart_link = f"https://dexscreener.com/solana/{mint}"

    row_buy_chart = [
        InlineKeyboardButton(text=T("btn_buy"), url=jup_link),
        InlineKeyboardButton(text=T("btn_chart"), url=chart_link),
    ]
    row_explorers = [
        InlineKeyboardButton(text=T("btn_birdeye"), url=be_link),
        InlineKeyboardButton(text=T("btn_solscan"), url=solscan_link),
    ]
    row_actions = [
        InlineKeyboardButton(text=T("btn_fav_add"), callback_data=f"fav:add:{mint}"),
        InlineKeyboardButton(text=T("btn_share"), switch_inline_query=mint),
    ]
    row_info = [
        InlineKeyboardButton(text="ℹ️ About FDV", callback_data="info:fdv"),
        InlineKeyboardButton(text="ℹ️ About LP", callback_data="info:lp"),
    ]

    return InlineKeyboardMarkup(
        inline_keyboard=[row_toggle, row_buy_chart, row_explorers, row_actions, row_info]
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
    val = j["result"]["value"]
    data_str = (val.get("data") or [""])[0]
    try:
        data_bytes = base64.b64decode(data_str)
    except Exception:
        return None
    if len(data_bytes) < 82:
        return None

    mint_auth_opt = _u64_le(data_bytes, 0)
    freeze_auth_opt = _u64_le(data_bytes, 46)

    mint_auth = _pubkey_hex(data_bytes, 4) if mint_auth_opt == 1 else None
    freeze_auth = _pubkey_hex(data_bytes, 50) if freeze_auth_opt == 1 else None

    return {
        "mintAuthority": mint_auth,
        "freezeAuthority": freeze_auth,
    }

async def helius_top_holders_share(session: aiohttp.ClientSession, mint: str, k: int = 10) -> Optional[float]:
    j = await helius_rpc(session, "getTokenLargestAccounts", [mint, {"commitment": "finalized"}])
    if not j or "result" not in j:
        return None
    result = j["result"]
    accounts = result.get("value") or []
    if not accounts:
        return None
    total_supply = sum(int(acc.get("amount") or 0) for acc in accounts)
    if total_supply <= 0:
        return None
    topk = sorted(accounts, key=lambda x: int(x.get("amount") or 0), reverse=True)[:k]
    topk_sum = sum(int(acc.get("amount") or 0) for acc in topk)
    pct = (topk_sum / total_supply) * 100.0
    return pct

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher()

@dp.message(Command("start"))
async def start_handler(m: Message):
    if not m.from_user:
        return
    key = get_user_key(m.from_user.id)
    if key:
        await m.answer(T("start", product=PRODUCT.replace("_", "\\_")), **MSG_KW)
    else:
        await m.answer(T("enter_key"), **MSG_KW)

@dp.message(Command("help"))
async def help_handler(m: Message):
    await m.answer(T("help"), **MSG_KW)

@dp.message(Command("logout"))
async def logout_handler(m: Message):
    if not m.from_user:
        return
    user_id = m.from_user.id
    if not get_user_key(user_id):
        await m.answer(T("no_key"), **MSG_KW)
        return
    conn = db()
    conn.execute("DELETE FROM user_access WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()
    await m.answer(T("logged_out"), **MSG_KW)

@dp.message(Command("my"))
async def my_handler(m: Message):
    if not m.from_user:
        return
    user_id = m.from_user.id
    key = get_user_key(user_id)
    if not key:
        await m.answer(T("no_key"), **MSG_KW)
        return
    valid, msg = is_key_valid_for_product(key)
    tier = "PRO" if is_pro_user(user_id) else "Free"
    if valid:
        await m.answer(T("my_status_valid", msg=msg, tier=tier), **MSG_KW)
    else:
        await m.answer(T("my_status_invalid", msg=msg), **MSG_KW)

@dp.message(Command("scan"))
async def scan_handler(m: Message):
    if not m.from_user:
        return
    start_time = time.time()
    user_id = m.from_user.id
    key = get_user_key(user_id)
    if not key:
        log_command(user_id, "/scan", "", ok=False, err="no_access")
        await m.answer(T("no_access"), **MSG_KW)
        return
    valid, msg = is_key_valid_for_product(key)
    if not valid:
        log_command(user_id, "/scan", "", ok=False, err="access_invalid")
        await m.answer(T("access_invalid", msg=msg), **MSG_KW)
        return

    chain = get_user_chain(user_id)
    if chain != "sol":
        await m.answer(T("chain_not_supported", chain=chain), **MSG_KW)
        return

    is_pro = is_pro_user(user_id)
    cooldown = SCAN_COOLDOWN_PRO_SEC if is_pro else SCAN_COOLDOWN_SEC

    now_ts = int(time.time())
    last_ts = get_last_scan_ts(user_id)
    if (now_ts - last_ts) < cooldown:
        remaining = cooldown - (now_ts - last_ts)
        log_command(user_id, "/scan", "", ok=False, err="cooldown")
        await m.answer(T("cooldown", remaining=remaining), **MSG_KW)
        return

    set_last_scan_ts(user_id, now_ts)

    status = await m.answer(T("scan_progress", i=0, n=0), **MSG_KW)

    pairs = await fetch_latest_sol_pairs(limit=8)

    if not pairs:
        log_command(user_id, "/scan", "", ok=False, err="no_pairs")
        await status.edit_text(T("no_pairs_all_sources"), **MSG_KW)
        return

    _cleanup_scan_sessions()
    sid = _new_sid()
    _scan_cache_sessions[sid] = {
        "pairs": pairs,
        "ts": time.time()
    }

    p = pairs[0]
    mint = (p.get("baseToken") or {}).get("address", "")

    async with aiohttp.ClientSession() as session:
        extra = None
        if BIRDEYE_API_KEY and mint:
            extra = await birdeye_overview(session, mint)
        if p.get("priceUsd") is None and mint:
            jp = await jupiter_price(session, mint)
            if jp is not None:
                p["priceUsd"] = jp

    text = build_summary_text(p, extra, mkts=None, is_pro=is_pro)
    kb = scan_nav_kb(sid, 0, mint, mode="summary")

    try:
        await status.edit_text(text, reply_markup=kb, **MSG_KW)
    except Exception:
        await m.answer(text, reply_markup=kb, **MSG_KW)

    elapsed_ms = int((time.time() - start_time) * 1000)
    log_command(user_id, "/scan", f"sid={sid}", ok=True, ms=elapsed_ms)

@dp.message(Command("token"))
async def token_handler(m: Message):
    if not m.from_user:
        return
    start_time = time.time()
    user_id = m.from_user.id
    key = get_user_key(user_id)
    if not key:
        log_command(user_id, "/token", "", ok=False, err="no_access")
        await m.answer(T("no_access"), **MSG_KW)
        return
    valid, msg = is_key_valid_for_product(key)
    if not valid:
        log_command(user_id, "/token", "", ok=False, err="access_invalid")
        await m.answer(T("access_invalid", msg=msg), **MSG_KW)
        return

    chain = get_user_chain(user_id)
    if chain != "sol":
        await m.answer(T("chain_not_supported", chain=chain), **MSG_KW)
        return

    args = (m.text or "").split(maxsplit=1)
    if len(args) < 2:
        log_command(user_id, "/token", "", ok=False, err="usage")
        await m.answer(T("usage_token"), **MSG_KW)
        return

    mint = normalize_mint_arg(args[1])
    if not mint:
        log_command(user_id, "/token", args[1], ok=False, err="cant_detect_mint")
        await m.answer(T("cant_detect_mint"), **MSG_KW)
        return

    is_pro = is_pro_user(user_id)

    status = await m.answer(T("fetching_data", mint=mint), **MSG_KW)

    async with aiohttp.ClientSession() as session:
        extra = None
        security = None
        mkts = None
        birdeye_price_val = None
        
        if BIRDEYE_API_KEY:
            results = await asyncio.gather(
                birdeye_overview(session, mint),
                birdeye_token_security(session, mint),
                birdeye_markets(session, mint),
                birdeye_price(session, mint),
                return_exceptions=True
            )
            extra = results[0] if not isinstance(results[0], Exception) else None
            security = results[1] if not isinstance(results[1], Exception) else None
            mkts = results[2] if not isinstance(results[2], Exception) else None
            birdeye_price_val = results[3] if not isinstance(results[3], Exception) else None
        
        if not extra:
            print(f"[TOKEN] Birdeye failed, trying DexScreener for {mint[:8]}...")
            extra = await dexscreener_token(session, mint)
        
        if security and extra:
            extra.update(security)
        
        p = {
            "baseToken": {
                "symbol": (extra or {}).get("symbol") or "",
                "name": (extra or {}).get("name") or "",
                "address": mint
            },
            "priceUsd": birdeye_price_val or (extra or {}).get("price"),
            "liquidity": {"usd": (extra or {}).get("liquidity")},
            "fdv": (extra or {}).get("mc") or (extra or {}).get("marketCap") or (extra or {}).get("fdv"),
            "volume": {"h24": (extra or {}).get("v24hUSD") or (extra or {}).get("v24h") or (extra or {}).get("volume24h")},
            "pairCreatedAt": (extra or {}).get("createdAt") or (extra or {}).get("firstTradeAt") or (extra or {}).get("firstTradeUnixTime") or (extra or {}).get("pairCreatedAt"),
            "chainId": "solana",
        }

        if p.get("priceUsd") is None:
            jupiter_price_val = await jupiter_price(session, mint)
            if jupiter_price_val is not None:
                p["priceUsd"] = jupiter_price_val

    if not extra and p.get("priceUsd") is None:
        elapsed_ms = int((time.time() - start_time) * 1000)
        log_command(user_id, "/token", mint, ok=False, ms=elapsed_ms, err="token_not_found")
        await status.edit_text(T("token_not_found"), **MSG_KW)
        return

    _cleanup_token_sessions()
    _token_sessions[mint] = {
        "p": p,
        "extra": extra,
        "mkts": mkts,
        "ts": time.time()
    }

    text = build_summary_text(p, extra, mkts, is_pro)
    kb = token_keyboard(p, mode="summary")

    try:
        await status.edit_text(text, reply_markup=kb, **MSG_KW)
    except Exception:
        await m.answer(text, reply_markup=kb, **MSG_KW)

    elapsed_ms = int((time.time() - start_time) * 1000)
    log_command(user_id, "/token", mint, ok=True, ms=elapsed_ms)

@dp.message(Command("fav"))
async def fav_handler(m: Message):
    if not m.from_user:
        return
    user_id = m.from_user.id
    key = get_user_key(user_id)
    if not key:
        await m.answer(T("no_access"), **MSG_KW)
        return

    args = (m.text or "").split()
    if len(args) < 2:
        await m.answer(T("fav_usage"), **MSG_KW)
        return

    sub = args[1].lower()

    if sub == "add":
        if len(args) < 3:
            await m.answer(T("fav_add_usage"), **MSG_KW)
            return
        mint = normalize_mint_arg(args[2])
        if not mint:
            await m.answer(T("cant_detect_mint"), **MSG_KW)
            return
        add_favorite(user_id, mint)
        await m.answer(T("fav_added", mint=mint), **MSG_KW)
        return

    if sub == "list":
        favs = list_favorites(user_id)
        if not favs:
            await m.answer(T("fav_empty"), **MSG_KW)
            return
        fav_lines = "\n".join(f"• `{f}`" for f in favs)
        await m.answer(T("fav_list_header", favs=fav_lines), **MSG_KW)
        return

    await m.answer(T("unknown_subcommand"), **MSG_KW)

@dp.message(Command("alerts"))
async def alerts_handler(m: Message):
    if not m.from_user:
        return
    user_id = m.from_user.id
    key = get_user_key(user_id)
    if not key:
        await m.answer(T("no_access"), **MSG_KW)
        return
    
    args = (m.text or "").split()
    if len(args) < 2:
        conn = db()
        cur = conn.execute("SELECT thresholds FROM alerts WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        conn.close()
        
        if not row or not row[0]:
            await m.answer(T("alert_list_empty"), **MSG_KW)
            return
        
        try:
            thresholds = json.loads(row[0])
            alert_lines = []
            for mint, price in thresholds.items():
                alert_lines.append(f"• `{mint[:8]}...` at ${price}")
            await m.answer(T("alert_list_header", alerts="\n".join(alert_lines)), **MSG_KW)
        except Exception as e:
            print(f"[ALERTS] Parse error: {e}")
            await m.answer(T("alert_list_empty"), **MSG_KW)
        return
    
    sub = args[1].lower()
    
    if sub == "set":
        if len(args) < 4:
            await m.answer(T("alert_set_usage"), **MSG_KW)
            return
        
        mint = normalize_mint_arg(args[2])
        if not mint:
            await m.answer(T("cant_detect_mint"), **MSG_KW)
            return
        
        try:
            price = float(args[3])
        except ValueError:
            await m.answer(T("alert_invalid_price"), **MSG_KW)
            return
        
        conn = db()
        cur = conn.execute("SELECT thresholds FROM alerts WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        
        thresholds = {}
        if row and row[0]:
            try:
                thresholds = json.loads(row[0])
            except Exception:
                thresholds = {}
        
        thresholds[mint] = price
        ts = int(time.time())
        conn.execute(
            "INSERT OR REPLACE INTO alerts(user_id, thresholds, created_at) VALUES (?, ?, ?)",
            (user_id, json.dumps(thresholds), ts)
        )
        conn.commit()
        conn.close()
        
        await m.answer(T("alert_set_success", mint=mint[:8] + "...", price=price), **MSG_KW)
        return
    
    await m.answer(T("alerts_soon"), **MSG_KW)

@dp.callback_query(F.data.startswith("token:"))
async def token_callback_handler(cb: CallbackQuery):
    if not cb.from_user or not cb.message:
        return
    start_time = time.time()
    try:
        parts = cb.data.split(":")
        mint = parts[1]
        mode = parts[2] if len(parts) > 2 else "summary"
    except Exception:
        await cb.answer(T("bad_callback"))
        return

    user_id = cb.from_user.id
    is_pro = is_pro_user(user_id)

    _cleanup_token_sessions()
    sess = _token_sessions.get(mint)
    
    if sess:
        p = sess.get("p", {})
        extra = sess.get("extra")
        mkts = sess.get("mkts")
    else:
        extra = None
        mkts = None
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
            
            if not extra:
                extra = await dexscreener_token(session, mint)

            p = {
                "baseToken": {
                    "symbol": (extra or {}).get("symbol") or "",
                    "name": (extra or {}).get("name") or "",
                    "address": mint
                },
                "priceUsd": (extra or {}).get("price"),
                "liquidity": {"usd": (extra or {}).get("liquidity")},
                "fdv": (extra or {}).get("mc") or (extra or {}).get("marketCap"),
                "volume": {"h24": (extra or {}).get("v24hUSD") or (extra or {}).get("v24h")},
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

    helius_info = None
    topk_share = None
    if mode == "details":
        async with aiohttp.ClientSession() as session:
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
            text = build_details_text(p, extra, mkts, helius_info, topk_share, is_pro)
            kb = token_keyboard(p, mode="details")
        else:
            mint_active = helius_info.get("mintAuthority") is not None if helius_info else False
            freeze_active = helius_info.get("freezeAuthority") is not None if helius_info else False
            text = build_summary_text(p, extra, mkts, is_pro, mint_active, freeze_active, topk_share)
            kb = token_keyboard(p, mode="summary")

        await cb.message.edit_text(text, reply_markup=kb, **MSG_KW)
    except Exception:
        pass

    elapsed_ms = int((time.time() - start_time) * 1000)
    log_command(user_id, f"callback:token:{mode}", mint, ok=True, ms=elapsed_ms)

    await cb.answer()

@dp.callback_query(F.data.startswith("scan:session:"))
async def scan_cb_handler(cb: CallbackQuery):
    if not cb.from_user or not cb.message:
        return
    start_time = time.time()
    try:
        parts = cb.data.split(":")
        sid = parts[2]
        action = parts[3]
        idx = int(parts[4])
    except Exception:
        await cb.answer(T("bad_callback"))
        return

    user_id = cb.from_user.id
    is_pro = is_pro_user(user_id)

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
            text = build_details_text(p, extra, mkts, helius_info, topk_share, is_pro)
            kb = scan_nav_kb(sid, idx, mint, mode="details")
        else:
            text = build_summary_text(p, extra, mkts=None, is_pro=is_pro)
            kb = scan_nav_kb(sid, idx, mint, mode="summary")

    try:
        await cb.message.edit_text(text, reply_markup=kb, **MSG_KW)
    except Exception:
        await cb.message.answer(text, reply_markup=kb, **MSG_KW)

    elapsed_ms = int((time.time() - start_time) * 1000)
    log_command(user_id, f"callback:scan:{action}", f"sid={sid},idx={idx}", ok=True, ms=elapsed_ms)

    await cb.answer()

@dp.callback_query(F.data.startswith("fav:add:"))
async def fav_add_callback(cb: CallbackQuery):
    if not cb.from_user:
        return
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
    log_command(user_id, "callback:fav:add", mint, ok=True)
    await cb.answer(T("fav_added_callback", mint=mint))

@dp.callback_query(F.data.startswith("info:"))
async def info_callback(cb: CallbackQuery):
    if not cb.from_user:
        return
    parts = cb.data.split(":")
    if len(parts) < 2:
        await cb.answer(T("bad_callback"))
        return
    
    info_type = parts[1]
    if info_type == "fdv":
        await cb.answer(T("info_fdv"), show_alert=True)
    elif info_type == "lp":
        await cb.answer(T("info_lp"), show_alert=True)
    else:
        await cb.answer(T("bad_callback"))

@dp.message(Command("chain"))
async def chain_handler(m: Message):
    if not m.from_user:
        return
    user_id = m.from_user.id
    key = get_user_key(user_id)
    if not key:
        await m.answer(T("no_access"), **MSG_KW)
        return
    
    args = (m.text or "").split()
    if len(args) < 2:
        current = get_user_chain(user_id)
        chain_names = {"sol": "Solana", "eth": "Ethereum", "bsc": "BSC"}
        await m.answer(T("chain_current", chain=chain_names.get(current, current)), **MSG_KW)
        return
    
    chain = args[1].lower()
    if chain not in ["sol", "eth", "bsc"]:
        await m.answer(T("chain_invalid"), **MSG_KW)
        return
    
    set_user_chain(user_id, chain)
    chain_names = {"sol": "Solana", "eth": "Ethereum", "bsc": "BSC"}
    await m.answer(T("chain_set", chain=chain_names.get(chain, chain)), **MSG_KW)

@dp.message(F.text)
async def key_input_handler(m: Message):
    if not m.from_user:
        return
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
    
    print(f"[BOT] Starting with Birdeye API key: {BIRDEYE_API_KEY[:10] if BIRDEYE_API_KEY else 'MISSING'}...")
    print("[BOT] Single polling loop guaranteed - webhook deleted")
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
