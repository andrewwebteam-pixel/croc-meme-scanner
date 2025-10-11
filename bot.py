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
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton
from dotenv import load_dotenv
import aiohttp

# === Config ===
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_KEY = os.getenv("ADMIN_KEY", "ADMIN-ROOT-ACCESS")
DB_PATH = os.getenv("DB_PATH", "./keys.db")
PRODUCT = os.getenv("PRODUCT", "meme_scanner")
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "").strip()
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "").strip()

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
        "🔎 *Research Tools:*\n"
        "Use filters to find tokens by liquidity, volume, age, or holder distribution.\n\n"
        "🎯 *Find Token:*\n"
        "Get detailed info on any token by mint address or symbol.\n\n"
        "⭐ *Favorites:*\n"
        "Save and manage your favorite tokens.\n\n"
        "🔔 *Alerts:*\n"
        "Set price alerts for your tokens.\n\n"
        "💡 *Quick Commands:*\n"
        "/token <mint> — token details\n"
        "/my — your subscription tier\n"
        "/logout — remove your key"
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
    "fav_usage": "Usage: `/fav add <mint>`, `/fav list`, or `/fav del <mint>`",
    "fav_add_usage": "Usage: `/fav add <mint>`",
    "fav_added": "✅ {mint} added to favorites.",
    "fav_empty": "Your favorites list is empty.",
    "fav_list_header": "⭐ Your favorites:\n{favs}",
    "unknown_subcommand": "Unknown subcommand. Use `/fav add <mint>`, `/fav list`, or `/fav del <mint>`",
    "fav_del_usage": "Usage: `/fav del <mint>`",
    "fav_removed": "✅ {mint} removed from favorites.",
    "fav_not_found": "❌ {mint} not in your favorites.",
    "btn_fav_remove": "⭐ Remove from favorites",
    "fav_removed_callback": "Removed from favorites: {mint}",
    "awaiting_mint": "Send me a mint address, Birdeye/Solscan link, or SYMBOL (MINT) format:",
    "key_accepted": "✅ Key accepted. {msg}\nYou can now use /scan",
    "key_rejected": "⛔ {msg}\nPlease try again.",
    "fav_added_callback": "Added to favorites: {mint}",
    "btn_prev": "◀ Prev",
    "btn_next": "▶ Next",
    "btn_details": "ℹ️ Details",
    "btn_back": "◀ Back",
    "btn_birdeye": "🐦 Birdeye",
    "btn_solscan": "🔍 Solscan",
    "btn_buy": "💰 Trade",
    "btn_chart": "📈 Chart",
    "btn_fav_add": "⭐ Add to favorites",
    "btn_share": "📤 Share",
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
    "alert_list_empty": "You have no alerts yet.\n\nUse the buttons below to add or remove alerts.",
    "alert_list_header": "🔔 Your alerts:\n{alerts}\n\nManage your alerts using the buttons below:",
    "alert_invalid_price": "❌ Invalid price. Please use a number.",
    "no_pairs_all_sources": "😕 No fresh pairs available from any source.\nTry `/token <mint>` instead.",
    "chain_current": "Current chain: {chain}",
    "chain_set": "✅ Chain set to: {chain}",
    "chain_invalid": "❌ Invalid chain. Use: sol, eth, or bsc",
    "chain_usage": "Usage: `/chain <sol|eth|bsc>` or `/chain` to see current",
    "chain_not_supported": "⚠️ {chain} support coming soon. Only Solana (sol) is currently available.\nUse `/chain sol` to switch back.",
    "research_menu": "🔎 *Research Tools*\n\nUse filters to find tokens matching specific criteria:",
    "btn_filters": "🎛 Filters",
    "btn_scan": "🔎 Scan",
    "filters_menu": "Select a filter criterion (you can leave any filter empty):",
    "btn_filter_liq": "💧 Liquidity",
    "btn_filter_age": "⏰ Age",
    "btn_filter_vol": "📊 Volume 24h",
    "btn_clear_filters": "🗑️ Clear All Filters",
    "filter_liq_prompt": "Enter minimum liquidity (USD):\nExample: 10000\n\nSend /skip to leave empty",
    "filter_age_prompt": "Enter maximum token age:\nExamples: 1m, 1h, 1d, 1month, 1year\n\nSend /skip to leave empty",
    "filter_vol_prompt": "Enter minimum 24h volume (USD):\nExample: 50000\n\nSend /skip to leave empty",
    "filter_set": "✅ Filter set: {filter} = {value}",
    "filter_cleared": "✅ Filter cleared: {filter}",
    "filter_invalid": "❌ Invalid value. Try again or send /skip",
    "filters_cleared_all": "✅ All filters cleared",
    "filters_active": "📊 Active filters:\n{filters}",
    "filters_none": "No filters set",
    "no_pairs_filtered": "😕 No pairs matching your filters.\n\nTry adjusting your criteria or tap /Filters to modify.",
    "favorites_menu": "⭐ *Your Favorites*\n\n{favs}\n\nManage your saved tokens:",
    "btn_add_fav": "➕ Add Favorite",
    "btn_remove_fav": "➖ Remove Favorite",
    "alerts_menu": "🔔 *Price Alerts*\n\nTrack price changes for your favourite tokens:",
    "btn_add_alert": "➕ Add Alert",
    "btn_remove_alert": "➖ Remove Alert",
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

_awaiting_token_input: Dict[int, bool] = {}
_awaiting_fav_add: Dict[int, bool] = {}
_awaiting_fav_del: Dict[int, bool] = {}
_awaiting_alert_set: Dict[int, Dict[str, Any]] = {}
_awaiting_alert_del: Dict[int, bool] = {}
_awaiting_filter_liq: Dict[int, bool] = {}
_awaiting_filter_age: Dict[int, bool] = {}
_awaiting_filter_vol: Dict[int, bool] = {}

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

def main_menu_keyboard() -> ReplyKeyboardMarkup:
    """Create the main menu ReplyKeyboardMarkup with 3 columns and emojis."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔎 Research"), KeyboardButton(text="🎯 Find Token"), KeyboardButton(text="⭐ Favorites")],
            [KeyboardButton(text="🔔 Alerts"), KeyboardButton(text="🧾 My Tier"), KeyboardButton(text="❔ Help")],
            [KeyboardButton(text="🚪 Logout")],
        ],
        resize_keyboard=True,
        row_width=3
    )

def scan_nav_kb(sid: str, idx: int, mint: str, user_id: int) -> InlineKeyboardMarkup:
    prev_idx = max(idx - 1, 0)
    next_idx = idx + 1

    row_nav = [
        InlineKeyboardButton(text=T("btn_prev"), callback_data=f"scan:session:{sid}:idx:{prev_idx}"),
        InlineKeyboardButton(text=T("btn_next"), callback_data=f"scan:session:{sid}:idx:{next_idx}"),
    ]

    be_link = f"https://birdeye.so/token/{mint}?chain=solana"
    solscan_link = f"https://solscan.io/token/{mint}"
    dex_link = f"https://dexscreener.com/solana/{mint}"

    row_buy_chart = [
        InlineKeyboardButton(text=T("btn_buy"), url=dex_link),
        InlineKeyboardButton(text=T("btn_chart"), url=dex_link),
    ]
    row_links = [
        InlineKeyboardButton(text=T("btn_birdeye"), url=be_link),
        InlineKeyboardButton(text=T("btn_solscan"), url=solscan_link),
    ]
    
    is_fav = is_favorited(user_id, mint)
    if is_fav:
        fav_btn = InlineKeyboardButton(text=T("btn_fav_remove"), callback_data=f"fav:del:{mint}")
    else:
        fav_btn = InlineKeyboardButton(text=T("btn_fav_add"), callback_data=f"fav:add:{mint}")
    
    row_actions = [
        fav_btn,
        InlineKeyboardButton(text=T("btn_share"), switch_inline_query=mint),
    ]

    return InlineKeyboardMarkup(inline_keyboard=[row_nav, row_buy_chart, row_links, row_actions])

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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_filters (
            user_id INTEGER PRIMARY KEY,
            min_liq REAL,
            min_vol REAL,
            max_age_h REAL,
            min_top10 REAL
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

def is_favorited(user_id: int, mint: str) -> bool:
    """Check if a token is in user's favorites"""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM favorites WHERE user_id = ? AND mint = ? LIMIT 1",
            (user_id, mint),
        ).fetchone()
    return row is not None

def get_user_chain(user_id: int) -> str:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT chain FROM user_chain WHERE user_id = ?", (user_id,)).fetchone()
    return row[0] if row else "sol"

def set_user_chain(user_id: int, chain: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("INSERT OR REPLACE INTO user_chain (user_id, chain) VALUES (?, ?)", (user_id, chain))

def get_user_filters(user_id: int) -> Dict[str, Any]:
    """Get user's scan filters"""
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT min_liq, min_vol, max_age_h, min_top10 FROM user_filters WHERE user_id = ?", (user_id,)).fetchone()
    if not row:
        return {}
    return {
        "min_liq": row[0],
        "min_vol": row[1],
        "max_age_h": row[2],
        "min_top10": row[3]
    }

def set_user_filter(user_id: int, filter_key: str, value: Optional[float]):
    """Set a specific filter value for user"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("INSERT OR IGNORE INTO user_filters (user_id) VALUES (?)", (user_id,))
        conn.execute(f"UPDATE user_filters SET {filter_key} = ? WHERE user_id = ?", (value, user_id))

def clear_user_filters(user_id: int):
    """Clear all filters for user"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM user_filters WHERE user_id = ?", (user_id,))

def get_last_scan_ts(user_id: int) -> int:
    conn = db()
    cur = conn.execute("SELECT last_scan_ts FROM user_throttle WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else 0

def apply_filters_to_pairs(pairs: List[Dict[str, Any]], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Apply user filters to pairs list, sort by creation time, and limit to 8 pairs"""
    if not filters:
        # No filters, just sort and limit
        sorted_pairs = sorted(pairs, key=lambda x: x.get("pairCreatedAt", 0), reverse=True)
        return sorted_pairs[:8]
    
    filtered = []
    now = datetime.now(timezone.utc)
    
    for p in pairs:
        # Check liquidity filter (min_liq)
        min_liq = filters.get("min_liq")
        if min_liq is not None:
            liq = (p.get("liquidity") or {}).get("usd")
            if liq is None or liq < min_liq:
                continue
        
        # Check volume filter (min_vol)
        min_vol = filters.get("min_vol")
        if min_vol is not None:
            vol = (p.get("volume") or {}).get("h24")
            if vol is None or vol < min_vol:
                continue
        
        # Check age filter (max_age_h)
        max_age_h = filters.get("max_age_h")
        if max_age_h is not None:
            created = p.get("pairCreatedAt")
            if created:
                created_dt = from_unix_ms(created) if isinstance(created, (int, float)) else None
                if created_dt:
                    age_h = (now - created_dt).total_seconds() / 3600
                    if age_h > max_age_h:
                        continue
                else:
                    # If we can't parse creation time and filter is set, skip this pair
                    continue
            else:
                # No creation time and filter is set, skip
                continue
        
        # Note: top10 filter (min_top10) requires fetching security data for ALL pairs
        # This is not cost-effective to fetch during scan, so it's not applied here
        # Individual token displays will show holder data when available
        
        filtered.append(p)
    
    # Sort by creation time (newest first) and limit to 8 pairs
    sorted_filtered = sorted(filtered, key=lambda x: x.get("pairCreatedAt", 0), reverse=True)
    return sorted_filtered[:8]

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


async def fetch_latest_sol_pairs(limit: int = 8) -> List[Dict[str, Any]]:
    """
    Fetch fresh Solana pairs using Birdeye API only.
    Uses /defi/v2/markets with liquidity sorting to get recent active pairs.
    """
    if (_scan_cache["ts"] + SCAN_CACHE_TTL) > time.time() and _scan_cache["pairs"]:
        return _scan_cache["pairs"][:limit]

    pairs = []

    if not BIRDEYE_API_KEY:
        print("[SCAN] Birdeye: BIRDEYE_API_KEY is empty -> cannot scan")
        return []
    
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
                print(f"[SCAN] Birdeye /defi/v2/markets status={status}")
                if status in [400, 401, 403, 429]:
                    print(f"[SCAN] /defi/v2/markets returned {status} - check API key or plan limits")
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
    """
    Fetch token security info from Birdeye (mint authority, freeze authority, top holders, etc.)
    Returns data with mintAuthority, freezeAuthority, top10HolderPercent fields.
    """
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

async def fetch_pair_data(session: aiohttp.ClientSession, mint: str) -> Optional[Dict[str, Any]]:
    """
    Build basic pair dict using Birdeye API only.
    Returns dict with: baseToken {symbol, name, address}, priceUsd, liquidity, fdv, volume, pairCreatedAt
    """
    if not BIRDEYE_API_KEY:
        print(f"[FETCH_PAIR] No Birdeye API key for {mint[:8]}...")
        return None
    
    overview = await birdeye_overview(session, mint)
    if overview:
        return {
            "baseToken": {
                "symbol": overview.get("symbol", ""),
                "name": overview.get("name", ""),
                "address": mint
            },
            "priceUsd": overview.get("price"),
            "liquidity": {"usd": overview.get("liquidity")},
            "fdv": overview.get("mc") or overview.get("marketCap"),
            "volume": {"h24": overview.get("v24hUSD") or overview.get("v24h")},
            "pairCreatedAt": overview.get("createdAt") or overview.get("firstTradeUnixTime"),
            "chainId": "solana",
        }
    
    return None


def extract_holders(data: Dict[str, Any]) -> Optional[int]:
    for k in ("holders", "holder", "holder_count", "holdersCount", "uniqueHolders"):
        v = data.get(k)
        if isinstance(v, (int, float)) and v >= 0:
            return int(v)
    return None

def extract_lp_lock_ratio(data: Dict[str, Any]) -> Optional[float]:
    """
    Extract LP lock ratio from Birdeye data.
    
    NOTE: LP lock data requires Birdeye Pro API access. Without Pro tier,
    this field typically returns None and will show "—" or be hidden for Free users.
    """
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
    security_info: Optional[Dict[str, Any]],
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
    if security_info:
        mint_txt = format_authority(security_info.get('mintAuthority'))
        freeze_txt = format_authority(security_info.get('freezeAuthority'))
        mint_active = (security_info.get('mintAuthority') is not None)
        freeze_active = (security_info.get('freezeAuthority') is not None)
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

def build_full_token_text(
    p: Dict[str, Any],
    extra: Optional[Dict[str, Any]],
    mkts: Optional[List[Dict[str, Any]]],
    security_info: Optional[Dict[str, Any]],
    topk_share: Optional[float],
    is_pro: bool
) -> str:
    """Build comprehensive single-card token text with all details at once."""
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
    if security_info:
        mint_txt = format_authority(security_info.get('mintAuthority'))
        freeze_txt = format_authority(security_info.get('freezeAuthority'))
        mint_active = (security_info.get('mintAuthority') is not None)
        freeze_active = (security_info.get('freezeAuthority') is not None)
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

def token_keyboard(p: Dict[str, Any], user_id: Optional[int] = None) -> InlineKeyboardMarkup:
    mint = (p.get("baseToken") or {}).get("address")
    
    be_link = f"https://birdeye.so/token/{mint}?chain=solana"
    solscan_link = f"https://solscan.io/token/{mint}"
    dex_link = f"https://dexscreener.com/solana/{mint}"

    row_buy_chart = [
        InlineKeyboardButton(text=T("btn_buy"), url=dex_link),
        InlineKeyboardButton(text=T("btn_chart"), url=dex_link),
    ]
    row_explorers = [
        InlineKeyboardButton(text=T("btn_birdeye"), url=be_link),
        InlineKeyboardButton(text=T("btn_solscan"), url=solscan_link),
    ]
    
    is_favorited = False
    if user_id and mint:
        conn = db()
        cur = conn.execute("SELECT 1 FROM favorites WHERE user_id = ? AND mint = ?", (user_id, mint))
        is_favorited = cur.fetchone() is not None
        conn.close()
    
    if is_favorited:
        row_actions = [
            InlineKeyboardButton(text=T("btn_fav_remove"), callback_data=f"fav:del:{mint}"),
            InlineKeyboardButton(text=T("btn_share"), switch_inline_query=mint),
        ]
    else:
        row_actions = [
            InlineKeyboardButton(text=T("btn_fav_add"), callback_data=f"fav:add:{mint}"),
            InlineKeyboardButton(text=T("btn_share"), switch_inline_query=mint),
        ]
    
    row_info = [
        InlineKeyboardButton(text="ℹ️ About FDV", callback_data="info:fdv"),
        InlineKeyboardButton(text="ℹ️ About LP", callback_data="info:lp"),
    ]

    return InlineKeyboardMarkup(
        inline_keyboard=[row_buy_chart, row_explorers, row_actions, row_info]
    )


bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher()

@dp.message(Command("start"))
async def start_handler(m: Message):
    if not m.from_user:
        return
    key = get_user_key(m.from_user.id)
    if key:
        await m.answer(T("start", product=PRODUCT.replace("_", "\\_")), reply_markup=main_menu_keyboard(), **MSG_KW)
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

    pairs = await fetch_latest_sol_pairs(limit=20)

    if not pairs:
        log_command(user_id, "/scan", "", ok=False, err="no_pairs")
        await status.edit_text(T("no_pairs_all_sources"), **MSG_KW)
        return
    
    # Apply user filters
    user_filters = get_user_filters(user_id)
    if user_filters:
        pairs = apply_filters_to_pairs(pairs, user_filters)
        if not pairs:
            log_command(user_id, "/scan", "", ok=False, err="no_pairs_filtered")
            await status.edit_text(T("no_pairs_filtered"), **MSG_KW)
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
        mkts = None
        security_info = None
        topk_share = None
        
        if BIRDEYE_API_KEY and mint:
            try:
                extra, mkts, security_info = await asyncio.gather(
                    birdeye_overview(session, mint),
                    birdeye_markets(session, mint),
                    birdeye_token_security(session, mint),
                )
            except Exception:
                extra, mkts, security_info = None, None, None
        
        if security_info:
            topk_share = security_info.get("top10HolderPercent") or security_info.get("top10_holder_percent")

    text = build_full_token_text(p, extra, mkts, security_info, topk_share, is_pro)
    kb = scan_nav_kb(sid, 0, mint, user_id)

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
        _awaiting_token_input[user_id] = True
        await m.answer(T("awaiting_mint"), **MSG_KW)
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
        security_info = None
        mkts = None
        birdeye_price_val = None
        topk_share = None
        
        if BIRDEYE_API_KEY:
            results = await asyncio.gather(
                birdeye_overview(session, mint),
                birdeye_token_security(session, mint),
                birdeye_markets(session, mint),
                birdeye_price(session, mint),
                return_exceptions=True
            )
            extra = results[0] if not isinstance(results[0], Exception) else None
            security_info = results[1] if not isinstance(results[1], Exception) else None
            mkts = results[2] if not isinstance(results[2], Exception) else None
            birdeye_price_val = results[3] if not isinstance(results[3], Exception) else None
        
        if not extra:
            print(f"[TOKEN] Birdeye failed for {mint[:8]}...")
        
        if security_info and extra:
            extra.update(security_info)
        
        if security_info:
            topk_share = security_info.get("top10HolderPercent") or security_info.get("top10_holder_percent")
        
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
        "security_info": security_info,
        "topk_share": topk_share,
        "ts": time.time()
    }

    text = build_full_token_text(p, extra, mkts, security_info, topk_share, is_pro)
    kb = token_keyboard(p, user_id=user_id)

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
    
    # Handle button taps
    if m.text == "📜 My Favs":
        sub = "list"
    elif len(args) < 2:
        await m.answer(T("fav_usage"), **MSG_KW)
        return
    else:
        sub = args[1].lower()

    if sub == "add":
        if len(args) >= 3:
            mint = normalize_mint_arg(args[2])
            if not mint:
                await m.answer(T("cant_detect_mint"), **MSG_KW)
                return
            add_favorite(user_id, mint)
            await m.answer(T("fav_added", mint=mint), **MSG_KW)
            return
        else:
            _awaiting_fav_add[user_id] = True
            await m.answer(T("awaiting_mint"), **MSG_KW)
            return

    if sub == "list":
        favs = list_favorites(user_id)
        
        # Build inline keyboard with Add/Remove buttons
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=T("btn_add_fav"), callback_data="favmenu:add")],
            [InlineKeyboardButton(text=T("btn_remove_fav"), callback_data="favmenu:remove")]
        ])
        
        if not favs:
            await m.answer(T("fav_empty"), reply_markup=kb, **MSG_KW)
            return
        fav_lines = "\n".join(f"• `{f}`" for f in favs)
        await m.answer(T("fav_list_header", favs=fav_lines), reply_markup=kb, **MSG_KW)
        return
    
    if sub == "del":
        if len(args) >= 3:
            mint = normalize_mint_arg(args[2])
            if not mint:
                await m.answer(T("cant_detect_mint"), **MSG_KW)
                return
            conn = db()
            cur = conn.execute("DELETE FROM favorites WHERE user_id = ? AND mint = ?", (user_id, mint))
            deleted = cur.rowcount > 0
            conn.commit()
            conn.close()
            if deleted:
                await m.answer(T("fav_removed", mint=mint), **MSG_KW)
            else:
                await m.answer(T("fav_not_found", mint=mint), **MSG_KW)
            return
        else:
            _awaiting_fav_del[user_id] = True
            await m.answer(T("awaiting_mint"), **MSG_KW)
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
    
    # Handle button tap - show alert list with inline buttons
    if m.text == "🔔 Alerts":
        args = ["/alerts"]  # Treat as command with no subcommand
    
    if len(args) < 2:
        conn = db()
        cur = conn.execute("SELECT thresholds FROM alerts WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        conn.close()
        
        # Build inline keyboard with Add/Remove buttons
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=T("btn_add_alert"), callback_data="alertmenu:add")],
            [InlineKeyboardButton(text=T("btn_remove_alert"), callback_data="alertmenu:remove")]
        ])
        
        if not row or not row[0]:
            await m.answer(T("alert_list_empty"), reply_markup=kb, **MSG_KW)
            return
        
        try:
            thresholds = json.loads(row[0])
            alert_lines = []
            for mint, price in thresholds.items():
                alert_lines.append(f"• `{mint[:8]}...` — ${price}")
            await m.answer(T("alert_list_header", alerts="\n".join(alert_lines)), reply_markup=kb, **MSG_KW)
        except Exception as e:
            print(f"[ALERTS] Parse error: {e}")
            await m.answer(T("alert_list_empty"), reply_markup=kb, **MSG_KW)
        return
    
    sub = args[1].lower()
    
    if sub == "set":
        if len(args) >= 4:
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
        else:
            _awaiting_alert_set[user_id] = {"step": "mint"}
            await m.answer(T("awaiting_mint"), **MSG_KW)
            return
    
    if sub == "del":
        if len(args) >= 3:
            mint = normalize_mint_arg(args[2])
            if not mint:
                await m.answer(T("cant_detect_mint"), **MSG_KW)
                return
            
            conn = db()
            cur = conn.execute("SELECT thresholds FROM alerts WHERE user_id = ?", (user_id,))
            row = cur.fetchone()
            
            if not row or not row[0]:
                await m.answer(f"❌ No alerts found for {mint[:8]}...", **MSG_KW)
                conn.close()
                return
            
            try:
                thresholds = json.loads(row[0])
            except Exception:
                thresholds = {}
            
            if mint in thresholds:
                del thresholds[mint]
                if thresholds:
                    conn.execute(
                        "UPDATE alerts SET thresholds = ? WHERE user_id = ?",
                        (json.dumps(thresholds), user_id)
                    )
                else:
                    conn.execute("DELETE FROM alerts WHERE user_id = ?", (user_id,))
                conn.commit()
                await m.answer(f"✅ {mint} removed from alerts.", **MSG_KW)
            else:
                await m.answer(f"❌ No alert found for {mint[:8]}...", **MSG_KW)
            
            conn.close()
            return
        else:
            _awaiting_alert_del[user_id] = True
            await m.answer(T("awaiting_mint"), **MSG_KW)
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
        security_info = sess.get("security_info")
        topk_share = sess.get("topk_share")
    else:
        extra = None
        mkts = None
        security_info = None
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
                try:
                    security_info = await birdeye_token_security(session, mint)
                except Exception:
                    security_info = None
            

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
            
            if security_info:
                topk_share = security_info.get("top10HolderPercent") or security_info.get("top10_holder_percent")

    try:
        if mode == "details":
            text = build_details_text(p, extra, mkts, security_info, topk_share, is_pro)
            kb = token_keyboard(p, mode="details")
        else:
            mint_active = security_info.get("mintAuthority") is not None if security_info else False
            freeze_active = security_info.get("freezeAuthority") is not None if security_info else False
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

    # Check if user is trying to navigate beyond bounds
    if idx < 0:
        idx = 0
        await cb.answer("📍 Already at first pair")
    elif idx >= len(pairs):
        idx = len(pairs) - 1
        await cb.answer("📍 No more pairs available")
        # Keep session alive, just show message
        p = pairs[idx]
        mint = (p.get("baseToken") or {}).get("address", "")
        
        async with aiohttp.ClientSession() as session:
            extra = None
            mkts = None
            security_info = None
            topk_share = None
            
            if BIRDEYE_API_KEY and mint:
                try:
                    extra, mkts, security_info = await asyncio.gather(
                        birdeye_overview(session, mint),
                        birdeye_markets(session, mint),
                        birdeye_token_security(session, mint),
                    )
                except Exception:
                    extra, mkts, security_info = None, None, None
            
            if security_info:
                topk_share = security_info.get("top10HolderPercent") or security_info.get("top10_holder_percent")

        text = build_full_token_text(p, extra, mkts, security_info, topk_share, is_pro)
        kb = scan_nav_kb(sid, idx, mint, user_id)

        try:
            await cb.message.edit_text(text, reply_markup=kb, **MSG_KW)
        except Exception:
            pass

        elapsed_ms = int((time.time() - start_time) * 1000)
        log_command(user_id, f"callback:scan:{action}", f"sid={sid},idx={idx}", ok=True, ms=elapsed_ms)
        return

    p = pairs[idx]
    mint = (p.get("baseToken") or {}).get("address", "")

    async with aiohttp.ClientSession() as session:
        extra = None
        mkts = None
        security_info = None
        topk_share = None
        
        if BIRDEYE_API_KEY and mint:
            try:
                extra, mkts, security_info = await asyncio.gather(
                    birdeye_overview(session, mint),
                    birdeye_markets(session, mint),
                    birdeye_token_security(session, mint),
                )
            except Exception:
                extra, mkts, security_info = None, None, None
        
        if security_info:
            topk_share = security_info.get("top10HolderPercent") or security_info.get("top10_holder_percent")

    text = build_full_token_text(p, extra, mkts, security_info, topk_share, is_pro)
    kb = scan_nav_kb(sid, idx, mint, user_id)

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
    
    if cb.message and cb.message.reply_markup:
        is_scan_context = any(
            "scan:session:" in btn.callback_data
            for row in cb.message.reply_markup.inline_keyboard
            for btn in row
            if hasattr(btn, 'callback_data') and btn.callback_data
        )
        
        if is_scan_context:
            for row in cb.message.reply_markup.inline_keyboard:
                for btn in row:
                    if hasattr(btn, 'callback_data') and btn.callback_data and "scan:session:" in btn.callback_data:
                        cb_parts = btn.callback_data.split(":")
                        if len(cb_parts) >= 5:
                            sid = cb_parts[2]
                            idx = int(cb_parts[4])
                            kb = scan_nav_kb(sid, idx, mint, user_id)
                            try:
                                await cb.message.edit_reply_markup(reply_markup=kb)
                            except Exception:
                                pass
                            break
                break
        else:
            p = {"baseToken": {"address": mint}}
            kb = token_keyboard(p, user_id=user_id)
            try:
                await cb.message.edit_reply_markup(reply_markup=kb)
            except Exception:
                pass
    
    await cb.answer(T("fav_added_callback", mint=mint))

@dp.callback_query(F.data.startswith("fav:del:"))
async def fav_del_callback(cb: CallbackQuery):
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

    conn = db()
    cur = conn.execute("DELETE FROM favorites WHERE user_id = ? AND mint = ?", (user_id, mint))
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    
    log_command(user_id, "callback:fav:del", mint, ok=deleted)
    
    if cb.message and deleted and cb.message.reply_markup:
        is_scan_context = any(
            "scan:session:" in btn.callback_data
            for row in cb.message.reply_markup.inline_keyboard
            for btn in row
            if hasattr(btn, 'callback_data') and btn.callback_data
        )
        
        if is_scan_context:
            for row in cb.message.reply_markup.inline_keyboard:
                for btn in row:
                    if hasattr(btn, 'callback_data') and btn.callback_data and "scan:session:" in btn.callback_data:
                        cb_parts = btn.callback_data.split(":")
                        if len(cb_parts) >= 5:
                            sid = cb_parts[2]
                            idx = int(cb_parts[4])
                            kb = scan_nav_kb(sid, idx, mint, user_id)
                            try:
                                await cb.message.edit_reply_markup(reply_markup=kb)
                            except Exception:
                                pass
                            break
                break
        else:
            p = {"baseToken": {"address": mint}}
            kb = token_keyboard(p, user_id=user_id)
            try:
                await cb.message.edit_reply_markup(reply_markup=kb)
            except Exception:
                pass
    
    if deleted:
        await cb.answer(T("fav_removed_callback", mint=mint))
    else:
        await cb.answer(T("fav_not_found", mint=mint))

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

@dp.message(Command("research"))
async def research_handler(m: Message):
    """Show Research menu with Filters and Scan inline buttons"""
    if not m.from_user:
        return
    user_id = m.from_user.id
    key = get_user_key(user_id)
    if not key:
        await m.answer(T("no_access"), **MSG_KW)
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=T("btn_filters"), callback_data="researchmenu:filters")],
        [InlineKeyboardButton(text=T("btn_scan"), callback_data="researchmenu:scan")]
    ])
    await m.answer(T("research_menu"), reply_markup=kb, **MSG_KW)

@dp.callback_query(F.data.startswith("researchmenu:"))
async def research_menu_callback_handler(cb: CallbackQuery):
    """Handle research menu inline button callbacks"""
    if not cb.from_user or not cb.message:
        return
    
    user_id = cb.from_user.id
    key = get_user_key(user_id)
    if not key:
        await cb.message.answer(T("no_access"), **MSG_KW)
        await cb.answer()
        return
    
    action = cb.data.split(":")[1]
    
    if action == "filters":
        # Inline filters logic
        filters = get_user_filters(user_id)
        filter_text = []
        if filters.get("min_liq"):
            filter_text.append(f"💧 Liquidity: ≥ ${filters['min_liq']:,.0f}")
        if filters.get("max_age_h"):
            filter_text.append(f"⏰ Age: ≤ {filters['max_age_h']}h")
        if filters.get("min_vol"):
            filter_text.append(f"📊 Volume: ≥ ${filters['min_vol']:,.0f}")
        
        if filter_text:
            active_filters = T("filters_active", filters="\n".join(filter_text))
        else:
            active_filters = T("filters_none")
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=T("btn_filter_liq"), callback_data="filter:liq")],
            [InlineKeyboardButton(text=T("btn_filter_age"), callback_data="filter:age")],
            [InlineKeyboardButton(text=T("btn_filter_vol"), callback_data="filter:vol")],
            [InlineKeyboardButton(text=T("btn_clear_filters"), callback_data="filter:clear")]
        ])
        
        await cb.message.answer(f"{active_filters}\n\n{T('filters_menu')}", reply_markup=kb, **MSG_KW)
        
    elif action == "scan":
        # Inline scan logic
        start_time = time.time()
        
        chain = get_user_chain(user_id)
        if chain != "sol":
            await cb.message.answer(T("chain_not_supported", chain=chain), **MSG_KW)
            await cb.answer()
            return
        
        is_pro = is_pro_user(user_id)
        cooldown = SCAN_COOLDOWN_PRO_SEC if is_pro else SCAN_COOLDOWN_SEC
        
        now_ts = int(time.time())
        last_ts = get_last_scan_ts(user_id)
        if (now_ts - last_ts) < cooldown:
            remaining = cooldown - (now_ts - last_ts)
            log_command(user_id, "/scan", "", ok=False, err="cooldown")
            await cb.message.answer(T("cooldown", remaining=remaining), **MSG_KW)
            await cb.answer()
            return
        
        set_last_scan_ts(user_id, now_ts)
        
        status = await cb.message.answer(T("scan_progress", i=0, n=0), **MSG_KW)
        
        pairs = await fetch_latest_sol_pairs(limit=20)
        
        if not pairs:
            log_command(user_id, "/scan", "", ok=False, err="no_pairs")
            await status.edit_text(T("no_pairs_all_sources"), **MSG_KW)
            await cb.answer()
            return
        
        # Apply user filters
        user_filters = get_user_filters(user_id)
        if user_filters:
            pairs = apply_filters_to_pairs(pairs, user_filters)
            if not pairs:
                log_command(user_id, "/scan", "", ok=False, err="no_pairs_filtered")
                await status.edit_text(T("no_pairs_filtered"), **MSG_KW)
                await cb.answer()
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
            mkts = None
            security_info = None
            topk_share = None
            
            if BIRDEYE_API_KEY and mint:
                try:
                    extra, mkts, security_info = await asyncio.gather(
                        birdeye_overview(session, mint),
                        birdeye_markets(session, mint),
                        birdeye_token_security(session, mint),
                    )
                except Exception:
                    extra, mkts, security_info = None, None, None
            
            if security_info:
                topk_share = security_info.get("top10HolderPercent") or security_info.get("top10_holder_percent")
        
        text = build_full_token_text(p, extra, mkts, security_info, topk_share, is_pro)
        kb = scan_nav_kb(sid, 0, mint, user_id)
        
        try:
            await status.edit_text(text, reply_markup=kb, **MSG_KW)
        except Exception:
            await cb.message.answer(text, reply_markup=kb, **MSG_KW)
        
        elapsed_ms = int((time.time() - start_time) * 1000)
        log_command(user_id, "/scan", f"sid={sid}", ok=True, ms=elapsed_ms)
    
    await cb.answer()

@dp.message(Command("filters"))
async def filters_handler(m: Message):
    """Show filter selection inline keyboard"""
    if not m.from_user:
        return
    user_id = m.from_user.id
    key = get_user_key(user_id)
    if not key:
        await m.answer(T("no_access"), **MSG_KW)
        return
    
    filters = get_user_filters(user_id)
    filter_text = []
    if filters.get("min_liq"):
        filter_text.append(f"💧 Liquidity: ≥ ${filters['min_liq']:,.0f}")
    if filters.get("max_age_h"):
        filter_text.append(f"⏰ Age: ≤ {filters['max_age_h']}h")
    if filters.get("min_vol"):
        filter_text.append(f"📊 Volume: ≥ ${filters['min_vol']:,.0f}")
    
    if filter_text:
        active_filters = T("filters_active", filters="\n".join(filter_text))
    else:
        active_filters = T("filters_none")
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=T("btn_filter_liq"), callback_data="filter:liq")],
        [InlineKeyboardButton(text=T("btn_filter_age"), callback_data="filter:age")],
        [InlineKeyboardButton(text=T("btn_filter_vol"), callback_data="filter:vol")],
        [InlineKeyboardButton(text=T("btn_clear_filters"), callback_data="filter:clear")]
    ])
    
    await m.answer(f"{active_filters}\n\n{T('filters_menu')}", reply_markup=kb, **MSG_KW)

@dp.callback_query(F.data.startswith("filter:"))
async def filter_callback_handler(cb: CallbackQuery):
    """Handle filter selection callbacks"""
    if not cb.from_user or not cb.message:
        return
    
    user_id = cb.from_user.id
    action = cb.data.split(":")[1]
    
    if action == "clear":
        clear_user_filters(user_id)
        await cb.answer(T("filters_cleared_all"))
        await cb.message.edit_text(f"{T('filters_none')}\n\n{T('filters_menu')}", **MSG_KW)
        return
    
    if action == "liq":
        _awaiting_filter_liq[user_id] = True
        await cb.message.answer(T("filter_liq_prompt"), **MSG_KW)
    elif action == "age":
        _awaiting_filter_age[user_id] = True
        await cb.message.answer(T("filter_age_prompt"), **MSG_KW)
    elif action == "vol":
        _awaiting_filter_vol[user_id] = True
        await cb.message.answer(T("filter_vol_prompt"), **MSG_KW)
    
    await cb.answer()

@dp.message(Command("favorites"))
async def favorites_menu_handler(m: Message):
    """Show favorites list with inline Add/Remove buttons"""
    if not m.from_user:
        return
    user_id = m.from_user.id
    key = get_user_key(user_id)
    if not key:
        await m.answer(T("no_access"), **MSG_KW)
        return
    
    favs = list_favorites(user_id)
    if favs:
        fav_lines = "\n".join(f"• `{f}`" for f in favs)
    else:
        fav_lines = T("fav_empty")
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=T("btn_add_fav"), callback_data="favmenu:add")],
        [InlineKeyboardButton(text=T("btn_remove_fav"), callback_data="favmenu:remove")]
    ])
    
    await m.answer(T("favorites_menu", favs=fav_lines), reply_markup=kb, **MSG_KW)

@dp.callback_query(F.data.startswith("favmenu:"))
async def favmenu_callback_handler(cb: CallbackQuery):
    """Handle favorites menu callbacks"""
    if not cb.from_user or not cb.message:
        return
    
    user_id = cb.from_user.id
    action = cb.data.split(":")[1]
    
    if action == "add":
        _awaiting_fav_add[user_id] = True
        await cb.message.answer(T("awaiting_mint"), **MSG_KW)
    elif action == "remove":
        _awaiting_fav_del[user_id] = True
        await cb.message.answer(T("awaiting_mint"), **MSG_KW)
    
    await cb.answer()

@dp.message(Command("alertsmenu"))
async def alerts_menu_handler(m: Message):
    """Show alerts menu with current alerts list and inline Add/Remove buttons"""
    if not m.from_user:
        return
    user_id = m.from_user.id
    key = get_user_key(user_id)
    if not key:
        await m.answer(T("no_access"), **MSG_KW)
        return
    
    # Fetch current alerts
    conn = db()
    cur = conn.execute("SELECT thresholds FROM alerts WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    
    # Build inline keyboard with Add/Remove buttons
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=T("btn_add_alert"), callback_data="alertmenu:add")],
        [InlineKeyboardButton(text=T("btn_remove_alert"), callback_data="alertmenu:remove")]
    ])
    
    # Display alerts list or empty message
    if not row or not row[0]:
        await m.answer(T("alert_list_empty"), reply_markup=kb, **MSG_KW)
        return
    
    try:
        thresholds = json.loads(row[0])
        if not thresholds:
            await m.answer(T("alert_list_empty"), reply_markup=kb, **MSG_KW)
            return
        
        alert_lines = []
        for mint, price in thresholds.items():
            alert_lines.append(f"• `{mint[:8]}...` — ${price}")
        await m.answer(T("alert_list_header", alerts="\n".join(alert_lines)), reply_markup=kb, **MSG_KW)
    except Exception as e:
        print(f"[ALERTS] Parse error in alerts_menu_handler: {e}")
        await m.answer(T("alert_list_empty"), reply_markup=kb, **MSG_KW)

@dp.callback_query(F.data.startswith("alertmenu:"))
async def alertmenu_callback_handler(cb: CallbackQuery):
    """Handle alerts menu callbacks"""
    if not cb.from_user or not cb.message:
        return
    
    user_id = cb.from_user.id
    action = cb.data.split(":")[1]
    
    if action == "add":
        _awaiting_alert_set[user_id] = {"step": "mint"}
        await cb.message.answer(T("awaiting_mint"), **MSG_KW)
    elif action == "remove":
        _awaiting_alert_del[user_id] = True
        await cb.message.answer(T("awaiting_mint"), **MSG_KW)
    
    await cb.answer()

@dp.message(F.text)
async def text_input_handler(m: Message):
    if not m.from_user:
        return
    user_id = m.from_user.id
    text_input = (m.text or "").strip()
    
    if user_id in _awaiting_fav_add and _awaiting_fav_add[user_id]:
        _awaiting_fav_add[user_id] = False
        mint = normalize_mint_arg(text_input)
        if not mint:
            await m.answer(T("cant_detect_mint"), **MSG_KW)
            return
        add_favorite(user_id, mint)
        await m.answer(T("fav_added", mint=mint), **MSG_KW)
        return
    
    if user_id in _awaiting_fav_del and _awaiting_fav_del[user_id]:
        _awaiting_fav_del[user_id] = False
        mint = normalize_mint_arg(text_input)
        if not mint:
            await m.answer(T("cant_detect_mint"), **MSG_KW)
            return
        conn = db()
        cur = conn.execute("DELETE FROM favorites WHERE user_id = ? AND mint = ?", (user_id, mint))
        deleted = cur.rowcount > 0
        conn.commit()
        conn.close()
        if deleted:
            await m.answer(T("fav_removed", mint=mint), **MSG_KW)
        else:
            await m.answer(T("fav_not_found", mint=mint), **MSG_KW)
        return
    
    if user_id in _awaiting_alert_set and _awaiting_alert_set[user_id]:
        state = _awaiting_alert_set[user_id]
        if state.get("step") == "mint":
            mint = normalize_mint_arg(text_input)
            if not mint:
                await m.answer(T("cant_detect_mint"), **MSG_KW)
                _awaiting_alert_set.pop(user_id, None)
                return
            _awaiting_alert_set[user_id] = {"step": "price", "mint": mint}
            await m.answer("Now send me the target price (number only):", **MSG_KW)
            return
        elif state.get("step") == "price":
            try:
                price = float(text_input)
            except ValueError:
                await m.answer(T("alert_invalid_price"), **MSG_KW)
                _awaiting_alert_set.pop(user_id, None)
                return
            
            mint = state.get("mint")
            _awaiting_alert_set.pop(user_id, None)
            
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
    
    if user_id in _awaiting_alert_del and _awaiting_alert_del[user_id]:
        _awaiting_alert_del[user_id] = False
        mint = normalize_mint_arg(text_input)
        if not mint:
            await m.answer(T("cant_detect_mint"), **MSG_KW)
            return
        
        conn = db()
        cur = conn.execute("SELECT thresholds FROM alerts WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        
        if not row or not row[0]:
            await m.answer(f"❌ No alerts found for {mint[:8]}...", **MSG_KW)
            conn.close()
            return
        
        try:
            thresholds = json.loads(row[0])
        except Exception:
            thresholds = {}
        
        if mint in thresholds:
            del thresholds[mint]
            if thresholds:
                conn.execute(
                    "UPDATE alerts SET thresholds = ? WHERE user_id = ?",
                    (json.dumps(thresholds), user_id)
                )
            else:
                conn.execute("DELETE FROM alerts WHERE user_id = ?", (user_id,))
            conn.commit()
            await m.answer(f"✅ {mint} removed from alerts.", **MSG_KW)
        else:
            await m.answer(f"❌ No alert found for {mint[:8]}...", **MSG_KW)
        
        conn.close()
        return
    
    # Filter input handlers
    if user_id in _awaiting_filter_liq and _awaiting_filter_liq[user_id]:
        _awaiting_filter_liq[user_id] = False
        if text_input.lower() == "/skip":
            set_user_filter(user_id, "min_liq", None)
            await m.answer(T("filter_cleared", filter="Liquidity"), **MSG_KW)
        else:
            try:
                value = float(text_input.replace(",", ""))
                set_user_filter(user_id, "min_liq", value)
                await m.answer(T("filter_set", filter="Liquidity", value=f"${value:,.0f}"), **MSG_KW)
            except ValueError:
                await m.answer(T("filter_invalid"), **MSG_KW)
        return
    
    if user_id in _awaiting_filter_age and _awaiting_filter_age[user_id]:
        _awaiting_filter_age[user_id] = False
        if text_input.lower() == "/skip":
            set_user_filter(user_id, "max_age_h", None)
            await m.answer(T("filter_cleared", filter="Age"), **MSG_KW)
        else:
            # Parse age format: 1m, 1h, 1d, 1month, 1year
            import re
            match = re.match(r"(\d+)(m|h|d|month|year)", text_input.lower())
            if match:
                num = int(match.group(1))
                unit = match.group(2)
                hours = {
                    "m": num / 60.0,
                    "h": num,
                    "d": num * 24,
                    "month": num * 24 * 30,
                    "year": num * 24 * 365
                }.get(unit, 0)
                set_user_filter(user_id, "max_age_h", hours)
                await m.answer(T("filter_set", filter="Age", value=text_input), **MSG_KW)
            else:
                await m.answer(T("filter_invalid"), **MSG_KW)
        return
    
    if user_id in _awaiting_filter_vol and _awaiting_filter_vol[user_id]:
        _awaiting_filter_vol[user_id] = False
        if text_input.lower() == "/skip":
            set_user_filter(user_id, "min_vol", None)
            await m.answer(T("filter_cleared", filter="Volume"), **MSG_KW)
        else:
            try:
                value = float(text_input.replace(",", ""))
                set_user_filter(user_id, "min_vol", value)
                await m.answer(T("filter_set", filter="Volume", value=f"${value:,.0f}"), **MSG_KW)
            except ValueError:
                await m.answer(T("filter_invalid"), **MSG_KW)
        return
    
    if user_id in _awaiting_token_input and _awaiting_token_input[user_id]:
        _awaiting_token_input[user_id] = False
        mint_arg = text_input
        mint = normalize_mint_arg(mint_arg)
        if not mint:
            await m.answer(T("cant_detect_mint"), **MSG_KW)
            return
        
        start_time = time.time()
        status = await m.answer(T("fetching_data", mint=mint), **MSG_KW)
        
        async with aiohttp.ClientSession() as session:
            p = await fetch_pair_data(session, mint)
            
            if not p:
                log_command(user_id, "/token", mint, ok=False, err="not_found")
                await status.edit_text(T("token_not_found"), **MSG_KW)
                return
            
            extra = await birdeye_overview(session, mint) if BIRDEYE_API_KEY and mint else None
            mkts = await birdeye_markets(session, mint) if BIRDEYE_API_KEY and mint else None
            security_info = await birdeye_token_security(session, mint) if BIRDEYE_API_KEY and mint else None
            topk_share = security_info.get("top10HolderPercent") or security_info.get("top10_holder_percent") if security_info else None
        
        is_pro = is_pro_user(user_id)
        text = build_full_token_text(p, extra, mkts, security_info, topk_share, is_pro)
        kb = token_keyboard(p, user_id=user_id)
        
        try:
            await status.edit_text(text, reply_markup=kb, **MSG_KW)
        except Exception:
            await m.answer(text, reply_markup=kb, **MSG_KW)
        
        elapsed_ms = int((time.time() - start_time) * 1000)
        log_command(user_id, "/token", mint, ok=True, ms=elapsed_ms)
        return
    
    # Handle menu button taps by converting emoji labels to commands
    if text_input == "🔎 Research":
        await research_handler(m)
        return
    elif text_input == "🎯 Find Token":
        _awaiting_token_input[user_id] = True
        await m.answer(T("awaiting_mint"), **MSG_KW)
        return
    elif text_input == "⭐ Favorites":
        await favorites_menu_handler(m)
        return
    elif text_input == "🔔 Alerts":
        await alerts_menu_handler(m)
        return
    elif text_input == "🧾 My Tier":
        await my_handler(m)
        return
    elif text_input == "❔ Help":
        await help_handler(m)
        return
    elif text_input == "🚪 Logout":
        await logout_handler(m)
        return
    
    if get_user_key(user_id):
        return
    
    candidate = text_input
    ok, msg = is_key_valid_for_product(candidate)
    if ok:
        bind_user(user_id, candidate)
        await m.answer(T("key_accepted", msg=msg), reply_markup=main_menu_keyboard(), **MSG_KW)
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
