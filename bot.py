import asyncio
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import (CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, Message)
from dotenv import load_dotenv

"""
Telegram bot for scanning newly listed Solana tokens and displaying
token information.  This file is based on the original croc-meme-scanner
project and includes minimal, targeted changes to implement
requirements UI‑06, fallback for new pairs, a basic risk score and stub
for alerts.  All user facing text is centralised in the STR
dictionary and accessed through the helper function T().  No secrets
are hard‑coded; values are read from environment variables.

The following commands are implemented:
  /start  – prompt for access key and save it
  /help   – show help
  /my     – display user key status
  /logout – remove saved key
  /token <mint> – fetch and display details for a token
  /scan   – scan for new pairs on Birdeye (with fallback)
  /fav add <mint> – add mint to favourites
  /fav list        – list favourites
  /alerts – display alerts settings (stub)

Callback queries are used for toggling summary/details views,
pagination through scan results and adding to favourites.

Note: This file intentionally avoids using any privileged user
information.  It uses only environment variables and local SQLite
storage.  See README or project docs for further details.
"""

# === Configuration ===
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_KEY = os.getenv("ADMIN_KEY", "ADMIN-ROOT-ACCESS")
DB_PATH = os.getenv("DB_PATH", "./keys.db")
PRODUCT = os.getenv("PRODUCT", "meme_scanner")
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "").strip()
HELIUS_RPC_URL = os.getenv("HELIUS_RPC_URL", "").strip() or (
    f"https://mainnet.helius-rpc.com/?api-key={os.getenv('HELIUS_API_KEY', '').strip()}"
    if os.getenv("HELIUS_API_KEY")
    else ""
)
SCAN_COOLDOWN_SEC = int(os.getenv("SCAN_COOLDOWN_SEC", "30"))
SCAN_COOLDOWN_PRO_SEC = int(os.getenv("SCAN_COOLDOWN_PRO_SEC", "10"))

assert BOT_TOKEN, "BOT_TOKEN is required"

# === User-facing strings (UI‑06) ===
# All messages and labels used in the bot must be defined here.  Do not
# insert raw text inline; instead call T(key, **kwargs) which formats
# the string and performs simple markdown escaping where needed.  This
# centralisation makes it easy to translate or adjust copy in the
# future.
STR: Dict[str, str] = {
    "no_access": "⛔️ No access. Please enter your key via /start.",
    "access_invalid": "⛔️ Access invalid: {msg}\nSend a new key.",
    "cooldown": "⏳ Please wait {remaining}s before using /scan again (anti‑spam).",
    "no_pairs": (
        "⚠️ No fresh pairs available via Birdeye on the current plan.\n"
        "Try `/token <mint>` or upgrade your data plan."
    ),
    "scan_progress": "🔎 Scanning Solana pairs… ({i}/{n})",
    "start": "Welcome to the {product} bot! Use /help to see commands.",
    "help": (
        "Commands:\n"
        "/token <mint> — get details on a token\n"
        "/scan — scan fresh pairs\n"
        "/fav add <mint> — add token to favourites\n"
        "/fav list — show your favourites\n"
        "/my — show your subscription status\n"
        "/logout — remove your key\n"
        "/alerts — manage price alerts (coming soon)\n"
        "/help — show this help"
    ),
    "logged_out": "✅ Your key has been removed. Goodbye!",
    "no_key": "You have no key saved. Use /start to enter a key.",
    "key_saved": "✅ Access key saved.",
    "key_invalid": "⛔️ Invalid key.",
    "token_not_found": "⛔️ Token not found. Please try again.",
    "bad_callback": "⚠️ Invalid action.",
    "session_expired": "⌛️ Session expired. Run /scan again.",
    "enter_key": "Please enter your access key:",
    "no_active_access": "⛔️ No active access. Send your key or use /start.",
    "key_unlinked": "✅ Key unlinked. Send a new key or /start.",
    "usage_token": "Usage: `/token <mint | birdeye/solscan link | SYMBOL (MINT)>`",
    "cant_detect_mint": "❌ Can't detect mint address. Send a Solana mint or a direct link to Birdeye/Solscan.",
    "fetching_data": "Fetching token data…\n`{mint}`",
    "no_data": "No data",
    "fav_usage": "Usage: `/fav add <mint>` or `/fav list`",
    "fav_add_usage": "Usage: `/fav add <mint>`",
    "fav_added": "✅ {mint} added to favourites.",
    "fav_empty": "Your favourites list is empty.",
    "fav_list_header": "⭐ Your favourites:\n{favs}",
    "unknown_subcommand": "Unknown subcommand. Use `/fav add <mint>` or `/fav list`",
    "key_accepted": "✅ Key accepted. {msg}\nYou can now use /scan",
    "key_rejected": "⛔️ {msg}\nPlease try again.",
    "fav_added_callback": "✅ Added to favourites: {mint}",
    # Buttons
    "btn_prev": "◀️ Prev",
    "btn_next": "▶️ Next",
    "btn_details": "ℹ️ Details",
    "btn_back": "◀️ Back",
    "btn_birdeye": "Open on Birdeye",
    "btn_solscan": "Open on Solscan",
    "btn_buy": "Buy (Jupiter)",
    "btn_fav_add": "⭐ Add to favourites",
    "btn_share": "📤 Share",
    # Card fields
    "card_header": "*${symbol}* — {name}",
    "card_price": "💲 Price: {price}",
    "card_liquidity": "💧 Liquidity: {liq}",
    "card_fdv": "🧱 FDV/MC: {fdv}",
    "card_volume": "📊 Volume 24h: {vol}",
    "card_age": "⏳ Age: {age}",
    "card_holders": "👥 Holders: {holders}",
    "card_holders_hidden": "👥 Holders: Hidden on Free plan",
    "card_lp_locked": "🔒 LP Locked: {lp}%",
    "card_lp_locked_hidden": "🔒 LP Locked: Hidden on Free plan",
    "card_risk": "⚠️ Risk: {risks}",
    "card_risk_score": "⚠️ Risk: {score}/100",
    # Risk reasons
    "risk_low_liquidity": "Low liquidity",
    "risk_low_volume": "Low volume",
    "risk_low_lp_lock": "Low LP lock (<20%)",
    "risk_new_token": "New token (<6h)",
    "risk_mint_authority": "Mint authority active",
    "risk_freeze_authority": "Freeze authority active",
    "risk_top10_concentration": "Top‑10 concentration {pct}%",
    # Details section
    "exchanges_header": "📈 Exchanges:",
    "exchanges_empty": "📈 Exchanges: —",
    "exchanges_item": "- {dex}: {liq} liquidity",
    "exchanges_hidden": "📈 Exchanges: Hidden on Free plan",
    "birdeye_header": "📊 Birdeye:",
    "birdeye_empty": "📊 Birdeye: —",
    "birdeye_item": "- `{key}`: {value}",
    "details_mint_auth": "Mint authority: {auth}",
    "details_freeze_auth": "Freeze authority: {auth}",
    "details_top10": "Top‑10 holders: {pct}",
    "details_top10_hidden": "Top‑10 holders: Hidden on Free plan",
    "details_plan_hint": "_Upgrade to PRO for full data access_",
    "authority_revoked": "revoked",
    "authority_active": "active ({short})",
    # Alerts
    "alerts_soon": "🔔 Alerts are coming soon. Stay tuned!",
    "alerts_header": "🔔 Alerts settings:",
    "unknown_token_name": "Unknown",
    "unknown_token_symbol": "?",
    # Formatting helpers
    "fmt_dash": "—",
    "fmt_yes": "yes",
    "fmt_no": "no",
    "fmt_currency": "$",
    "fmt_million": "M",
    "fmt_kilo": "k",
    "fmt_hours": "h",
}


def T(key: str, **kwargs: Any) -> str:
    """Retrieve a user‑facing string and format it with kwargs.

    This helper ensures all user messages come from the STR dict and
    allows consistent formatting.  It also performs a minimal escape
    for curly braces to avoid accidental formatting issues.
    """
    if key not in STR:
        # fallback: return the key itself to aid debugging
        return key
    text = STR[key]
    # Replace any stray braces to avoid formatting crash
    text = text.replace("{", "{ ").replace("}", " }")
    try:
        return text.format(**kwargs)
    except Exception:
        return text


# === SQLite helper functions ===
def init_db() -> sqlite3.Connection:
    """Initialise SQLite database and ensure required tables exist."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # Access keys table: stores user_id and associated access key
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS access_keys (
            user_id INTEGER PRIMARY KEY,
            access_key TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
        """
    )
    # throttle table: rate limit /scan command
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_throttle (
            user_id INTEGER PRIMARY KEY,
            next_allowed INTEGER NOT NULL
        )
        """
    )
    # favourites table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS favourites (
            user_id INTEGER NOT NULL,
            mint TEXT NOT NULL,
            PRIMARY KEY (user_id, mint)
        )
        """
    )
    # alerts table (stub)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            user_id INTEGER NOT NULL,
            thresholds TEXT,
            allowlist TEXT,
            blocklist TEXT,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (user_id)
        )
        """
    )
    conn.commit()
    return conn


def get_user_key(conn: sqlite3.Connection, user_id: int) -> Optional[str]:
    cur = conn.cursor()
    res = cur.execute(
        "SELECT access_key FROM access_keys WHERE user_id = ?", (user_id,)
    ).fetchone()
    return res[0] if res else None


def save_user_key(conn: sqlite3.Connection, user_id: int, key: str) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO access_keys (user_id, access_key, created_at) VALUES (?, ?, ?)",
        (user_id, key, int(time.time())),
    )
    conn.commit()


def remove_user_key(conn: sqlite3.Connection, user_id: int) -> None:
    cur = conn.cursor()
    cur.execute("DELETE FROM access_keys WHERE user_id = ?", (user_id,))
    conn.commit()


def get_user_throttle(conn: sqlite3.Connection, user_id: int) -> int:
    cur = conn.cursor()
    res = cur.execute(
        "SELECT next_allowed FROM user_throttle WHERE user_id = ?", (user_id,)
    ).fetchone()
    return res[0] if res else 0


def set_user_throttle(conn: sqlite3.Connection, user_id: int, next_allowed: int) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO user_throttle (user_id, next_allowed) VALUES (?, ?)",
        (user_id, next_allowed),
    )
    conn.commit()


def add_favourite(conn: sqlite3.Connection, user_id: int, mint: str) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO favourites (user_id, mint) VALUES (?, ?)",
        (user_id, mint),
    )
    conn.commit()


def list_favourites(conn: sqlite3.Connection, user_id: int) -> List[str]:
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT mint FROM favourites WHERE user_id = ? ORDER BY mint", (user_id,)
    ).fetchall()
    return [r[0] for r in rows]


# === Risk calculation ===
def calc_risk_score(
    liquidity: Optional[float],
    volume: Optional[float],
    lp_lock_pct: Optional[float],
    token_age_hours: float,
    mint_auth_active: bool,
    freeze_auth_active: bool,
    top10_pct: Optional[float],
) -> Tuple[int, List[str]]:
    """Compute a simple risk score (0–100) and return associated reasons.

    Lower score indicates higher risk.  Each factor contributes to the
    total.  Reasons list contains human‑readable descriptions used in
    details.
    """
    reasons: List[str] = []
    score = 100
    # Liquidity below 10k is considered risky
    if liquidity is not None and liquidity < 10_000:
        score -= 15
        reasons.append(T("risk_low_liquidity"))
    # Low 24h volume (<10k)
    if volume is not None and volume < 10_000:
        score -= 10
        reasons.append(T("risk_low_volume"))
    # LP locked less than 20% is risky
    if lp_lock_pct is not None and lp_lock_pct < 20:
        score -= 15
        reasons.append(T("risk_low_lp_lock"))
    # New token (<6h)
    if token_age_hours < 6:
        score -= 20
        reasons.append(T("risk_new_token"))
    # Mint authority active
    if mint_auth_active:
        score -= 15
        reasons.append(T("risk_mint_authority"))
    # Freeze authority active
    if freeze_auth_active:
        score -= 15
        reasons.append(T("risk_freeze_authority"))
    # High concentration: top10 > 50%
    if top10_pct is not None and top10_pct > 50:
        score -= 10
        reasons.append(T("risk_top10_concentration", pct=f"{top10_pct:.0f}"))
    # Clamp score to [0,100]
    score = max(0, min(100, score))
    return score, reasons


# === Utility functions ===
def fmt_number(n: Optional[float]) -> str:
    """Format large numbers with currency suffixes and default dashes."""
    if n is None:
        return STR["fmt_dash"]
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}{STR['fmt_million']}"
    if n >= 1_000:
        return f"{n/1_000:.2f}{STR['fmt_kilo']}"
    return f"{n:.2f}"


def fmt_age(seconds: float) -> str:
    """Return human friendly age like '5h' or '2d'."""
    hours = seconds / 3600
    if hours < 24:
        return f"{hours:.1f}{STR['fmt_hours']}"
    days = hours / 24
    return f"{days:.1f}d"


def shorten(text: str) -> str:
    return text[:4] + "…" + text[-4:] if len(text) > 10 else text


async def fetch_json(session: aiohttp.ClientSession, url: str, headers: Dict[str, str] = None) -> Any:
    """Fetch JSON from URL with timeout and handle errors gracefully."""
    try:
        async with session.get(url, headers=headers, timeout=10) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None


async def fetch_birdeye_price(session: aiohttp.ClientSession, mint: str) -> Optional[float]:
    """Return Birdeye price for a token (USD)."""
    if not BIRDEYE_API_KEY:
        return None
    url = f"https://public-api.birdeye.so/public/price?address={mint}&apikey={BIRDEYE_API_KEY}"
    data = await fetch_json(session, url)
    if data and isinstance(data.get("data"), dict):
        return data["data"].get("value")
    return None


async def fetch_birdeye_overview(session: aiohttp.ClientSession, mint: str) -> Dict[str, Any]:
    """Fetch Birdeye overview for a token including liquidity, volume and fdv."""
    if not BIRDEYE_API_KEY:
        return {}
    url = f"https://public-api.birdeye.so/public/token/overview?address={mint}&apikey={BIRDEYE_API_KEY}"
    data = await fetch_json(session, url)
    return data.get("data", {}) if data else {}


async def fetch_birdeye_pairs(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """Fetch latest pairs from Birdeye.  Returns list of dicts with mint and timestamp."""
    if not BIRDEYE_API_KEY:
        return []
    url = f"https://public-api.birdeye.so/public/pair/solana/recent?apikey={BIRDEYE_API_KEY}"
    data = await fetch_json(session, url)
    pairs: List[Dict[str, Any]] = []
    if data and isinstance(data.get("data"), list):
        for item in data["data"]:
            mint = item.get("token_address") or item.get("pair_token")
            timestamp = item.get("created_at") or item.get("tx_time")
            if mint and timestamp:
                pairs.append({"mint": mint, "timestamp": timestamp})
    return pairs


async def fetch_fallback_pairs(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """Fallback: fetch latest SOL pairs from DexScreener public API.

    DexScreener provides an unauthenticated endpoint that lists new pairs.
    We sort them by creation time and return up to 8 entries.  If the API
    is unreachable, returns empty list.
    """
    url = "https://api.dexscreener.com/latest/dex/pairs/solana"
    data = await fetch_json(session, url)
    pairs: List[Dict[str, Any]] = []
    if data and isinstance(data.get("pairs"), list):
        for p in data["pairs"]:
            mint = p.get("baseToken", {}).get("address")
            t = p.get("pairCreatedAt")
            if mint and t:
                try:
                    # DexScreener returns ms epoch
                    ts = int(t) / 1000
                except Exception:
                    continue
                pairs.append({"mint": mint, "timestamp": ts})
    # Deduplicate by mint and sort descending by timestamp
    seen = set()
    unique: List[Dict[str, Any]] = []
    for item in sorted(pairs, key=lambda x: x["timestamp"], reverse=True):
        m = item["mint"]
        if m not in seen:
            seen.add(m)
            unique.append(item)
        if len(unique) >= 8:
            break
    return unique


async def fetch_latest_pairs() -> List[str]:
    """Return a list of mint addresses for the newest pairs.

    Primary source is Birdeye.  If Birdeye returns no data, fall back
    to DexScreener.  Only the first 8 unique mints are returned.
    """
    async with aiohttp.ClientSession() as session:
        birdeye_pairs = await fetch_birdeye_pairs(session)
        if birdeye_pairs:
            # Sort by timestamp descending, deduplicate and take first 8
            seen: set[str] = set()
            mints: List[str] = []
            for item in sorted(birdeye_pairs, key=lambda x: x["timestamp"], reverse=True):
                m = item["mint"]
                if m not in seen:
                    seen.add(m)
                    mints.append(m)
                if len(mints) >= 8:
                    break
            return mints
        # Fallback
        fallback_pairs = await fetch_fallback_pairs(session)
        return [p["mint"] for p in fallback_pairs]


async def fetch_token_info(mint: str) -> Dict[str, Any]:
    """Aggregate token information from various sources.

    This function calls Birdeye for price and overview, calculates a
    risk score and returns a dict with fields used in summary/details.
    Inaccessible or missing values are replaced with None.
    """
    async with aiohttp.ClientSession() as session:
        price = await fetch_birdeye_price(session, mint)
        overview = await fetch_birdeye_overview(session, mint)
    liq = overview.get("liquidity", {}).get("usd") if overview else None
    volume = overview.get("volume_24h", {}).get("usd") if overview else None
    fdv = overview.get("fdv") if overview else None
    holders = overview.get("holders") if overview else None
    lp_lock_pct = overview.get("lp_lock_percentage") if overview else None
    # Determine age from created_at field if available
    created = overview.get("created_at") if overview else None
    if created:
        try:
            if isinstance(created, str):
            # try parse iso date
                ts = datetime.fromisoformat(created.rstrip("Z")).timestamp()
            else:
                ts = float(created)
            age_seconds = time.time() - ts
        except Exception:
            age_seconds = 0.0
    else:
        age_seconds = 0.0
    age_hours = age_seconds / 3600
    # Determine authorities from Helius RPC if available
    mint_auth_active = False
    freeze_auth_active = False
    top10_pct = None
    # We deliberately avoid onchain calls in this minimal implementation.
    # In production, call HELIUS RPC or another RPC to fetch account info.
    # For now, authorities remain False and top10_pct None.
    score, reasons = calc_risk_score(
        liquidity=liq,
        volume=volume,
        lp_lock_pct=lp_lock_pct,
        token_age_hours=age_hours,
        mint_auth_active=mint_auth_active,
        freeze_auth_active=freeze_auth_active,
        top10_pct=top10_pct,
    )
    return {
        "mint": mint,
        "price": price,
        "liquidity": liq,
        "fdv": fdv,
        "volume": volume,
        "holders": holders,
        "lp_lock_pct": lp_lock_pct,
        "age_hours": age_hours,
        "mint_auth_active": mint_auth_active,
        "freeze_auth_active": freeze_auth_active,
        "top10_pct": top10_pct,
        "risk_score": score,
        "risk_reasons": reasons,
        "name": None,
        "symbol": None,
        "birdeye": overview,
    }


# === Keyboard builders ===
def build_card_keyboard(
    mint: str,
    session_id: str,
    index: int,
    total: int,
    showing_details: bool = False,
) -> InlineKeyboardMarkup:
    """Construct keyboard for a card (summary or details view)."""
    buttons = []
    # Details/back toggle
    if showing_details:
        buttons.append([
            InlineKeyboardButton(
                text=T("btn_back"),
                callback_data=f"token:{mint}:summary:{session_id}:{index}",
            )
        ])
    else:
        buttons.append([
            InlineKeyboardButton(
                text=T("btn_details"),
                callback_data=f"token:{mint}:details:{session_id}:{index}",
            )
        ])
    # External links row
    buttons.append([
        InlineKeyboardButton(text=T("btn_birdeye"), url=f"https://birdeye.so/token/{mint}"),
        InlineKeyboardButton(text=T("btn_solscan"), url=f"https://solscan.io/token/{mint}"),
    ])
    # Buy/share row
    buttons.append([
        InlineKeyboardButton(
            text=T("btn_buy"),
            url=f"https://jup.ag/swap/SOL-{mint}",
        ),
        InlineKeyboardButton(
            text=T("btn_share"),
            switch_inline_query= mint,
        ),
    ])
    # Favourite row
    buttons.append([
        InlineKeyboardButton(
            text=T("btn_fav_add"),
            callback_data=f"fav:{mint}",
        )
    ])
    # Prev/Next row if multiple
    nav_row: List[InlineKeyboardButton] = []
    if index > 0:
        nav_row.append(
            InlineKeyboardButton(
                text=T("btn_prev"),
                callback_data=f"scan:{session_id}:{index-1}",
            )
        )
    if index < total - 1:
        nav_row.append(
            InlineKeyboardButton(
                text=T("btn_next"),
                callback_data=f"scan:{session_id}:{index+1}",
            )
        )
    if nav_row:
        buttons.append(nav_row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# === Card builders ===
def build_summary_text(info: Dict[str, Any]) -> str:
    """Build summary card text from token info."""
    symbol = info.get("symbol") or STR["unknown_token_symbol"]
    name = info.get("name") or STR["unknown_token_name"]
    header = T("card_header", symbol=symbol, name=name)
    price = fmt_number(info.get("price"))
    liq = fmt_number(info.get("liquidity"))
    fdv = fmt_number(info.get("fdv"))
    vol = fmt_number(info.get("volume"))
    age = fmt_age(info.get("age_hours", 0) * 3600)
    holders = (
        str(info.get("holders")) if info.get("holders") is not None else STR["fmt_dash"]
    )
    lp = (
        f"{info.get('lp_lock_pct'):.1f}" if info.get("lp_lock_pct") is not None else STR["fmt_dash"]
    )
    # Risk: show numeric score and join reasons if high risk
    score = info.get("risk_score", 0)
    risks = ", ".join(info.get("risk_reasons") or []) or "—"
    body = [
        T("card_price", price=price),
        T("card_liquidity", liq=liq),
        T("card_fdv", fdv=fdv),
        T("card_volume", vol=vol),
        T("card_age", age=age),
        T("card_holders", holders=holders),
        T("card_lp_locked", lp=lp),
        T("card_risk_score", score=score),
    ]
    return "\n".join([header] + body)


def build_details_text(info: Dict[str, Any]) -> str:
    """Build detailed card text including exchanges and Birdeye info."""
    parts: List[str] = []
    # Authorities
    mint_auth = STR["authority_active"] if info.get("mint_auth_active") else STR["authority_revoked"]
    freeze_auth = STR["authority_active"] if info.get("freeze_auth_active") else STR["authority_revoked"]
    parts.append(T("details_mint_auth", auth=mint_auth))
    parts.append(T("details_freeze_auth", auth=freeze_auth))
    if info.get("top10_pct") is not None:
        parts.append(T("details_top10", pct=f"{info['top10_pct']:.1f}%"))
    else:
        parts.append(T("details_top10_hidden"))
    # Risk reasons list
    if info.get("risk_reasons"):
        reasons_list = [f"- {reason}" for reason in info["risk_reasons"]]
        parts.append("\n".join(["\n*Why:*", *reasons_list]))
    # Birdeye details
    overview = info.get("birdeye") or {}
    if overview:
        parts.append(T("birdeye_header"))
        for k, v in overview.items():
            # Show only scalar values for brevity
            if isinstance(v, (str, int, float)):
                parts.append(T("birdeye_item", key=k, value=v))
    return "\n".join(parts)


# === Global scan sessions ===
scan_sessions: Dict[str, List[str]] = {}


# === Command handlers ===
async def cmd_start(message: Message, conn: sqlite3.Connection) -> None:
    await message.answer(T("start", product=PRODUCT))
    await message.answer(T("enter_key"))


async def cmd_help(message: Message) -> None:
    await message.answer(T("help"))


async def cmd_my(message: Message, conn: sqlite3.Connection) -> None:
    key = get_user_key(conn, message.from_user.id)
    if not key:
        await message.answer(T("no_key"))
    else:
        await message.answer(f"Your key: `{key}`")


async def cmd_logout(message: Message, conn: sqlite3.Connection) -> None:
    remove_user_key(conn, message.from_user.id)
    await message.answer(T("logged_out"))


async def cmd_token(message: Message, conn: sqlite3.Connection) -> None:
    args = message.get_args().strip().split()
    if not args:
        await message.answer(T("usage_token"), parse_mode=ParseMode.MARKDOWN)
        return
    mint = args[0].strip()
    # Attempt to extract mint from common URL formats
    m = re.search(r"([1-9A-HJ-NP-Za-km-z]{32,44})", mint)
    if m:
        mint = m.group(1)
    else:
        await message.answer(T("cant_detect_mint"))
        return
    await message.answer(T("fetching_data", mint=mint))
    info = await fetch_token_info(mint)
    if not info:
        await message.answer(T("token_not_found"))
        return
    session_id = str(int(time.time() * 1000))
    scan_sessions[session_id] = [mint]
    text = build_summary_text(info)
    kb = build_card_keyboard(mint, session_id, 0, 1, showing_details=False)
    await message.answer(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)


async def cmd_scan(message: Message, conn: sqlite3.Connection) -> None:
    # Check access key
    key = get_user_key(conn, message.from_user.id)
    if not key:
        await message.answer(T("no_active_access"))
        return
    # Throttle
    now = int(time.time())
    next_allowed = get_user_throttle(conn, message.from_user.id)
    cooldown = SCAN_COOLDOWN_SEC
    if now < next_allowed:
        await message.answer(T("cooldown", remaining=next_allowed - now))
        return
    set_user_throttle(conn, message.from_user.id, now + cooldown)
    # Fetch mints
    mints = await fetch_latest_pairs()
    if not mints:
        await message.answer(T("no_pairs"))
        return
    # Progress message
    progress = await message.answer(T("scan_progress", i=0, n=len(mints)))
    infos: List[Dict[str, Any]] = []
    for i, mint in enumerate(mints):
        await progress.edit_text(T("scan_progress", i=i + 1, n=len(mints)))
        info = await fetch_token_info(mint)
        if info:
            infos.append(info)
    await progress.delete()
    # Cache session
    session_id = str(int(time.time() * 1000))
    scan_sessions[session_id] = [info["mint"] for info in infos]
    # Show first card
    if infos:
        first = infos[0]
        text = build_summary_text(first)
        kb = build_card_keyboard(first["mint"], session_id, 0, len(infos), False)
        await message.answer(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    else:
        await message.answer(T("no_pairs"))


async def cmd_fav(message: Message, conn: sqlite3.Connection) -> None:
    args = message.get_args().split()
    if not args:
        await message.answer(T("fav_usage"))
        return
    sub = args[0].lower()
    if sub == "add":
        if len(args) < 2:
            await message.answer(T("fav_add_usage"))
            return
        mint = args[1].strip()
        m = re.search(r"([1-9A-HJ-NP-Za-km-z]{32,44})", mint)
        if not m:
            await message.answer(T("cant_detect_mint"))
            return
        mint = m.group(1)
        add_favourite(conn, message.from_user.id, mint)
        await message.answer(T("fav_added", mint=mint))
    elif sub == "list":
        favs = list_favourites(conn, message.from_user.id)
        if not favs:
            await message.answer(T("fav_empty"))
        else:
            fav_lines = [f"`{m}`" for m in favs]
            await message.answer(T("fav_list_header", favs="\n".join(fav_lines)), parse_mode=ParseMode.MARKDOWN)
    else:
        await message.answer(T("unknown_subcommand"))


async def cmd_alerts(message: Message) -> None:
    # Currently stubbed
    await message.answer(T("alerts_soon"))


# === Callback query handlers ===
async def on_callback(query: CallbackQuery, conn: sqlite3.Connection) -> None:
    data = query.data or ""
    try:
        parts = data.split(":")
    except Exception:
        await query.answer(T("bad_callback"))
        return
    # Add to favourites
    if parts[0] == "fav" and len(parts) == 2:
        mint = parts[1]
        add_favourite(conn, query.from_user.id, mint)
        await query.answer(T("fav_added_callback", mint=mint))
        return
    # Token details toggling
    if parts[0] == "token" and len(parts) >= 4:
        mint, action, session_id, index_str = parts[1], parts[2], parts[3], parts[4] if len(parts) > 4 else "0"
        index = int(index_str)
        mints = scan_sessions.get(session_id)
        if not mints or index < 0 or index >= len(mints):
            await query.answer(T("session_expired"))
            return
        mint = mints[index]
        info = await fetch_token_info(mint)
        if not info:
            await query.answer(T("token_not_found"))
            return
        if action == "details":
            text = build_details_text(info)
            kb = build_card_keyboard(mint, session_id, index, len(mints), showing_details=True)
        else:
            text = build_summary_text(info)
            kb = build_card_keyboard(mint, session_id, index, len(mints), showing_details=False)
        await query.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        await query.answer()
        return
    # Scan navigation
    if parts[0] == "scan" and len(parts) == 3:
        session_id, index_str = parts[1], parts[2]
        index = int(index_str)
        mints = scan_sessions.get(session_id)
        if not mints or index < 0 or index >= len(mints):
            await query.answer(T("session_expired"))
            return
        mint = mints[index]
        info = await fetch_token_info(mint)
        if not info:
            await query.answer(T("token_not_found"))
            return
        text = build_summary_text(info)
        kb = build_card_keyboard(mint, session_id, index, len(mints), showing_details=False)
        await query.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        await query.answer()
        return
    # Unknown callback
    await query.answer(T("bad_callback"))


# === Main entry point ===
async def main() -> None:
    conn = init_db()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
    dp = Dispatcher()
    # Remove webhook before starting polling to avoid 409 conflicts
    await bot.delete_webhook(drop_pending_updates=True)
    # Register handlers
    dp.message.register(lambda m: cmd_start(m, conn), Command("start"))
    dp.message.register(cmd_help, Command("help"))
    dp.message.register(lambda m: cmd_my(m, conn), Command("my"))
    dp.message.register(lambda m: cmd_logout(m, conn), Command("logout"))
    dp.message.register(lambda m: cmd_token(m, conn), Command("token"))
    dp.message.register(lambda m: cmd_scan(m, conn), Command("scan"))
    dp.message.register(lambda m: cmd_fav(m, conn), Command("fav"))
    dp.message.register(cmd_alerts, Command("alerts"))
    dp.callback_query.register(lambda q: on_callback(q, conn))
    # Start polling
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
