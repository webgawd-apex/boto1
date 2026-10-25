# bot.py
import os
import asyncio
import sqlite3
import base64
import struct
import logging
import html
import time
import json
from dotenv import load_dotenv
import requests
from solders.keypair import Keypair

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    Application, CommandHandler, ConversationHandler,
    MessageHandler, filters, ContextTypes, CallbackQueryHandler
)

from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import threading

load_dotenv()

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
HELIUS_KEY = os.environ["HELIUS_KEY"]
RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}"

TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPwcXVYew4W"
LAMPORTS_PER_SOL = 1_000_000_000

MAX_WALLETS = 10
POLL_INTERVAL = 15

DATABASE_URL = os.getenv("DATABASE_URL")

def resolve_db_path():
    """Auto-detect persistent storage directory if on Render (/var/data, /data) or DB_FILE."""
    env_file = os.getenv("DB_FILE")
    if env_file:
        return env_file
    for candidate in ("/var/data", "/data"):
        if os.path.isdir(candidate) and os.access(candidate, os.W_OK):
            return os.path.join(candidate, "wallets.db")
    return "wallets.db"

DB_FILE = resolve_db_path()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("watcher")


# --- Bot state (for /status) ------------------------------------------------
# Analysis: the monitor is invisible from the outside. These globals expose
# just enough for /status to prove the loop is alive and Helius is reachable.
# Written by the monitor loop, read by /status. Both run on the same asyncio
# loop, so no locking is needed.

state = {
    "last_cycle_ts": 0.0,
    "last_cycle_ms": 0,
    "last_cycle_wallets": 0,
    "last_cycle_new": 0,
    "last_cycle_sells": 0,
    "last_cycle_failures": 0,
    "last_rpc_ok": None,
    "started_at": time.time(),
}


def start_health_server():
    """Starts a minimal HTTP health-check server if $PORT is set (e.g. on Render)."""
    port_str = os.environ.get("PORT")
    if not port_str:
        return
    try:
        port = int(port_str)

        class HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"Solana Wallet Watcher Bot is running.")

            def do_HEAD(self):
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()

            def log_message(self, format, *args):
                pass

        server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        log.info(f"Started Render health check server on port {port}")
    except Exception as e:
        log.warning(f"Could not start health check server on port {port_str}: {e}")


# --- Database ---------------------------------------------------------------
# Keyed by (user_id, name) so one user cannot have two wallets called "main"
# but two users can each have their own.
#
# Persistence:
# 1. If DATABASE_URL is set (Neon, Supabase, Render Postgres), uses persistent cloud PostgreSQL.
# 2. Otherwise uses SQLite, automatically storing in persistent mount (/var/data, /data, or $DB_FILE).

def get_db():
    """Returns (conn, is_pg) connected to PostgreSQL or SQLite."""
    if DATABASE_URL and DATABASE_URL.startswith(("postgres://", "postgresql://")):
        import psycopg2
        url = DATABASE_URL
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://"):]
        conn = psycopg2.connect(url)
        return conn, True
    else:
        conn = sqlite3.connect(DB_FILE)
        return conn, False


def db_init():
    conn, is_pg = get_db()
    cur = conn.cursor()
    if is_pg:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS wallets (
                id       SERIAL PRIMARY KEY,
                user_id  BIGINT NOT NULL,
                name     TEXT   NOT NULL,
                address  TEXT   NOT NULL,
                paused   INTEGER NOT NULL DEFAULT 0,
                UNIQUE(user_id, name)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trading_wallets (
                user_id     BIGINT PRIMARY KEY,
                address     TEXT NOT NULL,
                private_key TEXT NOT NULL
            )
        """)
        log.info("Database initialized: connected to persistent PostgreSQL")
    else:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS wallets (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id  INTEGER NOT NULL,
                name     TEXT    NOT NULL,
                address  TEXT    NOT NULL,
                paused   INTEGER NOT NULL DEFAULT 0,
                UNIQUE(user_id, name)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trading_wallets (
                user_id     INTEGER PRIMARY KEY,
                address     TEXT NOT NULL,
                private_key TEXT NOT NULL
            )
        """)
        try:
            cur.execute("ALTER TABLE wallets ADD COLUMN paused INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass
        log.info("Database initialized: using SQLite at %s", DB_FILE)
    conn.commit()
    cur.close()
    conn.close()


def db_get_trading_wallet(user_id):
    conn, is_pg = get_db()
    cur = conn.cursor()
    q = "SELECT address, private_key FROM trading_wallets WHERE user_id=%s" if is_pg else "SELECT address, private_key FROM trading_wallets WHERE user_id=?"
    cur.execute(q, (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return (row[0], row[1]) if row else None


def db_set_trading_wallet(user_id, address, private_key):
    conn, is_pg = get_db()
    cur = conn.cursor()
    q = """
        INSERT INTO trading_wallets (user_id, address, private_key) VALUES (%s, %s, %s)
        ON CONFLICT(user_id) DO UPDATE SET address=EXCLUDED.address, private_key=EXCLUDED.private_key
    """ if is_pg else """
        INSERT INTO trading_wallets (user_id, address, private_key) VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET address=excluded.address, private_key=excluded.private_key
    """
    cur.execute(q, (user_id, address, private_key))
    conn.commit()
    cur.close()
    conn.close()


def db_remove_trading_wallet(user_id):
    conn, is_pg = get_db()
    cur = conn.cursor()
    q = "DELETE FROM trading_wallets WHERE user_id=%s" if is_pg else "DELETE FROM trading_wallets WHERE user_id=?"
    cur.execute(q, (user_id,))
    conn.commit()
    removed = cur.rowcount > 0
    cur.close()
    conn.close()
    return removed


def generate_new_wallet():
    """Generates a fresh Solana keypair. Returns (address, private_key_b58)."""
    kp = Keypair()
    return str(kp.pubkey()), str(kp)


def parse_and_validate_private_key(key_str: str):
    """Validates base58 private key or JSON byte array. Returns (pubkey, private_key_b58) or None."""
    s = key_str.strip()
    if s.startswith("[") and s.endswith("]"):
        try:
            b = bytes(json.loads(s))
            if len(b) == 64:
                k = Keypair.from_bytes(b)
                return str(k.pubkey()), str(k)
            elif len(b) == 32:
                k = Keypair.from_seed(b)
                return str(k.pubkey()), str(k)
        except Exception:
            pass
    try:
        k = Keypair.from_base58_string(s)
        return str(k.pubkey()), str(k)
    except Exception:
        return None


def db_add(user_id, name, address):
    conn, is_pg = get_db()
    cur = conn.cursor()
    q = "INSERT INTO wallets (user_id, name, address) VALUES (%s, %s, %s)" if is_pg else "INSERT INTO wallets (user_id, name, address) VALUES (?, ?, ?)"
    try:
        cur.execute(q, (user_id, name, address))
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        cur.close()
        conn.close()


def db_count(user_id):
    conn, is_pg = get_db()
    cur = conn.cursor()
    q = "SELECT COUNT(*) FROM wallets WHERE user_id=%s" if is_pg else "SELECT COUNT(*) FROM wallets WHERE user_id=?"
    cur.execute(q, (user_id,))
    n = cur.fetchone()[0]
    cur.close()
    conn.close()
    return n


def db_list(user_id):
    """Returns list of (name, address, paused) tuples ordered by insertion."""
    conn, is_pg = get_db()
    cur = conn.cursor()
    q = "SELECT name, address, paused FROM wallets WHERE user_id=%s ORDER BY id" if is_pg else "SELECT name, address, paused FROM wallets WHERE user_id=? ORDER BY id"
    cur.execute(q, (user_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def db_set_paused(user_id, name, paused: bool):
    """Toggle the paused flag for a single wallet."""
    conn, is_pg = get_db()
    cur = conn.cursor()
    q = "UPDATE wallets SET paused=%s WHERE user_id=%s AND name=%s" if is_pg else "UPDATE wallets SET paused=? WHERE user_id=? AND name=?"
    cur.execute(q, (1 if paused else 0, user_id, name))
    conn.commit()
    cur.close()
    conn.close()


def db_remove(user_id, name):
    conn, is_pg = get_db()
    cur = conn.cursor()
    q = "DELETE FROM wallets WHERE user_id=%s AND name=%s" if is_pg else "DELETE FROM wallets WHERE user_id=? AND name=?"
    cur.execute(q, (user_id, name))
    conn.commit()
    removed = cur.rowcount > 0
    cur.close()
    conn.close()
    return removed


def db_all_wallets():
    """Returns list of (user_id, name, address, paused) for every user."""
    conn, is_pg = get_db()
    cur = conn.cursor()
    cur.execute("SELECT user_id, name, address, paused FROM wallets")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# --- RPC --------------------------------------------------------------------
# Single choke point. Any failure collapses to None. No retries — the next
# poll is 15s away and retries inside a cycle cause pile-ups. Records success
# or failure in `state` so /status can tell the user whether Helius is reachable.

def rpc(method, params):
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        r = requests.post(RPC_URL, json=body, timeout=10)
        result = r.json().get("result")
        state["last_rpc_ok"] = True
        return result
    except Exception as e:
        state["last_rpc_ok"] = False
        log.warning("rpc %s failed: %s", method, e)
        return None


def get_sol_balance(address):
    """Blocking. Returns SOL as float, or None on any RPC failure.
    Called only when an alert fires — never on the hot poll path — so it
    adds zero latency to normal 15-second cycles."""
    res = rpc("getBalance", [address])
    if res is None:
        return None
    try:
        return res.get("value", 0) / LAMPORTS_PER_SOL
    except Exception:
        return None


# --- Mint decoding ----------------------------------------------------------
# SPL Token mint layout (fixed width):
#   0..3    mint_authority COption flag (1 = present)
#   4..35   mint_authority pubkey
#   36..43  supply (u64 LE)
#   44      decimals
#   45      is_initialized
#   46..49  freeze_authority COption flag
#   50..81  freeze_authority pubkey

def parse_mint_account(data):
    mint_flag = struct.unpack_from("<I", data, 0)[0]
    freeze_flag = struct.unpack_from("<I", data, 46)[0]
    return (mint_flag == 1, freeze_flag == 1)


# Token-2022 extensions start at offset 166 (82 base + 83 padding + 1 tag).
# Each entry: u16 type, u16 len, then len bytes. Type 0 = trailing pad, stop.

def parse_token2022_extensions(data):
    extensions = {}
    offset = 166
    total = len(data)
    while offset + 4 <= total:
        ext_type = struct.unpack_from("<H", data, offset)[0]
        ext_len = struct.unpack_from("<H", data, offset + 2)[0]
        offset += 4
        if ext_type == 0:
            break
        if offset + ext_len > total:
            break
        extensions[ext_type] = data[offset:offset + ext_len]
        offset += ext_len
    return extensions


# --- Rug check --------------------------------------------------------------
# Analysis: static reads only. Never signs, never simulates, never touches a
# stateful endpoint. Score starts at 100, drops per red flag.
#
# Deliberately NOT here, because each needs a service we do not use:
#   - Liquidity depth (DEX API)
#   - Bundle / sniper detection (historical tx analysis)
#   - Holder wallet reputation (label service)
# Anything we cannot prove with Helius RPC alone is reported as WARN, not SAFE.

def check_rug(mint):
    score = 100
    risks = []

    # Step 1: mint must exist.
    acct = rpc("getAccountInfo", [mint, {"encoding": "base64"}])
    if not acct or not acct.get("value"):
        return ("UNKNOWN", 0, ["mint not found"])

    value = acct["value"]
    owner = value.get("owner", "")
    try:
        data = base64.b64decode(value["data"][0])
    except Exception:
        return ("UNKNOWN", 0, ["data unavailable"])

    if len(data) < 82:
        return ("UNKNOWN", 0, ["data unavailable"])

    # Step 2: classic authorities.
    try:
        has_mint_auth, has_freeze_auth = parse_mint_account(data)
    except Exception:
        return ("UNKNOWN", 0, ["data unavailable"])

    if has_mint_auth:
        score -= 30
        risks.append("Mint authority active")
    if has_freeze_auth:
        score -= 30
        risks.append("Freeze authority active — sell can be blocked")

    # Step 3: Token-2022 extras (only if the mint is owned by that program).
    if owner == TOKEN_2022_PROGRAM:
        try:
            exts = parse_token2022_extensions(data)
        except Exception:
            exts = {}

        if 14 in exts:
            score -= 25
            risks.append("Transfer hook — programmable transfer block")
        if 12 in exts:
            score -= 30
            risks.append("Permanent delegate — tokens can be seized")
        if 13 in exts:
            score -= 50
            risks.append("Non-transferable — cannot sell")
        if 15 in exts:
            ext_data = exts[15]
            if len(ext_data) >= 1 and ext_data[0] == 2:
                score -= 20
                risks.append("Default frozen accounts")
        if 1 in exts:
            ext_data = exts[1]
            if len(ext_data) >= 68:
                # Newer fee lives at offset 66 inside the extension body.
                newer_bps = struct.unpack_from("<H", ext_data, 66)[0]
                if newer_bps > 500:
                    score -= 20
                    risks.append("Transfer fee >5%")
                if newer_bps > 0:
                    risks.append(f"Transfer fee {newer_bps / 100:.2f}%")

    # Step 4: top-10 holder concentration vs. total supply.
    # Snapshot only. Isolated try/except so a bad response here does not
    # invalidate the authority checks above.
    try:
        supply_res = rpc("getTokenSupply", [mint])
        largest_res = rpc("getTokenLargestAccounts", [mint])
        if supply_res and largest_res:
            total = int(supply_res["value"]["amount"])
            accounts = largest_res.get("value") or []
            top10 = sum(int(a["amount"]) for a in accounts[:10])
            if total > 0:
                pct = top10 / total * 100
                if pct > 50:
                    score -= 20
                    risks.append(f"Top 10 hold {pct:.1f}%")
                elif pct > 30:
                    score -= 10
                    risks.append(f"Top 10 hold {pct:.1f}%")
    except Exception as e:
        log.warning("holder check failed for %s: %s", mint, e)

    # Step 5: metadata mutability via Helius DAS.
    try:
        asset = rpc("getAsset", [{"id": mint}])
        if asset and isinstance(asset, dict) and asset.get("mutable") is True:
            score -= 10
            risks.append("Metadata mutable")
    except Exception as e:
        log.warning("getAsset failed for %s: %s", mint, e)

    # Step 6: clamp and bucket.
    score = max(0, min(100, score))
    if score >= 70:
        verdict = "SAFE"
    elif score >= 40:
        verdict = "WARN"
    else:
        verdict = "DANGER"
    return (verdict, score, risks[:5])


# --- Telegram handlers ------------------------------------------------------

BASE58_ALPHABET = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
ASK_ADDRESS, ASK_NAME, ASK_IMPORT_KEY = range(3)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    text = (
        "👋 <b>Solana Wallet Watcher & Trader</b>\n\n"
        "I monitor Solana wallets in real time and alert you when they buy "
        "or sell tokens, complete with live balance and an on-chain safety score.\n\n"
        "<b>Commands:</b>\n"
        "• /my_wallet — Manage your trading wallet (generate or import)\n"
        "• /create_wallet — Generate a new trading wallet\n"
        "• /import_wallet — Import an existing trading wallet private key\n"
        "• /new_watch — Add a wallet to watch (max 10)\n"
        "• /my_watches — List your tracked wallets\n"
        "• /pause_watch — Pause or resume watching a wallet\n"
        "• /remove_watch — Remove a watched wallet\n"
        "• /cancel — Abort the current operation"
    )
    await update.effective_message.reply_text(text, parse_mode="HTML")
    return ConversationHandler.END


# --- Trading Wallet Dual Mode (Generate or Import) --------------------------

async def cmd_my_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the user's trading wallet with balance, deposit address, and action buttons."""
    user_id = update.effective_user.id
    tw = db_get_trading_wallet(user_id)
    if not tw:
        buttons = [
            [InlineKeyboardButton("🆕 Generate New Wallet", callback_data="twal:create")],
            [InlineKeyboardButton("📥 Import Private Key", callback_data="twal:import_prompt")],
        ]
        await update.effective_message.reply_text(
            "⚡ <b>Solana Trading Wallet</b>\n\n"
            "You don't have a trading wallet connected yet.\n\n"
            "Choose an option below:\n"
            "• <b>Generate New Wallet</b>: Creates a fresh dedicated burner wallet and reveals your private key.\n"
            "• <b>Import Private Key</b>: Connect an existing burner wallet using its private key.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return

    addr, _ = tw
    sol = await asyncio.to_thread(get_sol_balance, addr)
    sol_str = f"{sol:.4f} SOL" if sol is not None else "unavailable"
    mints = await asyncio.to_thread(fetch_wallet_mints, addr)
    token_count = len(mints) if mints is not None else "unavailable"

    buttons = [
        [
            InlineKeyboardButton("🔑 Show Private Key", callback_data="twal:export"),
            InlineKeyboardButton("🔄 Reset Wallet", callback_data="twal:reset_confirm"),
        ],
        [
            InlineKeyboardButton("🔍 Birdeye", url=f"https://birdeye.so/profile/{addr}?chain=solana"),
            InlineKeyboardButton("🌐 Solscan", url=f"https://solscan.io/account/{addr}"),
        ],
    ]
    await update.effective_message.reply_text(
        "⚡ <b>My Trading Wallet</b>\n\n"
        f"💳 <b>Deposit Address:</b>\n<code>{addr}</code>\n"
        f"<i>(Tap address above to copy)</i>\n\n"
        f"💰 SOL Balance: <b>{sol_str}</b>\n"
        f"🪙 Tokens Held: <b>{token_count}</b>\n\n"
        "<i>Send SOL to your deposit address to fund your trading wallet.</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(buttons),
        disable_web_page_preview=True,
    )


async def cmd_create_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generates a fresh Solana keypair and displays address & private key."""
    user_id = update.effective_user.id
    addr, pk = generate_new_wallet()
    db_set_trading_wallet(user_id, addr, pk)

    buttons = InlineKeyboardMarkup([[
        InlineKeyboardButton("💼 View Wallet Dashboard", callback_data="twal:view"),
    ]])
    await update.effective_message.reply_text(
        "🎉 <b>New Trading Wallet Created!</b>\n\n"
        f"💳 <b>Deposit Address:</b>\n<code>{addr}</code>\n"
        f"<i>(Tap address above to copy)</i>\n\n"
        f"🔑 <b>Private Key:</b>\n<code>{pk}</code>\n"
        f"<i>(Tap private key above to copy)</i>\n\n"
        "⚠️ <b>IMPORTANT:</b>\n"
        "• Tap the private key above to copy and save it into Phantom or Solflare.\n"
        "• Send SOL to your deposit address to begin trading.\n"
        "• Use /my_wallet anytime to view your balance.",
        parse_mode="HTML",
        reply_markup=buttons,
    )
    return ConversationHandler.END


async def cmd_import_wallet_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for /import_wallet. Accepts key as argument: /import_wallet <key> or prompts."""
    user_id = update.effective_user.id

    if context.args and len(context.args) > 0:
        raw_key = context.args[0].strip()
        validated = parse_and_validate_private_key(raw_key)
        if validated:
            addr, pk = validated
            db_set_trading_wallet(user_id, addr, pk)
            try:
                await update.effective_message.delete()
            except Exception:
                pass
            sol = await asyncio.to_thread(get_sol_balance, addr)
            sol_str = f"{sol:.4f} SOL" if sol is not None else "unavailable"
            mints = await asyncio.to_thread(fetch_wallet_mints, addr)
            token_count = len(mints) if mints is not None else "unavailable"
            await update.effective_message.reply_text(
                "✅ <b>Trading Wallet Connected!</b>\n\n"
                f"💳 <b>Address:</b>\n<code>{addr}</code>\n"
                f"<i>(Tap address above to copy)</i>\n\n"
                f"💰 SOL Balance: <b>{sol_str}</b>\n"
                f"🪙 Tokens Held: <b>{token_count}</b>\n\n"
                "Use /my_wallet anytime to view your dashboard.",
                parse_mode="HTML",
            )
            return ConversationHandler.END
        else:
            await update.effective_message.reply_text(
                "❌ That is not a valid Solana private key. Try again or /cancel."
            )
            return ASK_IMPORT_KEY

    await update.effective_message.reply_text(
        "📥 <b>Import Existing Trading Wallet</b>\n\n"
        "Send your Solana private key (base58 format or JSON byte array):\n"
        "<i>(We recommend using a dedicated burner trading wallet)</i>\n\n"
        "Type /cancel to abort.",
        parse_mode="HTML",
    )
    return ASK_IMPORT_KEY


async def recv_import_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw_key = update.effective_message.text.strip()
    validated = parse_and_validate_private_key(raw_key)
    if not validated:
        await update.effective_message.reply_text(
            "❌ That is not a valid Solana private key. Please check and try again, or /cancel."
        )
        return ASK_IMPORT_KEY

    addr, pk = validated
    user_id = update.effective_user.id
    db_set_trading_wallet(user_id, addr, pk)

    # Delete the message containing the private key to protect user privacy
    try:
        await update.effective_message.delete()
    except Exception:
        pass

    sol = await asyncio.to_thread(get_sol_balance, addr)
    sol_str = f"{sol:.4f} SOL" if sol is not None else "unavailable"
    mints = await asyncio.to_thread(fetch_wallet_mints, addr)
    token_count = len(mints) if mints is not None else "unavailable"

    await update.effective_message.reply_text(
        "✅ <b>Trading Wallet Connected!</b>\n\n"
        f"💳 <b>Address:</b>\n<code>{addr}</code>\n"
        f"<i>(Tap address above to copy)</i>\n\n"
        f"💰 SOL Balance: <b>{sol_str}</b>\n"
        f"🪙 Tokens Held: <b>{token_count}</b>\n\n"
        "Use /my_wallet anytime to view your balance and actions.",
        parse_mode="HTML",
    )
    return ConversationHandler.END


async def on_trading_wallet_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data or ""
    await query.answer()

    if data == "twal:create":
        addr, pk = generate_new_wallet()
        db_set_trading_wallet(user_id, addr, pk)
        buttons = InlineKeyboardMarkup([[
            InlineKeyboardButton("💼 View Wallet Dashboard", callback_data="twal:view"),
        ]])
        await query.edit_message_text(
            "🎉 <b>New Trading Wallet Created!</b>\n\n"
            f"💳 <b>Deposit Address:</b>\n<code>{addr}</code>\n"
            f"<i>(Tap address above to copy)</i>\n\n"
            f"🔑 <b>Private Key:</b>\n<code>{pk}</code>\n"
            f"<i>(Tap private key above to copy)</i>\n\n"
            "⚠️ <b>IMPORTANT:</b>\n"
            "• Tap the private key above to copy and save it into Phantom or Solflare.\n"
            "• Send SOL to your deposit address to begin trading.\n"
            "• Use /my_wallet anytime to view your balance.",
            parse_mode="HTML",
            reply_markup=buttons,
        )
        return

    if data == "twal:import_prompt":
        await query.edit_message_text(
            "📥 <b>Import Trading Wallet</b>\n\n"
            "To connect your existing trading wallet, send your private key:\n"
            "👉 Send: <code>/import_wallet YOUR_PRIVATE_KEY</code>\n\n"
            "Or simply type /import_wallet and follow the prompt.",
            parse_mode="HTML",
        )
        return

    if data == "twal:export":
        tw = db_get_trading_wallet(user_id)
        if not tw:
            await query.edit_message_text("No trading wallet found. Use /create_wallet.")
            return
        _, pk = tw
        buttons = InlineKeyboardMarkup([[
            InlineKeyboardButton("◀️ Back to Wallet", callback_data="twal:view"),
        ]])
        await query.edit_message_text(
            "🔑 <b>Your Trading Private Key:</b>\n\n"
            f"<code>{pk}</code>\n\n"
            "<i>(Tap key above to copy directly into Phantom or Solflare)</i>\n\n"
            "⚠️ <b>Never share this key with anyone!</b>",
            parse_mode="HTML",
            reply_markup=buttons,
        )
        return

    if data == "twal:reset_confirm":
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚠️ Yes, Disconnect Wallet", callback_data="twal:reset_yes")],
            [InlineKeyboardButton("❌ Cancel", callback_data="twal:view")],
        ])
        await query.edit_message_text(
            "⚠️ <b>Disconnect Trading Wallet?</b>\n\n"
            "Make sure you have saved your private key before proceeding!\n"
            "Are you sure you want to disconnect?",
            parse_mode="HTML",
            reply_markup=buttons,
        )
        return

    if data == "twal:reset_yes":
        db_remove_trading_wallet(user_id)
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("🆕 Generate New Wallet", callback_data="twal:create")],
            [InlineKeyboardButton("📥 Import Private Key", callback_data="twal:import_prompt")],
        ])
        await query.edit_message_text(
            "🗑 <b>Trading wallet disconnected.</b>\n\n"
            "You can generate a new wallet or import an existing private key anytime.",
            parse_mode="HTML",
            reply_markup=buttons,
        )
        return

    if data == "twal:view":
        tw = db_get_trading_wallet(user_id)
        if not tw:
            buttons = [
                [InlineKeyboardButton("🆕 Generate New Wallet", callback_data="twal:create")],
                [InlineKeyboardButton("📥 Import Private Key", callback_data="twal:import_prompt")],
            ]
            await query.edit_message_text(
                "⚡ <b>Solana Trading Wallet</b>\n\n"
                "You don't have a trading wallet connected yet.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(buttons),
            )
            return

        addr, _ = tw
        sol = await asyncio.to_thread(get_sol_balance, addr)
        sol_str = f"{sol:.4f} SOL" if sol is not None else "unavailable"
        mints = await asyncio.to_thread(fetch_wallet_mints, addr)
        token_count = len(mints) if mints is not None else "unavailable"

        buttons = [
            [
                InlineKeyboardButton("🔑 Show Private Key", callback_data="twal:export"),
                InlineKeyboardButton("🔄 Reset Wallet", callback_data="twal:reset_confirm"),
            ],
            [
                InlineKeyboardButton("🔍 Birdeye", url=f"https://birdeye.so/profile/{addr}?chain=solana"),
                InlineKeyboardButton("🌐 Solscan", url=f"https://solscan.io/account/{addr}"),
            ],
        ]
        await query.edit_message_text(
            "⚡ <b>My Trading Wallet</b>\n\n"
            f"💳 <b>Deposit Address:</b>\n<code>{addr}</code>\n"
            f"<i>(Tap address above to copy)</i>\n\n"
            f"💰 SOL Balance: <b>{sol_str}</b>\n"
            f"🪙 Tokens Held: <b>{token_count}</b>\n\n"
            "<i>Send SOL to your deposit address to fund your trading wallet.</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(buttons),
            disable_web_page_preview=True,
        )
        return



# --- Watched Wallets --------------------------------------------------------

async def cmd_new_watch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if db_count(user_id) >= MAX_WALLETS:
        await update.effective_message.reply_text(
            f"You already track {MAX_WALLETS} wallets. Remove one with /remove_watch first."
        )
        return ConversationHandler.END

    context.user_data.clear()
    await update.effective_message.reply_text("Send the Solana wallet address to watch:")
    return ASK_ADDRESS


async def recv_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    addr = update.effective_message.text.strip()
    if not (32 <= len(addr) <= 44) or not all(c in BASE58_ALPHABET for c in addr):
        await update.effective_message.reply_text(
            "That does not look like a Solana address. Try again or /cancel."
        )
        return ASK_ADDRESS
    context.user_data["address"] = addr
    await update.effective_message.reply_text("Give this wallet a short name:")
    return ASK_NAME


async def recv_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.effective_message.text.strip()
    if not name or len(name) > 32:
        await update.effective_message.reply_text(
            "Name must be 1-32 characters. Try again or /cancel."
        )
        return ASK_NAME

    user_id = update.effective_user.id
    addr = context.user_data.get("address")
    if not addr:
        await update.effective_message.reply_text(
            "Something went wrong. Start over with /new_watch."
        )
        context.user_data.clear()
        return ConversationHandler.END

    if not db_add(user_id, name, addr):
        await update.effective_message.reply_text(
            f"You already have a wallet named <b>{html.escape(name)}</b>. "
            "Try a different name or /cancel.",
            parse_mode="HTML",
        )
        return ASK_NAME

    mints = await asyncio.to_thread(fetch_wallet_mints, addr)
    if mints is None:
        await update.effective_message.reply_text(
            f"Saved <b>{html.escape(name)}</b> → <code>{addr}</code>\n\n"
            "⚠️ Could not verify this wallet on-chain right now. "
            "The bot will automatically connect and verify on the next cycle.",
            parse_mode="HTML",
        )
    else:
        baselines[(user_id, addr)] = mints
        await update.effective_message.reply_text(
            f"✅ Now watching <b>{html.escape(name)}</b>\n"
            f"<code>{addr}</code>\n\n"
            f"Baseline: <b>{len(mints)}</b> token(s) currently held.\n"
            "I will alert you the moment a new token is bought or sold.",
            parse_mode="HTML",
        )

    context.user_data.clear()
    return ConversationHandler.END


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.effective_message.reply_text("Cancelled.")
    return ConversationHandler.END


async def cmd_my_watches(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    rows = db_list(user_id)
    if not rows:
        await update.effective_message.reply_text("No wallets tracked. Use /new_watch.")
        return
    lines = [f"<b>Your watched wallets</b> ({len(rows)}/{MAX_WALLETS})"]
    for name, addr, paused in rows:
        if paused:
            icon = "⏸"
        elif (user_id, addr) in baselines:
            icon = "🟢"
        else:
            icon = "🟡"
        lines.append(
            f"{icon} <code>{html.escape(name)}</code> — "
            f"<code>{addr[:4]}...{addr[-4:]}</code>"
        )
    lines.append("\n🟢 active  🟡 verifying  ⏸ paused")
    await update.effective_message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Redirect to user wallets list cleanly without exposing internal server metrics."""
    await cmd_my_watches(update, context)



# --- Pause / Resume flow (callback-based) -----------------------------------

def _pause_keyboard(rows):
    buttons = []
    for name, addr, paused in rows:
        label = f"▶️ Resume {name}" if paused else f"⏸ Pause {name}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"pg:toggle:{name}")])
    return InlineKeyboardMarkup(buttons)


async def cmd_pause_watch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    rows = db_list(user_id)
    if not rows:
        await update.effective_message.reply_text("No wallets tracked. Use /new_watch first.")
        return
    await update.effective_message.reply_text(
        "<b>Tap a wallet to pause or resume monitoring:</b>",
        parse_mode="HTML",
        reply_markup=_pause_keyboard(rows),
    )


async def on_pause_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data or ""
    await query.answer()

    parts = data.split(":", 2)
    if len(parts) == 3 and parts[0] == "pg" and parts[1] == "toggle":
        name = parts[2]
        rows = db_list(user_id)
        lookup = {n: bool(p) for n, _, p in rows}

        if name not in lookup:
            await query.edit_message_text(
                f"Wallet <b>{html.escape(name)}</b> is no longer tracked.",
                parse_mode="HTML",
            )
            return

        new_state = not lookup[name]
        db_set_paused(user_id, name, new_state)
        action = "Paused ⏸" if new_state else "Resumed ▶️"

        fresh_rows = db_list(user_id)
        await query.edit_message_text(
            f"{action} <b>{html.escape(name)}</b>\n\n"
            "<b>Tap a wallet to pause or resume monitoring:</b>",
            parse_mode="HTML",
            reply_markup=_pause_keyboard(fresh_rows),
        )
        return

    await query.edit_message_text("Something went wrong. Try /pause_watch again.")


# --- Remove flow (callback-based, no argument needed) -----------------------

async def cmd_remove_watch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    rows = db_list(user_id)

    if not rows:
        await update.effective_message.reply_text("No wallets tracked. Use /new_watch first.")
        return

    buttons = [
        [InlineKeyboardButton(
            f"🗑 {name}  ({addr[:4]}...{addr[-4:]})",
            callback_data=f"rm:pick:{name}"
        )]
        for name, addr, _ in rows
    ]
    keyboard = InlineKeyboardMarkup(buttons)
    await update.effective_message.reply_text(
        "<b>Which wallet do you want to remove?</b>",
        parse_mode="HTML",
        reply_markup=keyboard,
    )


async def on_remove_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data or ""

    # Acknowledge the tap, otherwise Telegram shows a spinner on the button.
    await query.answer()

    parts = data.split(":", 2)

    # --- Pick: user chose a wallet from the list ---
    if len(parts) == 3 and parts[0] == "rm" and parts[1] == "pick":
        name = parts[2]
        # The wallet may have been removed by another session. Re-check
        # before showing confirm.
        existing = {n: a for n, a, _ in db_list(user_id)}
        if name not in existing:
            await query.edit_message_text(
                f"Wallet <b>{html.escape(name)}</b> is no longer tracked.",
                parse_mode="HTML",
            )
            return

        addr = existing[name]
        confirm_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Confirm remove", callback_data=f"rm:yes:{name}"),
            InlineKeyboardButton("❌ Cancel", callback_data="rm:no"),
        ]])
        await query.edit_message_text(
            f"<b>Remove '{html.escape(name)}'?</b>\n<code>{addr}</code>",
            parse_mode="HTML",
            reply_markup=confirm_kb,
        )
        return

    # --- Yes: user confirmed ---
    if len(parts) == 3 and parts[0] == "rm" and parts[1] == "yes":
        name = parts[2]
        # Read addr BEFORE deleting so we can pop the baseline cleanly.
        existing = {n: a for n, a, _ in db_list(user_id)}
        addr = existing.get(name)
        removed = db_remove(user_id, name)
        if removed:
            if addr:
                baselines.pop((user_id, addr), None)
            await query.edit_message_text(
                f"✅ Removed <b>{html.escape(name)}</b>.",
                parse_mode="HTML",
            )
        else:
            await query.edit_message_text(
                f"Wallet <b>{html.escape(name)}</b> was already gone.",
                parse_mode="HTML",
            )
        return

    # --- No: user cancelled ---
    if data == "rm:no":
        await query.edit_message_text("Removal cancelled.")
        return

    # Unknown payload — fail gracefully.
    await query.edit_message_text("Something went wrong. Try /remove_watch again.")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error("Telegram handler error: %s", context.error)


# --- Monitor ----------------------------------------------------------------
# Analysis: baselines keyed by (user_id, address), not row id, so removing and
# re-adding the same wallet keeps the baseline. First sight is silent — alerting
# on initial state would dump every token the wallet ever held into the chat.

baselines = {}


def fetch_wallet_mints(address):
    # Returns None on RPC failure so the caller can tell "empty wallet" apart
    # from "request failed". The two must not be conflated in the monitor.
    # Queries BOTH TOKEN_PROGRAM and TOKEN_2022_PROGRAM so Token-2022 purchases
    # (e.g. pump.fun tokens launched post-migration) are detected too.
    mints = set()

    for program_id in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
        res = rpc("getTokenAccountsByOwner", [
            address,
            {"programId": program_id},
            {"encoding": "jsonParsed"},
        ])
        if res is None:
            # If either side fails, treat the whole call as failed.
            # This prevents a partial baseline that would silently miss one
            # token program and corrupt the diff on the next cycle.
            return None
        for item in res.get("value", []):
            try:
                info = item["account"]["data"]["parsed"]["info"]
                mints.add(info["mint"])
            except (KeyError, TypeError):
                continue

    return mints


async def send_buy_alert(app, user_id, name, mint, address):
    """Notification when a new token appears. Fetches SOL balance only when an alert fires."""
    sol = await asyncio.to_thread(get_sol_balance, address)
    sol_str = f"{sol:.4f} SOL" if sol is not None else "unavailable"
    try:
        await app.bot.send_message(
            chat_id=user_id,
            text=(
                f"🚨 <b>{html.escape(name)}</b> just bought a token!\n"
                f"CA: <code>{mint}</code>\n"
                f"Wallet balance: <b>{sol_str}</b>\n"
                f"<i>Scanning chain for risks...</i>"
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        log.warning("buy alert send failed: %s", e)


async def send_sell_alert(app, user_id, name, mint, address):
    """Notification when a token leaves the wallet (sold or transferred out)."""
    sol = await asyncio.to_thread(get_sol_balance, address)
    sol_str = f"{sol:.4f} SOL" if sol is not None else "unavailable"
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔍 Birdeye", url=f"https://birdeye.so/token/{mint}"),
    ]])
    try:
        await app.bot.send_message(
            chat_id=user_id,
            text=(
                f"📤 <b>{html.escape(name)}</b> sold / transferred a token\n"
                f"CA: <code>{mint}</code>\n"
                f"Wallet balance: <b>{sol_str}</b>"
            ),
            parse_mode="HTML",
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
    except Exception as e:
        log.warning("sell alert send failed: %s", e)


async def send_verdict(app, user_id, mint):
    # Off the event loop — check_rug does blocking HTTP.
    try:
        verdict, score, risks = await asyncio.to_thread(check_rug, mint)
    except Exception as e:
        log.error("check_rug crashed for %s: %s", mint, e)
        verdict, score, risks = ("UNKNOWN", 0, ["data unavailable"])

    emoji = {"SAFE": "✅", "WARN": "⚠️", "DANGER": "🔴", "UNKNOWN": "❓"}.get(verdict, "❓")
    risk_text = (
        "\n".join(f"• {html.escape(r)}" for r in risks)
        if risks
        else "No structural red flags found."
    )
    text = (
        f"{emoji} <b>{verdict}</b> — safety score {score}/100\n"
        f"CA: <code>{mint}</code>\n\n"
        f"{risk_text}"
    )

    # Deep links only — the user signs in their own wallet. The bot never
    # touches a private key, so there is nothing to compromise here.
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "🔍 Birdeye",
            url=f"https://birdeye.so/token/{mint}",
        ),
        InlineKeyboardButton(
            "⚡ Trade on Jupiter",
            url=f"https://jup.ag/swap/SOL-{mint}",
        ),
    ]])

    try:
        await app.bot.send_message(
            chat_id=user_id,
            text=text,
            parse_mode="HTML",
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
    except Exception as e:
        log.warning("verdict send failed: %s", e)


async def monitor_loop(app):
    log.info("monitor loop started")
    while True:
        cycle_start = time.time()
        new_count = 0
        sell_count = 0
        fail_count = 0
        wallet_count = 0

        try:
            wallets = db_all_wallets()
            wallet_count = len(wallets)
            still_present = set()

            for user_id, name, address, paused in wallets:
                key = (user_id, address)
                # Keep paused wallets in still_present so their baselines survive.
                still_present.add(key)

                if paused:
                    # Skip RPC polling while paused.
                    continue

                # Per-wallet try/except — one bad address or one RPC hiccup
                # must not take down the whole cycle.
                try:
                    mints = await asyncio.to_thread(fetch_wallet_mints, address)
                    if mints is None:
                        fail_count += 1
                        # RPC failed. Skip WITHOUT overwriting the baseline —
                        # otherwise the failure looks like a full drain and
                        # we would re-alert on everything next poll.
                        continue

                    if key not in baselines:
                        baselines[key] = mints
                        log.info(
                            "baseline seeded for %s/%s (%d mints)",
                            user_id, name, len(mints)
                        )
                        continue

                    new_mints = mints - baselines[key]    # tokens that appeared (buy)
                    sold_mints = baselines[key] - mints   # tokens that left (sell)

                    if new_mints or sold_mints:
                        # Update baseline BEFORE awaiting sends, so a slow
                        # Telegram call cannot cause a re-alert next cycle.
                        baselines[key] = mints

                        for mint in new_mints:
                            new_count += 1
                            log.info("buy: mint %s for %s (%s)", mint, name, user_id)
                            await send_buy_alert(app, user_id, name, mint, address)
                            await send_verdict(app, user_id, mint)

                        for mint in sold_mints:
                            sell_count += 1
                            log.info("sell: mint %s for %s (%s)", mint, name, user_id)
                            await send_sell_alert(app, user_id, name, mint, address)

                except Exception as e:
                    fail_count += 1
                    log.error("wallet %s (%s) processing failed: %s", name, address, e)
                    continue

            # Drop baselines for wallets removed from the DB.
            for key in list(baselines.keys()):
                if key not in still_present:
                    del baselines[key]

        except Exception as e:
            log.error("monitor loop iteration failed: %s", e)

        # Heartbeat + state for /status
        cycle_ms = int((time.time() - cycle_start) * 1000)
        state["last_cycle_ts"] = time.time()
        state["last_cycle_ms"] = cycle_ms
        state["last_cycle_wallets"] = wallet_count
        state["last_cycle_new"] = new_count
        state["last_cycle_sells"] = sell_count
        state["last_cycle_failures"] = fail_count
        log.info(
            "poll: %d wallets, %d buys, %d sells, %d failures, %dms",
            wallet_count, new_count, sell_count, fail_count, cycle_ms,
        )

        await asyncio.sleep(POLL_INTERVAL)


# --- Entry point ------------------------------------------------------------

async def post_init(app):
    try:
        await app.bot.set_my_commands([
            BotCommand("start",          "Introduce bot and show commands"),
            BotCommand("my_wallet",      "Manage trading wallet (generate or import)"),
            BotCommand("create_wallet",   "Generate a fresh trading wallet"),
            BotCommand("import_wallet",  "Import an existing trading private key"),
            BotCommand("new_watch",      "Watch a Solana wallet (up to 10)"),
            BotCommand("my_watches",     "List tracked wallets"),
            BotCommand("pause_watch",    "Pause or resume watching a wallet"),
            BotCommand("remove_watch",   "Remove a watched wallet"),
            BotCommand("cancel",         "Cancel current operation"),
        ])
        log.info("Bot commands registered")
    except Exception as e:
        log.warning("set_my_commands failed: %s", e)
    asyncio.create_task(monitor_loop(app))


def main():
    start_health_server()
    db_init()

    # Ensure an active asyncio event loop exists on this thread
    # (required for Python 3.12+ / 3.14 which removed implicit loop creation).
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    application = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .connect_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .pool_timeout(30.0)
        .build()
    )

    # ConversationHandler for importing a trading wallet private key
    import_wallet_conv = ConversationHandler(
        entry_points=[
            CommandHandler(["import_wallet", "importwallet", "add_wallet", "addwallet"], cmd_import_wallet_entry),
        ],
        states={
            ASK_IMPORT_KEY: [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_import_key)],
        },
        fallbacks=[
            CommandHandler("cancel",                         cmd_cancel),
            CommandHandler(["start", "help"],                cmd_start),
            CommandHandler(["my_wallet", "mywallet", "wallet"], cmd_my_wallet),
            CommandHandler(["create_wallet", "createwallet"], cmd_create_wallet),
            CommandHandler(["my_watches", "mywatches"],      cmd_my_watches),
            CommandHandler(["new_watch", "newwatch"],        cmd_new_watch),
            CommandHandler(["pause_watch", "pausewatch"],    cmd_pause_watch),
            CommandHandler(["remove_watch", "removewatch"],  cmd_remove_watch),
        ],
        allow_reentry=True,
    )

    # ConversationHandler for the /new_watch multi-step flow.
    new_watch = ConversationHandler(
        entry_points=[CommandHandler(["new_watch", "newwatch"], cmd_new_watch)],
        states={
            ASK_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_address)],
            ASK_NAME:    [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_name)],
        },
        fallbacks=[
            CommandHandler("cancel",                         cmd_cancel),
            CommandHandler(["start", "help"],                cmd_start),
            CommandHandler(["my_wallet", "mywallet", "wallet"], cmd_my_wallet),
            CommandHandler(["create_wallet", "createwallet"], cmd_create_wallet),
            CommandHandler(["import_wallet", "importwallet", "add_wallet", "addwallet"], cmd_import_wallet_entry),
            CommandHandler(["my_watches", "mywatches"],      cmd_my_watches),
            CommandHandler(["pause_watch", "pausewatch"],    cmd_pause_watch),
            CommandHandler(["remove_watch", "removewatch"],  cmd_remove_watch),
        ],
        allow_reentry=True,
    )

    application.add_handler(CommandHandler(["start", "help"], cmd_start))
    application.add_handler(CommandHandler(["my_wallet", "mywallet", "wallet"], cmd_my_wallet))
    application.add_handler(CommandHandler(["create_wallet", "createwallet"], cmd_create_wallet))
    application.add_handler(import_wallet_conv)
    application.add_handler(new_watch)
    application.add_handler(CommandHandler(["my_watches", "mywatches"], cmd_my_watches))
    application.add_handler(CommandHandler(["pause_watch", "pausewatch"], cmd_pause_watch))
    application.add_handler(CommandHandler(["remove_watch", "removewatch"], cmd_remove_watch))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CallbackQueryHandler(on_trading_wallet_callback, pattern=r"^twal:"))
    application.add_handler(CallbackQueryHandler(on_remfove_callback, pattern=r"^rm:"))
    application.add_handler(CallbackQueryHandler(on_pause_callback, pattern=r"^pg:"))
    application.add_handler(CommandHandler("cancel", cmd_cancel))
    application.add_error_handler(on_error)

    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,   # Do not replay stale messages after redeployment
    )


if __name__ == "__main__":
    main()



