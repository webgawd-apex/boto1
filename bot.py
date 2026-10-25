# bot.py
import os
import asyncio
import sqlite3
import base64
import struct
import logging
import html
import time
from dotenv import load_dotenv
import requests

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

MAX_WALLETS = 10
POLL_INTERVAL = 15
DB_FILE = os.getenv("DB_FILE", "wallets.db")

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

def db_init():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            address TEXT NOT NULL,
            UNIQUE(user_id, name)
        )
    """)
    conn.commit()
    conn.close()


def db_add(user_id, name, address):
    conn = sqlite3.connect(DB_FILE)
    try:
        conn.execute(
            "INSERT INTO wallets (user_id, name, address) VALUES (?, ?, ?)",
            (user_id, name, address),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def db_count(user_id):
    conn = sqlite3.connect(DB_FILE)
    n = conn.execute("SELECT COUNT(*) FROM wallets WHERE user_id=?", (user_id,)).fetchone()[0]
    conn.close()
    return n


def db_list(user_id):
    conn = sqlite3.connect(DB_FILE)
    rows = conn.execute(
        "SELECT name, address FROM wallets WHERE user_id=? ORDER BY id", (user_id,)
    ).fetchall()
    conn.close()
    return rows


def db_remove(user_id, name):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.execute("DELETE FROM wallets WHERE user_id=? AND name=?", (user_id, name))
    conn.commit()
    removed = cur.rowcount > 0
    conn.close()
    return removed


def db_all_wallets():
    conn = sqlite3.connect(DB_FILE)
    rows = conn.execute("SELECT user_id, name, address FROM wallets").fetchall()
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
        return ("UNKNOWN", 0, ["rpc error"])

    if len(data) < 82:
        return ("UNKNOWN", 0, ["rpc error"])

    # Step 2: classic authorities.
    try:
        has_mint_auth, has_freeze_auth = parse_mint_account(data)
    except Exception:
        return ("UNKNOWN", 0, ["rpc error"])

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
ASK_ADDRESS, ASK_NAME = range(2)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    text = (
        "👋 <b>Solana Wallet Watcher</b>\n\n"
        "I monitor Solana wallets in real time and alert you when they buy "
        "or receive new tokens — complete with an on-chain safety score.\n\n"
        "<b>Commands:</b>\n"
        "• /new_watch — Add a wallet to watch (max 10)\n"
        "• /my_watches — List your tracked wallets\n"
        "• /remove_watch — Remove a wallet (pick from list)\n"
        "• /status — Bot health and monitor state\n"
        "• /cancel — Abort the current operation"
    )
    await update.effective_message.reply_text(text, parse_mode="HTML")
    return ConversationHandler.END


async def cmd_new_watch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Enforce the cap at the entry point, not mid-conversation — a user
    # should not get blocked after they have already typed an address.
    if db_count(user_id) >= MAX_WALLETS:
        await update.effective_message.reply_text(
            f"You already track {MAX_WALLETS} wallets. Remove one with /remove_watch first."
        )
        return ConversationHandler.END

    context.user_data.clear()
    await update.effective_message.reply_text("Send the Solana wallet address to watch:")
    return ASK_ADDRESS


async def recv_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Use effective_message throughout — works in DM, groups, and forwarded updates.
    addr = update.effective_message.text.strip()
    # Shallow validation only — 32-44 base58 chars. The RPC finds nothing
    # if the address is wrong; we do not verify the checksum here.
    if not (32 <= len(addr) <= 44) or not all(c in BASE58_ALPHABET for c in addr):
        await update.effective_message.reply_text(
            "That does not look like a Solana address. Try again or /cancel."
        )
        return ASK_ADDRESS
    context.user_data["address"] = addr
    await update.effective_message.reply_text("Give this wallet a short name:")
    return ASK_NAME


async def recv_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Analysis: after the wallet is saved, fetch its current tokens and seed
    # the baseline right here. This proves the Helius key works and the wallet
    # is readable at the moment the user adds it. If the RPC fails here, the
    # user is told now, instead of silently watching nothing until they happen
    # to run /status.
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
        # Return to ASK_NAME so the user can pick a different name without
        # restarting the whole flow.
        return ASK_NAME

    # Seed the baseline immediately so the first real purchase triggers an alert,
    # not a false-positive dump of every token the wallet already holds.
    mints = await asyncio.to_thread(fetch_wallet_mints, addr)
    if mints is None:
        await update.effective_message.reply_text(
            f"Saved <b>{html.escape(name)}</b> → <code>{addr}</code>\n\n"
            "⚠️ Could not read this wallet from Helius right now. "
            "The monitor will retry every 15 s. Use /status to check RPC health.",
            parse_mode="HTML",
        )
    else:
        baselines[(user_id, addr)] = mints
        await update.effective_message.reply_text(
            f"✅ Now watching <b>{html.escape(name)}</b>\n"
            f"<code>{addr}</code>\n\n"
            f"Baseline: <b>{len(mints)}</b> token(s) currently held.\n"
            "I will alert you the moment a new token appears.",
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
    lines = [f"<b>Your wallets</b> ({len(rows)}/{MAX_WALLETS})"]
    for name, addr in rows:
        seeded = "🟢" if (user_id, addr) in baselines else "🟡"
        lines.append(
            f"{seeded} <code>{html.escape(name)}</code> — "
            f"<code>{addr[:4]}...{addr[-4:]}</code>"
        )
    lines.append("\n🟢 = monitoring active  🟡 = baseline pending (next poll)")
    await update.effective_message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Analysis: the only way to prove the monitor is alive from the user side.
    # Shows DB counts, baseline count, last cycle stats, and Helius reachability.
    user_id = update.effective_user.id
    my_wallets = db_count(user_id)
    total_wallets = len(db_all_wallets())
    active_baselines = len(baselines)

    if state["last_cycle_ts"] == 0:
        cycle_line = "Never run yet (waiting on first 15 s tick)."
    else:
        ago = int(time.time() - state["last_cycle_ts"])
        cycle_line = (
            f"{ago}s ago · {state['last_cycle_ms']}ms · "
            f"{state['last_cycle_wallets']} wallets · "
            f"{state['last_cycle_new']} new · "
            f"{state['last_cycle_failures']} failures"
        )

    if state["last_rpc_ok"] is None:
        rpc_line = "not attempted yet"
    elif state["last_rpc_ok"]:
        rpc_line = "✅ last RPC succeeded"
    else:
        rpc_line = "❌ last RPC failed (check HELIUS_KEY)"

    uptime_s = int(time.time() - state["started_at"])
    h, rem = divmod(uptime_s, 3600)
    m, s = divmod(rem, 60)
    uptime_str = f"{h}h {m}m {s}s" if h else f"{m}m {s}s"

    text = (
        "<b>Bot Status</b>\n"
        f"⏱ Uptime: {uptime_str}\n"
        f"👛 Your wallets: {my_wallets}/{MAX_WALLETS}\n"
        f"🌐 Total wallets (all users): {total_wallets}\n"
        f"📡 Active baselines: {active_baselines}\n"
        f"🔄 Last cycle: {cycle_line}\n"
        f"☁️ Helius: {rpc_line}\n"
        f"⏰ Poll interval: {POLL_INTERVAL}s"
    )
    await update.effective_message.reply_text(text, parse_mode="HTML")


# --- Remove flow (callback-based, no argument needed) -----------------------
# Analysis: /remove_watch takes no argument. It renders the user's wallets as
# a vertical inline keyboard. Tapping a wallet edits the same message into a
# Confirm / Cancel pair, so the conversation stays on one message and there is
# no drift between "the list I saw" and "the wallet I meant".
#
# Callback payload format:
#   rm:pick:<name>   -> user tapped a wallet, show confirm
#   rm:yes:<name>    -> user confirmed
#   rm:no            -> user cancelled

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
        for name, addr in rows
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
        existing = dict(db_list(user_id))
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
        existing = dict(db_list(user_id))
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


async def send_alert(app, user_id, name, mint):
    try:
        await app.bot.send_message(
            chat_id=user_id,
            text=(
                f"🚨 <b>{html.escape(name)}</b> just bought a token!\n"
                f"CA: <code>{mint}</code>\n"
                f"<i>Scanning chain for risks...</i>"
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        log.warning("alert send failed: %s", e)


async def send_verdict(app, user_id, mint):
    # Off the event loop — check_rug does blocking HTTP.
    try:
        verdict, score, risks = await asyncio.to_thread(check_rug, mint)
    except Exception as e:
        log.error("check_rug crashed for %s: %s", mint, e)
        verdict, score, risks = ("UNKNOWN", 0, ["rpc error"])

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
        fail_count = 0
        wallet_count = 0

        try:
            wallets = db_all_wallets()
            wallet_count = len(wallets)
            still_present = set()

            for user_id, name, address in wallets:
                key = (user_id, address)
                still_present.add(key)

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

                    new_mints = mints - baselines[key]
                    if new_mints:
                        # Update baseline BEFORE awaiting sends, so a slow
                        # Telegram call cannot cause a re-alert next cycle.
                        baselines[key] = mints
                        for mint in new_mints:
                            new_count += 1
                            log.info("new mint %s for %s (%s)", mint, name, user_id)
                            await send_alert(app, user_id, name, mint)
                            await send_verdict(app, user_id, mint)

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
        state["last_cycle_failures"] = fail_count
        log.info(
            "poll: %d wallets, %d new, %d failures, %dms",
            wallet_count, new_count, fail_count, cycle_ms,
        )

        await asyncio.sleep(POLL_INTERVAL)


# --- Entry point ------------------------------------------------------------

async def post_init(app):
    try:
        await app.bot.set_my_commands([
            BotCommand("start",        "Introduce bot and show commands"),
            BotCommand("new_watch",    "Watch a Solana wallet (up to 10)"),
            BotCommand("my_watches",   "List tracked wallets"),
            BotCommand("remove_watch", "Remove a watched wallet"),
            BotCommand("status",       "Bot health and monitor state"),
            BotCommand("cancel",       "Cancel current operation"),
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

    # ConversationHandler for the /new_watch multi-step flow.
    # fallbacks list every top-level command so that if a user is stuck in
    # ASK_ADDRESS or ASK_NAME and types another command, that command executes
    # cleanly instead of being silently dropped by ~filters.COMMAND.
    new_watch = ConversationHandler(
        entry_points=[CommandHandler(["new_watch", "newwatch"], cmd_new_watch)],
        states={
            ASK_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_address)],
            ASK_NAME:    [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_name)],
        },
        fallbacks=[
            CommandHandler("cancel",                         cmd_cancel),
            CommandHandler(["start", "help"],                cmd_start),
            CommandHandler(["my_watches", "mywatches"],      cmd_my_watches),
            CommandHandler(["remove_watch", "removewatch"],  cmd_remove_watch),
            CommandHandler("status",                         cmd_status),
        ],
        allow_reentry=True,
    )

    application.add_handler(CommandHandler(["start", "help"], cmd_start))
    application.add_handler(new_watch)
    application.add_handler(CommandHandler(["my_watches", "mywatches"], cmd_my_watches))
    application.add_handler(CommandHandler(["remove_watch", "removewatch"], cmd_remove_watch))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CallbackQueryHandler(on_remove_callback, pattern=r"^rm:"))
    application.add_handler(CommandHandler("cancel", cmd_cancel))
    application.add_error_handler(on_error)

    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,   # Do not replay stale messages after redeployment
    )


if __name__ == "__main__":
    main()
