# bot.py
import os
import asyncio
import sqlite3
import base64
import struct
import logging
import html
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
# Keyed by (user_id, name) so one user can't have two wallets called "main"
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
# poll is 15s away and retries inside a cycle cause pile-ups.

def rpc(method, params):
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        r = requests.post(RPC_URL, json=body, timeout=10)
        return r.json().get("result")
    except Exception as e:
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
# Deliberately NOT here, because each needs a service we don't use:
#   - Liquidity depth (DEX API)
#   - Bundle / sniper detection (historical tx analysis)
#   - Holder wallet reputation (label service)
# Anything we can't prove with Helius RPC alone is reported as WARN, not SAFE.

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
    # Snapshot only. Isolated try/except so a bad response here doesn't
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
        "or receive new tokens, complete with an on-chain safety score.\n\n"
        "<b>Commands:</b>\n"
        "• /new_watch — Add a wallet to watch (max 10)\n"
        "• /my_watches — List your tracked wallets\n"
        "• /remove_watch — Remove a wallet (pick from list)\n"
        "• /cancel — Abort the current operation"
    )
    try:
        if update.effective_message:
            await update.effective_message.reply_text(text, parse_mode="HTML")
    except Exception as e:
        log.warning("cmd_start HTML reply failed: %s", e)
        if update.effective_message:
            await update.effective_message.reply_text(
                "Solana Wallet Watcher\n\n"
                "Commands:\n"
                "/new_watch - Add a wallet to watch (max 10)\n"
                "/my_watches - List your tracked wallets\n"
                "/remove_watch - Remove a wallet\n"
                "/cancel - Abort the current operation"
            )
    return ConversationHandler.END


async def cmd_new_watch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Enforce the cap at the entry point, not mid-conversation — a user
    # shouldn't get blocked after they've already typed an address.
    if db_count(user_id) >= MAX_WALLETS:
        if update.effective_message:
            await update.effective_message.reply_text(
                f"You already track {MAX_WALLETS} wallets. Remove one with /remove_watch first."
            )
        return ConversationHandler.END

    context.user_data.clear()
    if update.effective_message:
        await update.effective_message.reply_text("Send the Solana wallet address to watch:")
    return ASK_ADDRESS


async def recv_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    addr = update.message.text.strip()
    # Shallow validation only — 32-44 base58 chars. The RPC finds nothing
    # if the address is wrong; we don't verify the checksum here.
    if not (32 <= len(addr) <= 44) or not all(c in BASE58_ALPHABET for c in addr):
        await update.message.reply_text("That doesn't look like a Solana address. Try again or /cancel.")
        return ASK_ADDRESS
    context.user_data["address"] = addr
    await update.message.reply_text("Give this wallet a short name:")
    return ASK_NAME


async def recv_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name or len(name) > 32:
        await update.message.reply_text("Name must be 1-32 characters. Try again or /cancel.")
        return ASK_NAME

    user_id = update.effective_user.id
    addr = context.user_data.get("address")
    if not addr:
        await update.message.reply_text("Something went wrong. Start over with /new_watch.")
        context.user_data.clear()
        return ConversationHandler.END

    if not db_add(user_id, name, addr):
        await update.message.reply_text(f"You already have a wallet named '{name}'.")
        context.user_data.clear()
        return ConversationHandler.END

    await update.message.reply_text(f"Watching '{name}' → {addr}")
    context.user_data.clear()
    return ConversationHandler.END


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    if update.effective_message:
        await update.effective_message.reply_text("Cancelled.")
    return ConversationHandler.END


async def cmd_my_watches(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    rows = db_list(user_id)
    if not rows:
        if update.effective_message:
            await update.effective_message.reply_text("No wallets tracked. Use /new_watch.")
        return ConversationHandler.END
    lines = [f"<b>Your wallets</b> ({len(rows)}/{MAX_WALLETS})"]
    for name, addr in rows:
        lines.append(f"• <code>{html.escape(name)}</code> — <code>{addr[:4]}...{addr[-4:]}</code>")
    try:
        if update.effective_message:
            await update.effective_message.reply_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        log.warning("cmd_my_watches HTML failed: %s", e)
        if update.effective_message:
            plain_lines = [f"Your wallets ({len(rows)}/{MAX_WALLETS})"]
            for name, addr in rows:
                plain_lines.append(f"• {name} - {addr[:4]}...{addr[-4:]}")
            await update.effective_message.reply_text("\n".join(plain_lines))
    return ConversationHandler.END


# --- Remove flow (callback-based, no argument needed) -----------------------
# Analysis: /remove_watch takes no argument now. It renders the user's wallets
# as a vertical inline keyboard. Tapping a wallet edits the same message into
# a Confirm / Cancel pair, so the conversation stays on one message and there
# is no drift between "the list I saw" and "the wallet I meant".
#
# Callback payload format:
#   rm:pick:<name>   -> user tapped a wallet, show confirm
#   rm:yes:<name>    -> user confirmed
#   rm:no            -> user cancelled

async def cmd_remove_watch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    rows = db_list(user_id)

    if not rows:
        if update.effective_message:
            await update.effective_message.reply_text("No wallets tracked. Use /new_watch first.")
        return ConversationHandler.END

    buttons = [
        [InlineKeyboardButton(f"🗑 {name}  ({addr[:4]}...{addr[-4:]})", callback_data=f"rm:pick:{name}")]
        for name, addr in rows
    ]
    keyboard = InlineKeyboardMarkup(buttons)
    if update.effective_message:
        await update.effective_message.reply_text(
            "<b>Which wallet do you want to remove?</b>",
            parse_mode="HTML",
            reply_markup=keyboard,
        )
    return ConversationHandler.END


async def on_remove_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data or ""

    # Any tap here acknowledges the button, otherwise Telegram shows a
    # spinner on the button for ~15s.
    await query.answer()

    parts = data.split(":", 2)

    # --- Pick: user chose a wallet from the list ---
    if len(parts) == 3 and parts[0] == "rm" and parts[1] == "pick":
        name = parts[2]
        # The wallet may have been removed by /new_watch overwrite or another
        # session. Re-check before showing confirm.
        existing = dict(db_list(user_id))
        if name not in existing:
            await query.edit_message_text(
                f"Wallet '{html.escape(name)}' is no longer tracked.",
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
        existing = dict(db_list(user_id))
        addr = existing.get(name)
        removed = db_remove(user_id, name)
        if removed:
            # Also drop the baseline immediately so the wallet won't be re-scanned.
            if addr:
                baselines.pop((user_id, addr), None)

            await query.edit_message_text(
                f"✅ Removed '<b>{html.escape(name)}</b>'.",
                parse_mode="HTML",
            )
        else:
            await query.edit_message_text(
                f"Wallet '<b>{html.escape(name)}</b>' was already gone.",
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
    res = rpc("getTokenAccountsByOwner", [
        address,
        {"programId": TOKEN_PROGRAM},
        {"encoding": "jsonParsed"},
    ])
    if not res:
        return None
    mints = set()
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
            text=f"🚨 <b>{html.escape(name)}</b> bought\nCA: <code>{mint}</code>\n<i>scanning chain...</i>",
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
    risk_text = "\n".join(f"• {html.escape(r)}" for r in risks) if risks else "No structural red flags"
    text = (
        f"{emoji} <b>{verdict}</b> — safety {score}/100\n"
        f"CA: <code>{mint}</code>\n"
        f"{risk_text}"
    )

    # Deep link only — the user signs in their own wallet. The bot never
    # touches a private key, so there is nothing to compromise here.
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "Trade on Jupiter",
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
        try:
            wallets = db_all_wallets()
            still_present = set()

            for user_id, name, address in wallets:
                key = (user_id, address)
                still_present.add(key)

                # Per-wallet try/except — one bad address or one RPC hiccup
                # must not take down the whole cycle.
                try:
                    mints = await asyncio.to_thread(fetch_wallet_mints, address)
                    if mints is None:
                        # RPC failed. Skip WITHOUT overwriting the baseline —
                        # otherwise the failure looks like a full drain and
                        # we'd re-alert on everything next poll.
                        continue

                    if key not in baselines:
                        baselines[key] = mints
                        log.info("baseline set for %s/%s (%d mints)", user_id, name, len(mints))
                        continue

                    new_mints = mints - baselines[key]
                    if new_mints:
                        # Update baseline BEFORE awaiting sends, so a slow
                        # Telegram call can't cause a re-alert next cycle.
                        baselines[key] = mints
                        for mint in new_mints:
                            await send_alert(app, user_id, name, mint)
                            await send_verdict(app, user_id, mint)

                except Exception as e:
                    log.error("wallet %s (%s) processing failed: %s", name, address, e)
                    continue

            # Drop baselines for wallets removed from the DB.
            for key in list(baselines.keys()):
                if key not in still_present:
                    del baselines[key]

        except Exception as e:
            log.error("monitor loop iteration failed: %s", e)

        await asyncio.sleep(POLL_INTERVAL)


# --- Entry point ------------------------------------------------------------

async def post_init(app):
    try:
        await app.bot.set_my_commands([
            BotCommand("start", "Introduce bot and show commands"),
            BotCommand("new_watch", "Watch a Solana wallet (up to 10)"),
            BotCommand("my_watches", "List tracked wallets"),
            BotCommand("remove_watch", "Remove a watched wallet"),
            BotCommand("cancel", "Cancel current operation"),
        ])
        log.info("Bot commands registered")
    except Exception as e:
        log.warning("set_my_commands failed: %s", e)
    asyncio.create_task(monitor_loop(app))


def main():
    start_health_server()
    db_init()

    # Ensure an active asyncio event loop exists on this thread (required for Python 3.12+ / 3.14)
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

    # /new_watch conversation handler with reentry and global fallbacks so
    # user never gets trapped in an incomplete conversation.
    new_watch = ConversationHandler(
        entry_points=[
            CommandHandler(["new_watch", "newwatch"], cmd_new_watch),
            MessageHandler(filters.Regex(r"^/(new_watch|newwatch)$"), cmd_new_watch),
        ],
        states={
            ASK_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_address)],
            ASK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_name)],
        },
        fallbacks=[
            CommandHandler("cancel", cmd_cancel),
            CommandHandler(["start", "help"], cmd_start),
            CommandHandler(["my_watches", "mywatches"], cmd_my_watches),
            CommandHandler(["remove_watch", "removewatch"], cmd_remove_watch),
        ],
        allow_reentry=True,
    )

    application.add_handler(CommandHandler(["start", "help"], cmd_start))
    application.add_handler(MessageHandler(filters.Regex(r"^/(start|help)$"), cmd_start))
    application.add_handler(new_watch)
    application.add_handler(CommandHandler(["my_watches", "mywatches"], cmd_my_watches))
    application.add_handler(MessageHandler(filters.Regex(r"^/(my_watches|mywatches)$"), cmd_my_watches))
    application.add_handler(CommandHandler(["remove_watch", "removewatch"], cmd_remove_watch))
    application.add_handler(MessageHandler(filters.Regex(r"^/(remove_watch|removewatch)$"), cmd_remove_watch))
    # All inline-button taps from /remove_watch land here.
    application.add_handler(CallbackQueryHandler(on_remove_callback, pattern=r"^rm:"))
    application.add_handler(CommandHandler("cancel", cmd_cancel))
    application.add_handler(MessageHandler(filters.Regex(r"^/cancel$"), cmd_cancel))
    application.add_error_handler(on_error)

    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()