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
from solders.pubkey import Pubkey

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    Application, CommandHandler, ConversationHandler,
    MessageHandler, filters, ContextTypes, CallbackQueryHandler
)

from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import threading
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter

load_dotenv()

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
HELIUS_KEY = os.environ["HELIUS_KEY"]
SOLANA_TRACKER_API = os.environ.get("SOLANA_TRACKER_API", "")
RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}"

TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
LAMPORTS_PER_SOL = 1_000_000_000

MAX_WALLETS = 10
POLL_INTERVAL = 15

DATABASE_URL = os.getenv("DATABASE_URL")

def resolve_db_path():
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

def get_db():
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
    kp = Keypair()
    return str(kp.pubkey()), str(kp)

def parse_and_validate_private_key(key_str: str):
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
    conn, is_pg = get_db()
    cur = conn.cursor()
    q = "SELECT name, address, paused FROM wallets WHERE user_id=%s ORDER BY id" if is_pg else "SELECT name, address, paused FROM wallets WHERE user_id=? ORDER BY id"
    cur.execute(q, (user_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def db_set_paused(user_id, name, paused: bool):
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
    conn, is_pg = get_db()
    cur = conn.cursor()
    cur.execute("SELECT user_id, name, address, paused FROM wallets")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

_rpc_session = requests.Session()
_rpc_adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50)
_rpc_session.mount("https://", _rpc_adapter)
_rpc_session.mount("http://", _rpc_adapter)

def rpc(method, params):
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        r = _rpc_session.post(RPC_URL, json=body, timeout=10)
        result = r.json().get("result")
        state["last_rpc_ok"] = True
        return result
    except Exception as e:
        state["last_rpc_ok"] = False
        log.warning("rpc %s failed: %s", method, e)
        return None

def get_sol_balance(address):
    res = rpc("getBalance", [address])
    if res is None:
        return None
    try:
        return res.get("value", 0) / LAMPORTS_PER_SOL
    except Exception:
        return None

def parse_mint_account(data):
    mint_flag = struct.unpack_from("<I", data, 0)[0]
    freeze_flag = struct.unpack_from("<I", data, 46)[0]
    return (mint_flag == 1, freeze_flag == 1)

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

def fetch_dex_paid(mint):
    try:
        url = f"https://api.dexscreener.com/orders/v1/solana/{mint}"
        r = _rpc_session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                orders = data.get("orders") or []
                boosts = data.get("boosts") or []
            elif isinstance(data, list):
                orders = data
                boosts = []
            else:
                orders, boosts = [], []
            if boosts:
                return True
            for o in orders:
                if isinstance(o, dict):
                    status = o.get("status")
                    if status in ("approved", "processing"):
                        return True
                    if o.get("paymentTimestamp") and status not in ("cancelled", "rejected", "failed"):
                        return True
            return False
        return False
    except Exception:
        return False

def fetch_solana_tracker_risk(mint):
    if not SOLANA_TRACKER_API:
        return None
    try:
        url = f"https://data.solanatracker.io/tokens/{mint}"
        headers = {"x-api-key": SOLANA_TRACKER_API, "User-Agent": "Mozilla/5.0"}
        r = _rpc_session.get(url, headers=headers, timeout=8)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                return data.get("risk")
        return None
    except Exception as e:
        log.warning("solana tracker fetch failed for %s: %s", mint, e)
        return None

def fetch_pump_graduation(mint):
    try:
        mint_pubkey = Pubkey.from_string(mint)
        pump_program = Pubkey.from_string(PUMP_PROGRAM)
        bc_pda, _ = Pubkey.find_program_address(
            [b"bonding-curve", bytes(mint_pubkey)],
            pump_program
        )
        acct = rpc("getAccountInfo", [str(bc_pda), {"encoding": "base64"}])
        if not acct or not acct.get("value"):
            return None
        val = acct["value"]
        if val.get("owner") != PUMP_PROGRAM:
            return None
        data = base64.b64decode(val["data"][0])
        if len(data) < 49:
            return None
        complete = data[48] != 0
        return {"complete": complete, "is_pump": True}
    except Exception as e:
        log.warning("pump graduation check failed for %s: %s", mint, e)
        return None

def fetch_holder_count(mint):
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": "holder-check",
            "method": "getTokenAccounts",
            "params": {
                "mint": mint,
                "limit": 1000,
                "page": 1
            }
        }
        r = _rpc_session.post(RPC_URL, json=payload, timeout=10)
        data = r.json()
        result = data.get("result")
        if not result:
            return None
        accounts = result.get("token_accounts") or []
        total = result.get("total") or 0
        owners = set()
        for acc in accounts:
            owner = acc.get("owner")
            if owner:
                owners.add(owner)
        if len(accounts) >= 1000:
            return f"{len(owners)}+"
        return str(len(owners))
    except Exception as e:
        log.warning("holder count fetch failed for %s: %s", mint, e)
        return None

def fetch_dex_metrics(mint):
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        r = _rpc_session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        if r.status_code != 200:
            return None
        data = r.json()
        pairs = data.get("pairs") or []
        if not pairs:
            return None
        
        best = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0))
        
        liquidity = float((best.get("liquidity") or {}).get("usd") or 0)
        volume = float((best.get("volume") or {}).get("h24") or 0)
        market_cap = float(best.get("marketCap") or best.get("fdv") or 0)
        txns = (best.get("txns") or {}).get("h24") or {}
        buys = int(txns.get("buys") or 0)
        sells = int(txns.get("sells") or 0)
        total_txns = buys + sells
        
        created_at = best.get("pairCreatedAt")
        age_hours = None
        if created_at:
            age_hours = (time.time() * 1000 - float(created_at)) / 3_600_000
        
        info = best.get("info") or {}
        socials = info.get("socials") or []
        websites = info.get("websites") or []
        
        twitter_url = next((s.get("url") for s in socials if s.get("type") in ("twitter", "x")), None)
        telegram_url = next((s.get("url") for s in socials if s.get("type") == "telegram"), None)
        website_url = websites[0].get("url") if websites else None
        
        price_usd = float(best.get("priceUsd") or 0)
        
        return {
            "liquidity": liquidity,
            "volume": volume,
            "market_cap": market_cap,
            "txns": total_txns,
            "age_hours": age_hours,
            "twitter_url": twitter_url,
            "telegram_url": telegram_url,
            "website_url": website_url,
            "price_usd": price_usd,
        }
    except Exception as e:
        log.warning("dex metrics fetch failed for %s: %s", mint, e)
        return None

def fetch_token_metadata(mint):
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": "asset-check",
            "method": "getAsset",
            "params": {"id": mint}
        }
        r = _rpc_session.post(RPC_URL, json=payload, timeout=10)
        data = r.json()
        result = data.get("result")
        if result:
            content = result.get("content") or {}
            metadata = content.get("metadata") or {}
            name = metadata.get("name") or "Unknown"
            symbol = metadata.get("symbol") or ""
            return {"name": name, "symbol": symbol}
        return None
    except Exception as e:
        log.warning("token metadata fetch failed for %s: %s", mint, e)
        return None

def get_wallet_token_balance(wallet_address, mint):
    try:
        res = rpc("getTokenAccountsByOwner", [
            wallet_address,
            {"mint": mint},
            {"encoding": "jsonParsed"}
        ])
        if not res:
            return 0.0
        accounts = res.get("value") or []
        total = 0.0
        for acc in accounts:
            try:
                info = acc["account"]["data"]["parsed"]["info"]
                amt = float(info["tokenAmount"]["uiAmountString"] or 0)
                total += amt
            except (KeyError, TypeError, ValueError):
                continue
        return total
    except Exception as e:
        log.warning("wallet token balance failed for %s/%s: %s", wallet_address, mint, e)
        return 0.0

def check_rug(mint):
    score = 100
    risks = []

    acct = rpc("getAccountInfo", [mint, {"encoding": "base64"}])
    if not acct or not acct.get("value"):
        return ("UNKNOWN", 0, ["mint not found"], {"dex_paid": False, "st_risk": None, "pump": None, "holders": None, "dex_metrics": None})

    value = acct["value"]
    owner = value.get("owner", "")
    try:
        data = base64.b64decode(value["data"][0])
    except Exception:
        return ("UNKNOWN", 0, ["data unavailable"], {"dex_paid": False, "st_risk": None, "pump": None, "holders": None, "dex_metrics": None})

    if len(data) < 82:
        return ("UNKNOWN", 0, ["data unavailable"], {"dex_paid": False, "st_risk": None, "pump": None, "holders": None, "dex_metrics": None})

    try:
        has_mint_auth, has_freeze_auth = parse_mint_account(data)
    except Exception:
        return ("UNKNOWN", 0, ["data unavailable"], {"dex_paid": False, "st_risk": None, "pump": None, "holders": None, "dex_metrics": None})

    if has_mint_auth:
        score -= 30
        risks.append("Mint authority active")
    if has_freeze_auth:
        score -= 30
        risks.append("Freeze authority active — sell can be blocked")

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
                newer_bps = struct.unpack_from("<H", ext_data, 66)[0]
                if newer_bps > 500:
                    score -= 20
                    risks.append("Transfer fee >5%")
                if newer_bps > 0:
                    risks.append(f"Transfer fee {newer_bps / 100:.2f}%")

    def _fetch_supply():
        return rpc("getTokenSupply", [mint])

    def _fetch_largest():
        return rpc("getTokenLargestAccounts", [mint])

    def _fetch_asset():
        return rpc("getAsset", [{"id": mint}])

    def _fetch_dex_paid():
        return fetch_dex_paid(mint)

    def _fetch_st_risk():
        return fetch_solana_tracker_risk(mint)

    def _fetch_pump():
        return fetch_pump_graduation(mint)

    def _fetch_holders():
        return fetch_holder_count(mint)

    def _fetch_dex_metrics():
        return fetch_dex_metrics(mint)

    with ThreadPoolExecutor(max_workers=8) as _pool:
        f_supply  = _pool.submit(_fetch_supply)
        f_largest = _pool.submit(_fetch_largest)
        f_asset   = _pool.submit(_fetch_asset)
        f_dex     = _pool.submit(_fetch_dex_paid)
        f_st      = _pool.submit(_fetch_st_risk)
        f_pump    = _pool.submit(_fetch_pump)
        f_holders = _pool.submit(_fetch_holders)
        f_dex_met = _pool.submit(_fetch_dex_metrics)
        
        supply_res  = f_supply.result()
        largest_res = f_largest.result()
        asset_res   = f_asset.result()
        dex_paid    = f_dex.result()
        st_risk     = f_st.result()
        pump_info   = f_pump.result()
        holders_count = f_holders.result()
        dex_metrics = f_dex_met.result()

    try:
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

    try:
        if asset_res and isinstance(asset_res, dict) and asset_res.get("mutable") is True:
            score -= 10
            risks.append("Metadata mutable")
    except Exception as e:
        log.warning("getAsset failed for %s: %s", mint, e)

    if isinstance(st_risk, dict):
        if st_risk.get("rugged"):
            score -= 40
            risks.append("Flagged as rugged (ST)")

        def _get_pct(parent_key, field="totalPercentage"):
            val = (st_risk.get(parent_key) or {}).get(field) if isinstance(st_risk.get(parent_key), dict) else 0
            try:
                return float(val or 0)
            except (ValueError, TypeError):
                return 0.0

        snipers = _get_pct("snipers")
        insiders = _get_pct("insiders")
        bundlers = _get_pct("bundlers")
        dev = _get_pct("dev", "percentage")

        if snipers > 10:
            score -= 15
            risks.append(f"Snipers hold {snipers:.1f}% (ST)")
        if insiders > 10:
            score -= 15
            risks.append(f"Insiders hold {insiders:.1f}% (ST)")
        if bundlers > 20:
            score -= 20
            risks.append(f"Bundlers hold {bundlers:.1f}% (ST)")
        if dev > 15:
            score -= 15
            risks.append(f"Dev holds {dev:.1f}% (ST)")

    if pump_info and not pump_info.get("complete"):
        score -= 15
        risks.append("Still on Pump.fun bonding curve")

    if dex_metrics:
        if dex_metrics["liquidity"] < 10_000:
            score -= 15
            risks.append(f"Low liquidity (${dex_metrics['liquidity']:,.0f})")
        if dex_metrics["volume"] == 0:
            score -= 10
            risks.append("No 24h volume")
        if dex_metrics["age_hours"] is not None and dex_metrics["age_hours"] < 1:
            score -= 10
            risks.append("Pair < 1 hour old")

    score = max(0, min(100, score))
    if score >= 70:
        verdict = "SAFE"
    elif score >= 40:
        verdict = "WARN"
    else:
        verdict = "DANGER"

    extra = {
        "dex_paid": dex_paid,
        "st_risk": st_risk,
        "pump": pump_info,
        "holders": holders_count,
        "dex_metrics": dex_metrics,
        "score": score,
        "verdict": verdict,
        "risks": risks[:8],
    }

    return (verdict, score, risks[:8], extra)

BASE58_ALPHABET = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
ASK_ADDRESS, ASK_NAME, ASK_IMPORT_KEY = range(3)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    text = (
        "👋 <b>Solana Wallet Watcher & Trader</b>\n\n"
        "I monitor Solana wallets in real time and alert you when they buy "
        "or sell tokens, complete with live balance and an on-chain safety score.\n\n"
        "<b>Commands:</b>\n"
        "• /my_wallet - manage your wallet (generate or import)\n"
        "• /create_wallet - generate a new wallet\n"
        "• /import_wallet - import an existing wallet private key\n"
        "• /new_watch - add a wallet to watch (max 10)\n"
        "• /my_watches - list your tracked wallets\n"
        "• /pause_watch - pause or resume watching a wallet\n"
        "• /remove_watch - remove a watched wallet\n"
        "• /cancel - abort the current operation"
    )
    await update.effective_message.reply_text(text, parse_mode="HTML")
    return ConversationHandler.END

async def cmd_my_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        sol_balances[(user_id, addr)] = await asyncio.to_thread(get_sol_balance, addr)
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
    await cmd_my_watches(update, context)

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

    await query.answer()

    parts = data.split(":", 2)

    if len(parts) == 3 and parts[0] == "rm" and parts[1] == "pick":
        name = parts[2]

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

    if len(parts) == 3 and parts[0] == "rm" and parts[1] == "yes":
        name = parts[2]

        existing = {n: a for n, a, _ in db_list(user_id)}
        addr = existing.get(name)
        removed = db_remove(user_id, name)
        if removed:
            if addr:
                baselines.pop((user_id, addr), None)
                sol_balances.pop((user_id, addr), None)
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

    if data == "rm:no":
        await query.edit_message_text("Removal cancelled.")
        return

    await query.edit_message_text("Something went wrong. Try /remove_watch again.")

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error("Telegram handler error: %s", context.error)

baselines = {}
sol_balances = {}

def fetch_wallet_mints(address):
    mints = set()

    for program_id in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
        res = rpc("getTokenAccountsByOwner", [
            address,
            {"programId": program_id},
            {"encoding": "jsonParsed"},
        ])
        if res is None:
            return None
        for item in res.get("value", []):
            try:
                info = item["account"]["data"]["parsed"]["info"]
                amt = int(info["tokenAmount"]["amount"])
                if amt > 0:
                    mints.add(info["mint"])
            except (KeyError, TypeError, ValueError):
                continue

    return mints

async def send_buy_alert(app, user_id, name, mint, address, prev_sol):
    sol_task = asyncio.to_thread(get_sol_balance, address)
    meta_task = asyncio.to_thread(fetch_token_metadata, mint)
    dex_task = asyncio.to_thread(fetch_dex_metrics, mint)

    current_sol, meta, dex = await asyncio.gather(sol_task, meta_task, dex_task)

    sol_str = f"{current_sol:.4f} SOL" if current_sol is not None else "unavailable"
    sol_spent = max(0.0, (prev_sol or 0) - (current_sol or 0)) if current_sol is not None and prev_sol is not None else 0.0
    sol_spent_str = f"{sol_spent:.4f} SOL" if sol_spent > 0 else "0.0000 SOL"

    token_name = f"{meta['name']} ({meta['symbol']})" if meta else mint[:8]
    price_usd = (dex or {}).get("price_usd", 0) or 0

    current_ui = await asyncio.to_thread(get_wallet_token_balance, address, mint)
    usd_est = current_ui * price_usd if price_usd > 0 else 0
    usd_str = f"${usd_est:,.2f}" if usd_est > 0 else "$0.00"

    try:
        await app.bot.send_message(
            chat_id=user_id,
            text=(
                f"🚨 <b>{html.escape(name)}</b> just bought a token!\n"
                f"CA: <code>{mint}</code>\n"
                f"Bought: <b>{html.escape(token_name)}</b> - <b>{usd_str}</b> - <b>{sol_spent_str}</b>\n"
                f"Wallet balance: <b>{sol_str}</b>\n"
                f"<i>Scanning chain for risks...</i>"
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        log.warning("buy alert send failed: %s", e)

async def send_sell_alert(app, user_id, name, mint, address):
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
    try:
        verdict, score, risks, extra = await asyncio.to_thread(check_rug, mint)
    except Exception as e:
        log.error("check_rug crashed for %s: %s", mint, e)
        verdict, score, risks, extra = ("UNKNOWN", 0, ["data unavailable"], {"dex_paid": False, "st_risk": None, "pump": None, "holders": None, "dex_metrics": None})

    emoji = {"SAFE": "✅", "WARN": "⚠️", "DANGER": "🔴", "UNKNOWN": "❓"}.get(verdict, "❓")
    risk_text = (
        "\n".join(f"• {html.escape(r)}" for r in risks)
        if risks
        else "No structural red flags found."
    )
    
    dex_paid_str = "✅ Paid" if (isinstance(extra, dict) and extra.get("dex_paid")) else "❌ Not Paid"
    info_lines = [f"Dex Paid: {dex_paid_str}"]

    holders = extra.get("holders") if isinstance(extra, dict) else None
    if holders:
        info_lines.append(f"Holders: <b>{holders}</b>")

    pump_info = extra.get("pump") if isinstance(extra, dict) else None
    if isinstance(pump_info, dict) and pump_info.get("is_pump"):
        pump_str = "🎓 Graduated" if pump_info.get("complete") else "🟡 Bonding Curve"
        info_lines.append(f"Pump.fun: {pump_str}")

    info_header = "\n".join(info_lines)

    text = (
        f"{emoji} <b>{verdict}</b> — safety score {score}/100\n"
        f"CA: <code>{mint}</code>\n"
        f"{info_header}\n\n"
        f"{risk_text}"
    )

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("📊 Show Full Metrics", callback_data=f"showmet:{mint}"),
        InlineKeyboardButton("⚡ Trade on Jupiter", url=f"https://jup.ag/swap/SOL-{mint}"),
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

async def on_show_metrics_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data or ""
    if not data.startswith("showmet:"):
        return

    mint = data[8:]

    try:
        verdict, score, risks, extra = await asyncio.to_thread(check_rug, mint)
    except Exception as e:
        log.error("check_rug crashed for showmet %s: %s", mint, e)
        await query.edit_message_text("Failed to fetch full metrics. Try again later.")
        return

    emoji = {"SAFE": "✅", "WARN": "⚠️", "DANGER": "🔴", "UNKNOWN": "❓"}.get(verdict, "❓")
    
    dex = extra.get("dex_metrics") if isinstance(extra, dict) else None
    st_risk = extra.get("st_risk") if isinstance(extra, dict) else None
    pump_info = extra.get("pump") if isinstance(extra, dict) else None
    holders = extra.get("holders") if isinstance(extra, dict) else None

    lines = [
        f"{emoji} <b>{verdict}</b> — safety score {score}/100",
        f"CA: <code>{mint}</code>",
        "",
        f"<b>Market Metrics:</b>",
    ]

    if dex:
        lines.append(f"• Liquidity: <b>${dex['liquidity']:,.0f}</b>")
        lines.append(f"• Volume (24h): <b>${dex['volume']:,.0f}</b>")
        lines.append(f"• Market Cap: <b>${dex['market_cap']:,.0f}</b>")
        lines.append(f"• Txns (24h): <b>{dex['txns']}</b>")
        if dex['age_hours'] is not None:
            lines.append(f"• Age: <b>{dex['age_hours']:.1f}h</b>")
        
        socials = []
        if dex.get('twitter_url'):
            socials.append(f"<a href='{html.escape(dex['twitter_url'], quote=True)}'>Twitter</a>")
        if dex.get('telegram_url'):
            socials.append(f"<a href='{html.escape(dex['telegram_url'], quote=True)}'>Telegram</a>")
        if dex.get('website_url'):
            socials.append(f"<a href='{html.escape(dex['website_url'], quote=True)}'>Website</a>")
        lines.append(f"• Socials: <b>{' | '.join(socials) if socials else 'None'}</b>")
    else:
        lines.append("• <i>No DexScreener data available</i>")

    lines.append("")
    lines.append("<b>On-Chain:</b>")
    if holders:
        lines.append(f"• Holders: <b>{holders}</b>")
    
    if isinstance(st_risk, dict):
        def _get_pct(parent_key, field="totalPercentage"):
            val = (st_risk.get(parent_key) or {}).get(field) if isinstance(st_risk.get(parent_key), dict) else 0
            try:
                return float(val or 0)
            except (ValueError, TypeError):
                return 0.0
        
        lines.append(f"• Dev Holding: <b>{_get_pct('dev', 'percentage'):.1f}%</b>")
        lines.append(f"• Insiders: <b>{_get_pct('insiders'):.1f}%</b>")
        lines.append(f"• Bundlers: <b>{_get_pct('bundlers'):.1f}%</b>")
        lines.append(f"• Snipers: <b>{_get_pct('snipers'):.1f}%</b>")
    
    if isinstance(pump_info, dict) and pump_info.get("is_pump"):
        pump_str = "🎓 Graduated" if pump_info.get("complete") else "🟡 Bonding Curve"
        lines.append(f"• Pump.fun: <b>{pump_str}</b>")

    lines.append("")
    lines.append("<b>Risks:</b>")
    if risks:
        for r in risks:
            lines.append(f"• {html.escape(r)}")
    else:
        lines.append("• No structural red flags found.")

    full_text = "\n".join(lines)

    new_keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("⚡ Trade on Jupiter", url=f"https://jup.ag/swap/SOL-{mint}"),
    ]])

    try:
        await query.edit_message_text(
            text=full_text,
            parse_mode="HTML",
            reply_markup=new_keyboard,
            disable_web_page_preview=True,
        )
    except Exception as e:
        log.warning("edit full metrics failed: %s", e)
        try:
            chat_id = query.message.chat_id if query.message else query.from_user.id
            await context.bot.send_message(
                chat_id=chat_id,
                text=full_text,
                parse_mode="HTML",
                reply_markup=new_keyboard,
                disable_web_page_preview=True,
            )
            if query.message:
                await query.message.delete()
        except Exception as e2:
            log.error("fallback send full metrics failed: %s", e2)

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

            unique_addresses = set()
            for user_id, name, address, paused in wallets:
                still_present.add((user_id, address))
                if not paused:
                    unique_addresses.add(address)

            _sem = asyncio.Semaphore(5)

            async def _fetch_one(addr):
                async with _sem:
                    return addr, await asyncio.to_thread(fetch_wallet_mints, addr)

            fetch_results = await asyncio.gather(
                *[_fetch_one(addr) for addr in unique_addresses],
                return_exceptions=True,
            )

            addr_mints: dict = {}
            for res in fetch_results:
                if isinstance(res, Exception):
                    fail_count += 1
                    log.error("fetch gather exception: %s", res)
                    continue
                addr, mints = res
                if mints is None:
                    fail_count += 1
                else:
                    addr_mints[addr] = mints

            for user_id, name, address, paused in wallets:
                key = (user_id, address)

                if paused:
                    continue

                if address not in addr_mints:
                    continue

                mints = addr_mints[address]

                try:
                    if key not in baselines:
                        baselines[key] = mints
                        sol_balances[key] = await asyncio.to_thread(get_sol_balance, address)
                        log.info(
                            "baseline seeded for %s/%s (%d mints)",
                            user_id, name, len(mints),
                        )
                        continue

                    new_mints  = mints - baselines[key]
                    sold_mints = baselines[key] - mints

                    if new_mints or sold_mints:
                        baselines[key] = mints

                        for mint in new_mints:
                            new_count += 1
                            prev_sol = sol_balances.get(key)
                            log.info("buy: mint %s for %s (%s)", mint, name, user_id)
                            await send_buy_alert(app, user_id, name, mint, address, prev_sol)
                            await send_verdict(app, user_id, mint)

                        for mint in sold_mints:
                            sell_count += 1
                            log.info("sell: mint %s for %s (%s)", mint, name, user_id)
                            await send_sell_alert(app, user_id, name, mint, address)

                        new_sol = await asyncio.to_thread(get_sol_balance, address)
                        if new_sol is not None:
                            sol_balances[key] = new_sol

                except Exception as e:
                    fail_count += 1
                    log.error("wallet %s (%s) diff failed: %s", name, address, e)
                    continue

            for key in list(baselines.keys()):
                if key not in still_present:
                    del baselines[key]
                    sol_balances.pop(key, None)

        except Exception as e:
            log.error("monitor loop iteration failed: %s", e)

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

async def post_init(app):
    try:
        await app.bot.set_my_commands([
            BotCommand("start",          "Start"),
            BotCommand("my_wallet",      "Manage wallet (generate or import)"),
            BotCommand("create_wallet",  "Generate a new wallet"),
            BotCommand("import_wallet",  "Import an existing wallet"),
            BotCommand("new_watch",      "Watch a Solana wallet(up to 10)"),
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
    application.add_handler(CallbackQueryHandler(on_trading_wallet_callback, pattern=r"^twal:"))
    application.add_handler(CallbackQueryHandler(on_remove_callback, pattern=r"^rm:"))
    application.add_handler(CallbackQueryHandler(on_pause_callback, pattern=r"^pg:"))
    application.add_handler(CallbackQueryHandler(on_show_metrics_callback, pattern=r"^showmet:"))
    application.add_handler(CommandHandler("cancel", cmd_cancel))
    application.add_error_handler(on_error)

    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=False,
    )

if __name__ == "__main__":
    main()