# Solana Wallet Watcher Telegram Bot

A Telegram bot that monitors Solana wallets and alerts users whenever new token mints (spl-tokens or Token-2022) appear in watched wallets, accompanied by RugCheck token verdicts and analysis.

---

## Features

- **Multi-Wallet Tracking**: Users can monitor multiple Solana wallets with custom labels.
- **Token Detection**: Monitors standard SPL Tokens and Token-2022 tokens via Helius RPC.
- **Safety Verdicts**: Fetches token security reports directly via RugCheck API.
- **Telegram Interface**: Easy commands `/new-watch`, `/my-watches`, `/remove-watch`, `/cancel`.
- **Render Ready**: Includes a built-in health-check HTTP server to run on Render's Web Service (free tier) or as a Background Worker.

---

## Local Setup

### 1. Prerequisites
- Python 3.10+
- Telegram Bot Token (from [@BotFather](https://t.me/BotFather))
- Helius API Key (from [helius.dev](https://helius.dev))

### 2. Installation
```bash
git clone https://github.com/webgawd-apex/boto1.git
cd boto1
pip install -r requirements.txt
```

### 3. Environment Variables
Create a `.env` file in the root directory (refer to `.env.example`):
```env
TELEGRAM_TOKEN=your_telegram_bot_token_here
HELIUS_KEY=your_helius_api_key_here
```

### 4. Run the Bot
```bash
python bot.py
```

---

## Deploying to Render

You can deploy this bot on [Render](https://render.com) using either a **Web Service** (supported on the Free tier) or a **Background Worker**.

### Option A: Deploy as a Web Service (Free Tier)
1. Sign in to [Render Dashboard](https://dashboard.render.com).
2. Click **New +** > **Web Service**.
3. Connect your GitHub repository (`webgawd-apex/boto1`).
4. Configure the settings:
   - **Name**: `solana-wallet-bot` (or any name you prefer)
   - **Environment**: `Python 3`
   - **Region**: Choose the closest region
   - **Branch**: `main`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python bot.py`
   - **Plan Type**: `Free`
5. Under **Environment Variables**, add:
   - `TELEGRAM_TOKEN`: Your Telegram Bot Token
   - `HELIUS_KEY`: Your Helius API Key
6. Click **Create Web Service**. Render will automatically detect the port and pass health checks.

> **Note on Data Persistence**: Render's free tier uses an ephemeral filesystem. If the container restarts or re-deploys, the SQLite database (`wallets.db`) resets. For persistent storage on Render, attach a **Persistent Disk** (under Disks) mounted at `/data` and set `DB_FILE=/data/wallets.db`.

### Option B: Deploy as a Background Worker (Paid Tier)
1. Click **New +** > **Background Worker**.
2. Connect your GitHub repository.
3. Configure:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python bot.py`
4. Add your `TELEGRAM_TOKEN` and `HELIUS_KEY` under **Environment Variables**.
5. Click **Create Background Worker**.
