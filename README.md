# Web3 Intelligence Bot (v1)

High-signal Web3 intelligence system: ingestion → processing → intelligence → storage → Telegram output.
Strict rolling 24h window, dedup, caching/backoff, logging, and graceful failures.

## Quickstart (local)

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# set TELEGRAM_BOT_TOKEN (required)
# optional: set TELEGRAM_CHAT_ID for scheduled sends
python scripts/local_test_run.py   # end-to-end dry/real analysis
python main.py                     # run Telegram bot (long polling)
```

## Commands

- /dailybrief /news /newprojects /trends /funding /github /rawsignals

## Env Vars

See `.env.example` (matches spec).

## Deployment (GCP Compute Engine VM - recommended)

This bot uses long polling for Telegram commands, so a VM is simplest and most reliable.

1. Create a small VM (e2-micro or e2-small), install python3.12 and git.
2. Copy repo to `/opt/web3-intelligence`
3. Create venv and install requirements
4. Create `.env` with TELEGRAM_BOT_TOKEN + optional keys
5. Install systemd unit:

```bash
sudo cp deploy/systemd/web3-intelligence.service /etc/systemd/system/web3-intelligence.service
sudo systemctl daemon-reload
sudo systemctl enable web3-intelligence
sudo systemctl start web3-intelligence
sudo systemctl status web3-intelligence
```

Logs:
- `journalctl -u web3-intelligence -f`
- `./logs/web3_intelligence.log`

