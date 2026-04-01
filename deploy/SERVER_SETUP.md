# VPS setup (step-by-step)

Use a small Ubuntu 22.04+ VPS. **Clone the repo to a path with no spaces**, e.g. `~/kalshi-trading`. The systemd units below assume that path.

## What you must provide

- SSH access to the VPS (user account, not necessarily root).
- A **private** Git remote (GitHub/GitLab) for this project, or use `rsync`/`scp` instead of `git pull`.
- On the server: copy **`.env`** and **`kalshi.pem`** yourself (never commit them). Same variables as on your Mac.

Everything else can follow these steps verbatim after adjusting `kalshi-trading` if you use a different directory name.

---

## 1. Create user and basic packages

```bash
sudo apt update
sudo apt install -y git python3-venv python3-pip
```

---

## 2. Clone the repo

```bash
cd ~
git clone https://github.com/YOU/YOUR_PRIVATE_REPO.git kalshi-trading
cd kalshi-trading
```

---

## 3. Python venv and dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
pip install -r weather-bot/requirements.txt
pip install -r control-panel/requirements.txt
deactivate
```

---

## 4. Secrets on the server

```bash
cd ~/kalshi-trading
nano .env          # paste KALSHI_API_KEY_ID, KALSHI_HOST, keys paths, etc.
nano kalshi.pem    # paste private key; chmod 600 kalshi.pem
```

---

## 5. systemd user services

```bash
mkdir -p ~/.config/systemd/user
cp deploy/systemd/user/*.service ~/.config/systemd/user/
# Edit the three files if your path is not ~/kalshi-trading — replace every %h/kalshi-trading
systemctl --user daemon-reload
```

Enable **linger** so user services keep running after you disconnect SSH:

```bash
loginctl enable-linger "$USER"
```

Start services:

```bash
systemctl --user enable --now kalshi-btc15m.service
systemctl --user enable --now kalshi-weather-bot.service
systemctl --user enable --now kalshi-control-panel.service
```

Check status:

```bash
systemctl --user status kalshi-btc15m.service
journalctl --user -u kalshi-btc15m.service -f
```

---

## 6. Deploy updates from your laptop

On the server, `deploy/deploy.sh` pulls and restarts:

```bash
chmod +x deploy/deploy.sh   # once
./deploy/deploy.sh
```

From your Mac, typical flow:

```bash
git add -A && git commit -m "..." && git push
ssh you@vps 'cd ~/kalshi-trading && ./deploy/deploy.sh'
```

---

## 7. Open the dashboard (localhost on your Mac)

The control panel listens on **127.0.0.1:8080** on the VPS only.

```bash
ssh -L 8080:127.0.0.1:8080 you@YOUR_VPS_IP
```

Then open in your browser: **http://127.0.0.1:8080**

You get start/stop/restart, journal tail for the BTC bot, and SQLite tables for the weather bot.

---

## 8. Optional: Tailscale

If you prefer not to use SSH port forwarding, install Tailscale on VPS + laptop and keep the app on `127.0.0.1`, or bind to the Tailscale IP with firewall rules. Do not expose port 8080 on the public internet without TLS and auth.

---

## Troubleshooting

| Issue | What to check |
|--------|----------------|
| `systemctl --user` fails over SSH | Run `loginctl enable-linger $USER` |
| Bots can't find modules | `WorkingDirectory` in `.service` files (weather-bot vs repo root) |
| Control panel 502 / empty DB | Weather DB is created on first run; path `weather-bot/db/trades.db` |
| Permission denied on `.env` / pem | `chmod 600` on secrets; own files as the same user running systemd |
