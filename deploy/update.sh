#!/bin/bash
# ─────────────────────────────────────────────────────────────────────
# GraphenMail — Pull latest code and restart
# Run as root: bash /opt/graphenmail/deploy/update.sh
# ─────────────────────────────────────────────────────────────────────
set -e

APP_DIR="/opt/graphenmail"
APP_USER="graphenmail"

echo "Pulling latest code..."
cd "$APP_DIR"
sudo -u "$APP_USER" git pull

echo "Updating dependencies..."
sudo -u "$APP_USER" ./venv/bin/pip install -r requirements.txt --quiet

echo "Restarting service..."
systemctl restart graphenmail

echo "Done! Status:"
systemctl status graphenmail --no-pager
