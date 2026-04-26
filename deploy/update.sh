#!/bin/bash
# ─────────────────────────────────────────────────────────────────────
# GraphenMail — Pull latest code and restart
# Run as root: bash /opt/graphenmail/deploy/update.sh
# ─────────────────────────────────────────────────────────────────────
set -e

APP_DIR="/opt/graphenmail"
APP_USER="graphenmail"

echo "Ensuring runtime directories..."
mkdir -p /var/lib/graphenmail /etc/graphenmail
chown -R "$APP_USER:$APP_USER" /var/lib/graphenmail

echo "Pulling latest code..."
cd "$APP_DIR"
sudo -u "$APP_USER" git pull

echo "Updating dependencies..."
sudo -u "$APP_USER" ./venv/bin/pip install -r requirements.txt --quiet

echo "Restarting service..."
cp deploy/graphenmail.service /etc/systemd/system/graphenmail.service
systemctl daemon-reload
systemctl restart graphenmail

echo "Done! Status:"
systemctl status graphenmail --no-pager
