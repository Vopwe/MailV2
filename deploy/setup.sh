#!/bin/bash
# ─────────────────────────────────────────────────────────────────────
# GraphenMail — One-command server setup for Ubuntu 22.04 / 24.04
# Run as root: bash setup.sh
# ─────────────────────────────────────────────────────────────────────
set -e

APP_USER="graphenmail"
APP_DIR="/opt/graphenmail"
REPO_URL="https://github.com/Vopwe/MailV2.git"
DOMAIN=""

echo "══════════════════════════════════════════════════"
echo "  GraphenMail — Server Setup"
echo "══════════════════════════════════════════════════"

# ── 1. System updates + dependencies ─────────────────────────────────
echo "[1/7] Installing system packages..."
apt update && apt upgrade -y
apt install -y python3 python3-pip python3-venv git curl ufw

# ── 2. Create app user ───────────────────────────────────────────────
echo "[2/7] Creating app user..."
if ! id "$APP_USER" &>/dev/null; then
    useradd -r -m -s /bin/bash "$APP_USER"
fi

# ── 3. Clone or update repo ──────────────────────────────────────────
echo "[3/7] Setting up application..."
if [ -d "$APP_DIR" ]; then
    cd "$APP_DIR"
    sudo -u "$APP_USER" git pull
else
    if [ -z "$REPO_URL" ]; then
        echo "ERROR: Set REPO_URL in this script first!"
        echo "Example: REPO_URL=\"https://github.com/youruser/graphenmail.git\""
        exit 1
    fi
    git clone "$REPO_URL" "$APP_DIR"
    chown -R "$APP_USER":"$APP_USER" "$APP_DIR"
fi

# ── 4. Python venv + dependencies ────────────────────────────────────
echo "[4/7] Installing Python dependencies..."
cd "$APP_DIR"
sudo -u "$APP_USER" python3 -m venv venv
sudo -u "$APP_USER" ./venv/bin/pip install --upgrade pip
sudo -u "$APP_USER" ./venv/bin/pip install -r requirements.txt
sudo -u "$APP_USER" ./venv/bin/pip install gunicorn

# ── 4b. Config + license directories ─────────────────────────────────
echo "[4b/7] Creating config + data directories..."
mkdir -p /etc/graphenmail /var/lib/graphenmail
chown -R "$APP_USER":"$APP_USER" /var/lib/graphenmail
chown root:"$APP_USER" /etc/graphenmail
chmod 750 /etc/graphenmail

if [ ! -f /etc/graphenmail/env ]; then
    SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_urlsafe(48))')
    cat > /etc/graphenmail/env <<EOF
# GraphenMail runtime environment — root-owned, readable by $APP_USER
FLASK_DEBUG=0
FLASK_SECRET_KEY=$SECRET_KEY
LICENSE_PATH=/etc/graphenmail/license.key
EMAIL_DB_PATH=/var/lib/graphenmail/emails.db
CRAWL_TLS_VERIFY=true
# GM_OPENROUTER_API_KEY=
# GM_APP_PASSWORD_HASH=
EOF
    chmod 640 /etc/graphenmail/env
    chown root:"$APP_USER" /etc/graphenmail/env
    echo "  → /etc/graphenmail/env created (secret key auto-generated)"
fi

if [ ! -f /etc/graphenmail/license.key ]; then
    touch /etc/graphenmail/license.key
    chmod 640 /etc/graphenmail/license.key
    chown root:"$APP_USER" /etc/graphenmail/license.key
    echo "  → /etc/graphenmail/license.key placeholder created — paste key via web UI after first boot."
fi

# ── 5. Systemd service ───────────────────────────────────────────────
echo "[5/7] Setting up systemd service..."
cp deploy/graphenmail.service /etc/systemd/system/graphenmail.service
systemctl daemon-reload
systemctl enable graphenmail
systemctl restart graphenmail

# ── 6. Caddy (reverse proxy + auto-SSL) ──────────────────────────────
echo "[6/7] Installing Caddy..."
if ! command -v caddy &>/dev/null; then
    apt install -y debian-keyring debian-archive-keyring apt-transport-https
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | tee /etc/apt/sources.list.d/caddy-stable.list
    apt update && apt install -y caddy
fi

if [ -n "$DOMAIN" ]; then
    cp deploy/Caddyfile /etc/caddy/Caddyfile
    sed -i "s/YOUR_DOMAIN/$DOMAIN/g" /etc/caddy/Caddyfile
else
    # IP-only mode — no SSL
    cat > /etc/caddy/Caddyfile <<'EOF'
:80 {
    reverse_proxy localhost:5000
}
EOF
fi
systemctl restart caddy

# ── 7. Firewall ──────────────────────────────────────────────────────
echo "[7/7] Configuring firewall..."
ufw allow 22/tcp    # SSH
ufw allow 80/tcp    # HTTP
ufw allow 443/tcp   # HTTPS
ufw --force enable

echo ""
echo "══════════════════════════════════════════════════"
echo "  GraphenMail is live!"
if [ -n "$DOMAIN" ]; then
    echo "  Open: https://$DOMAIN"
else
    echo "  Open: http://$(curl -s ifconfig.me)"
fi
echo ""
echo "  Manage:"
echo "    systemctl status graphenmail"
echo "    systemctl restart graphenmail"
echo "    journalctl -u graphenmail -f"
echo "══════════════════════════════════════════════════"
