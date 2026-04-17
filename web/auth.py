"""
Simple password protection for GraphenMail.
Single-user auth using a password stored in settings.json.
"""
import logging
import secrets
from functools import wraps
from flask import request, redirect, url_for, session, flash, render_template_string
from werkzeug.security import generate_password_hash, check_password_hash
import config

logger = logging.getLogger(__name__)

LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login — GraphenMail</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        :root { --orange: #F97316; --orange-hover: #EA580C; --border: #E5E7EB; --text: #1F2937; --bg: #FAFAFA; --radius: 4px; }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Montserrat', sans-serif; background: var(--bg); color: var(--text); display: flex; align-items: center; justify-content: center; min-height: 100vh; }
        .login-card { background: #fff; border: 1px solid var(--border); border-radius: var(--radius); padding: 40px; width: 100%; max-width: 380px; text-align: center; }
        .login-card svg { width: 48px; height: 48px; margin-bottom: 16px; }
        .login-card h1 { font-size: 22px; font-weight: 700; margin-bottom: 4px; }
        .login-card p { font-size: 13px; color: #6B7280; margin-bottom: 24px; }
        .form-group { margin-bottom: 16px; text-align: left; }
        .form-group label { display: block; font-size: 13px; font-weight: 600; margin-bottom: 6px; }
        .form-group input { width: 100%; padding: 10px 12px; border: 1px solid var(--border); border-radius: var(--radius); font-family: 'Montserrat', sans-serif; font-size: 14px; }
        .form-group input:focus { outline: none; border-color: var(--orange); }
        .btn { width: 100%; padding: 10px; background: var(--orange); color: #fff; border: none; border-radius: var(--radius); font-family: 'Montserrat', sans-serif; font-size: 14px; font-weight: 600; cursor: pointer; }
        .btn:hover { background: var(--orange-hover); }
        .error { background: #FEE2E2; color: #991B1B; padding: 8px 12px; border-radius: var(--radius); font-size: 13px; margin-bottom: 16px; }
    </style>
</head>
<body>
    <div class="login-card">
        <svg viewBox="0 0 36 36" fill="none" xmlns="http://www.w3.org/2000/svg">
            <rect width="36" height="36" rx="8" fill="#F97316"/>
            <path d="M8 13L18 19.5L28 13" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M8 13V24C8 25.1 8.9 26 10 26H26C27.1 26 28 25.1 28 24V13" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M8 13L10 11H26L28 13" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
        <h1>GraphenMail</h1>
        <p>Enter your password to continue</p>
        {% if error %}<div class="error">{{ error }}</div>{% endif %}
        <form method="post">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            <div class="form-group">
                <label for="password">Password</label>
                <input type="password" id="password" name="password" autofocus required>
            </div>
            <button type="submit" class="btn">Sign In</button>
        </form>
    </div>
</body>
</html>
"""


def _migrate_legacy_password_if_needed():
    """
    Migrate legacy storage on first access:
      - If only plaintext `app_password` present: hash it with werkzeug, strip plaintext.
      - If only legacy SHA256 hex hash present: leave it (check_password falls through).
      - If plaintext AND hash both present: strip plaintext.
    """
    import hashlib as _hashlib
    plaintext = config.get_setting("app_password", "")
    stored = config.get_setting("app_password_hash", "")

    if plaintext and not stored:
        # Upgrade plaintext to werkzeug hash
        new_hash = generate_password_hash(plaintext)
        config.save_settings({"app_password_hash": new_hash, "app_password": ""})
        logger.info("Migrated legacy plaintext password to hashed storage.")
        return

    if plaintext and stored:
        # Just strip plaintext — keep whichever hash was stored.
        config.save_settings({"app_password": ""})
        logger.info("Stripped redundant plaintext password from settings.")
        return

    # Detect legacy SHA256 hash (64 hex chars, no $ prefix from werkzeug) and upgrade
    # only if we had the plaintext — otherwise it stays as-is and check_password handles it.


def has_app_password() -> bool:
    """True if a password is configured (checks both hash and legacy plaintext)."""
    return bool(config.get_setting("app_password_hash", "") or config.get_setting("app_password", ""))


# Keep `get_app_password` alias for backward compat across the codebase (route/settings).
def get_app_password() -> str | None:
    return "set" if has_app_password() else None


def set_app_password(password: str):
    """Set the app password. Stored as werkzeug hash only — never plaintext."""
    new_hash = generate_password_hash(password)
    # Explicitly clear any lingering plaintext key.
    config.save_settings({"app_password_hash": new_hash, "app_password": ""})


def check_password(password: str) -> bool:
    """Verify password against stored hash. Handles werkzeug, legacy SHA256, and (deprecated) plaintext."""
    import hashlib as _hashlib
    stored_hash = config.get_setting("app_password_hash", "")
    stored_plain = config.get_setting("app_password", "")  # legacy

    if stored_hash:
        # werkzeug hashes start with "pbkdf2:" or "scrypt:" etc; legacy SHA256 is 64 hex chars.
        if stored_hash.startswith(("pbkdf2:", "scrypt:", "argon2:")):
            return check_password_hash(stored_hash, password)
        # Legacy SHA256 fallback — migrate on successful check.
        if len(stored_hash) == 64 and all(c in "0123456789abcdef" for c in stored_hash):
            legacy = _hashlib.sha256(password.encode()).hexdigest()
            if legacy == stored_hash:
                # Upgrade to werkzeug hash for future checks.
                config.save_settings({"app_password_hash": generate_password_hash(password), "app_password": ""})
                return True
            return False
        return False
    if stored_plain:
        ok = password == stored_plain
        if ok:
            # Upgrade on successful legacy-plaintext login.
            config.save_settings({"app_password_hash": generate_password_hash(password), "app_password": ""})
        return ok
    return False


def login_required(f):
    """Decorator: require authentication if a password is configured."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not get_app_password():
            return f(*args, **kwargs)
        if session.get("authenticated"):
            return f(*args, **kwargs)
        return redirect(url_for("auth.login"))
    return decorated


def init_auth(app):
    """Register auth blueprint and protect all routes."""
    # One-shot migration for any lingering plaintext password from older installs.
    try:
        _migrate_legacy_password_if_needed()
    except Exception as e:
        logger.warning(f"Password migration skipped: {e}")

    from flask import Blueprint
    auth_bp = Blueprint("auth", __name__)

    @auth_bp.route("/login", methods=["GET", "POST"])
    def login():
        if not get_app_password():
            return redirect("/")
        error = None
        if request.method == "POST":
            password = request.form.get("password", "")
            if check_password(password):
                session["authenticated"] = True
                session.permanent = True
                return redirect("/")
            error = "Invalid password"
        return render_template_string(LOGIN_TEMPLATE, error=error)

    @auth_bp.route("/logout")
    def logout():
        session.pop("authenticated", None)
        return redirect(url_for("auth.login"))

    app.register_blueprint(auth_bp)

    @app.before_request
    def protect_routes():
        if not get_app_password():
            return
        endpoint = request.endpoint or ""
        # Allow auth, static, license gate, onboarding (user may be mid-setup)
        if (endpoint.startswith("auth.")
                or endpoint == "static"
                or endpoint.startswith("license_gate.")
                or endpoint.startswith("onboarding.")):
            return
        if not session.get("authenticated"):
            return redirect(url_for("auth.login"))
