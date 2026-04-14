"""
Simple password protection for GraphenMail.
Single-user auth using a password stored in settings.json.
"""
import hashlib
import secrets
from functools import wraps
from flask import request, redirect, url_for, session, flash, render_template_string
import config

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


def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def get_app_password() -> str | None:
    """Get the configured password. Returns None if no password is set."""
    pw = config.get_setting("app_password", "")
    return pw if pw else None


def set_app_password(password: str):
    """Set the app password (stored as hash)."""
    config.save_settings({"app_password_hash": _hash_password(password), "app_password": password})


def check_password(password: str) -> bool:
    """Check password against stored hash or plaintext."""
    stored_hash = config.get_setting("app_password_hash", "")
    stored_plain = config.get_setting("app_password", "")

    if stored_hash:
        return _hash_password(password) == stored_hash
    if stored_plain:
        return password == stored_plain
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
        if request.endpoint and request.endpoint.startswith("auth."):
            return
        if request.endpoint == "static":
            return
        if not session.get("authenticated"):
            return redirect(url_for("auth.login"))
