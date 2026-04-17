"""
First-run onboarding wizard — three optional steps to get the user
configured before they land on the dashboard.
"""
from flask import Blueprint, flash, redirect, render_template_string, request, session, url_for

import config
from web.auth import get_app_password, set_app_password

bp = Blueprint("onboarding", __name__, url_prefix="/onboarding")


_BASE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Welcome — GraphenMail</title>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        :root { --orange:#F97316; --border:#E5E7EB; --text:#1F2937; --muted:#6B7280; --bg:#0F172A; --radius:6px; }
        * { box-sizing:border-box; margin:0; padding:0; }
        body { font-family:'Montserrat',sans-serif; background:var(--bg); color:var(--text); min-height:100vh; display:flex; align-items:center; justify-content:center; padding:24px; }
        .card { background:#fff; border-radius:var(--radius); padding:40px; width:100%; max-width:520px; box-shadow:0 20px 60px rgba(0,0,0,0.4); }
        .steps { display:flex; gap:6px; margin-bottom:24px; }
        .dot { flex:1; height:4px; background:var(--border); border-radius:2px; }
        .dot.active { background:var(--orange); }
        h1 { font-size:22px; font-weight:700; margin-bottom:6px; }
        .sub { font-size:14px; color:var(--muted); margin-bottom:24px; line-height:1.5; }
        label { display:block; font-size:13px; font-weight:600; margin-bottom:6px; }
        input, textarea { width:100%; padding:10px 12px; border:1px solid var(--border); border-radius:var(--radius); font-family:inherit; font-size:14px; }
        textarea { min-height:100px; font-family:'Courier New',monospace; font-size:12px; resize:vertical; }
        .hint { font-size:12px; color:var(--muted); margin-top:6px; display:block; }
        .actions { display:flex; gap:10px; justify-content:space-between; margin-top:24px; }
        .btn { padding:10px 18px; border-radius:var(--radius); font-family:inherit; font-size:14px; font-weight:600; cursor:pointer; text-decoration:none; display:inline-block; }
        .btn-primary { background:var(--orange); color:#fff; border:none; }
        .btn-primary:hover { background:#EA580C; }
        .btn-secondary { background:#fff; color:var(--muted); border:1px solid var(--border); }
        .error { background:#FEE2E2; color:#991B1B; padding:10px 12px; border-radius:var(--radius); font-size:13px; margin-bottom:16px; }
    </style>
</head>
<body>
<div class="card">
    <div class="steps">
        <div class="dot {% if step >= 1 %}active{% endif %}"></div>
        <div class="dot {% if step >= 2 %}active{% endif %}"></div>
        <div class="dot {% if step >= 3 %}active{% endif %}"></div>
    </div>
    {{ body | safe }}
</div>
</body>
</html>
"""


def _is_onboarded() -> bool:
    return bool(config.get_setting("onboarded"))


def needs_onboarding() -> bool:
    """Public check used from the app factory's before_request hook."""
    return not _is_onboarded()


@bp.route("/", methods=["GET", "POST"])
def step1():
    """Step 1 — set app password (required if none yet)."""
    if _is_onboarded():
        return redirect(url_for("dashboard.index"))

    error = None
    if request.method == "POST":
        pw = (request.form.get("password", "") or "").strip()
        if get_app_password():
            # password already set — skip
            return redirect(url_for("onboarding.step2"))
        if not pw or len(pw) < 6:
            error = "Password must be at least 6 characters."
        else:
            set_app_password(pw)
            session["authenticated"] = True
            return redirect(url_for("onboarding.step2"))

    body = """
        <h1>Welcome to GraphenMail 👋</h1>
        <p class="sub">Let's get you set up in under a minute. First, choose a password to protect this install.</p>
        {error}
        <form method="post">
            <input type="hidden" name="csrf_token" value="{csrf}">
            <label for="password">Admin Password</label>
            <input type="password" id="password" name="password" placeholder="Choose a strong password" autofocus>
            <span class="hint">Minimum 6 characters. You'll use this to log in.</span>
            <div class="actions">
                <span></span>
                <button type="submit" class="btn btn-primary">Next →</button>
            </div>
        </form>
    """.format(
        error=f'<div class="error">{error}</div>' if error else "",
        csrf=_csrf(),
    )
    return render_template_string(_BASE, body=body, step=1)


@bp.route("/step-2", methods=["GET", "POST"])
def step2():
    """Step 2 — optional OpenRouter key for AI URL generation."""
    if _is_onboarded():
        return redirect(url_for("dashboard.index"))

    if request.method == "POST":
        key = (request.form.get("openrouter_api_key", "") or "").strip()
        if key:
            config.save_settings({"openrouter_api_key": key})
        return redirect(url_for("onboarding.step3"))

    body = """
        <h1>AI URL Generation <span style="color:#9CA3AF;font-weight:500;font-size:14px;">— optional</span></h1>
        <p class="sub">OpenRouter provides a free tier that helps discover additional business URLs. You can skip this and add it later.</p>
        <form method="post">
            <input type="hidden" name="csrf_token" value="{csrf}">
            <label for="key">OpenRouter API Key</label>
            <input type="password" id="key" name="openrouter_api_key" placeholder="sk-or-v1-...">
            <span class="hint">Get a free key at <a href="https://openrouter.ai/keys" target="_blank" rel="noopener">openrouter.ai/keys</a>.</span>
            <div class="actions">
                <a href="{skip}" class="btn btn-secondary">Skip</a>
                <button type="submit" class="btn btn-primary">Next →</button>
            </div>
        </form>
    """.format(csrf=_csrf(), skip=url_for("onboarding.step3"))
    return render_template_string(_BASE, body=body, step=2)


@bp.route("/step-3", methods=["GET", "POST"])
def step3():
    """Step 3 — optional outbound IPs for rotation."""
    if _is_onboarded():
        return redirect(url_for("dashboard.index"))

    if request.method == "POST":
        raw = (request.form.get("outbound_ips", "") or "").strip()
        ips = [ip.strip() for ip in raw.splitlines() if ip.strip()]
        updates = {"onboarded": True}
        if ips:
            updates["outbound_ips"] = ips
        config.save_settings(updates)
        flash("Setup complete! Welcome to GraphenMail.", "success")
        return redirect(url_for("dashboard.index"))

    body = """
        <h1>Outbound IPs <span style="color:#9CA3AF;font-weight:500;font-size:14px;">— optional</span></h1>
        <p class="sub">If your VPS has extra IPs bound to it, paste them below for round-robin rotation during scraping. Leave empty to use the server's default IP.</p>
        <form method="post">
            <input type="hidden" name="csrf_token" value="{csrf}">
            <label for="ips">Outbound IPs</label>
            <textarea id="ips" name="outbound_ips" placeholder="192.0.2.10&#10;192.0.2.11&#10;2001:db8::1"></textarea>
            <span class="hint">One IP per line. Supports IPv4 and IPv6.</span>
            <div class="actions">
                <button type="submit" name="skip" value="1" class="btn btn-secondary">Skip & Finish</button>
                <button type="submit" class="btn btn-primary">Finish →</button>
            </div>
        </form>
    """.format(csrf=_csrf())
    return render_template_string(_BASE, body=body, step=3)


def _csrf() -> str:
    """Lazy import — csrf is registered on the app factory."""
    try:
        from flask_wtf.csrf import generate_csrf
        return generate_csrf()
    except Exception:
        return ""
