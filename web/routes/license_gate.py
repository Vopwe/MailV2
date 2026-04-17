"""
License gate — page shown when the app boots with an invalid/missing license.
Also handles pasting a new license file.
"""
from flask import Blueprint, flash, redirect, render_template_string, request, url_for

from licensing import validator as lic

bp = Blueprint("license_gate", __name__, url_prefix="/license-gate")


GATE_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Activate GraphenMail</title>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        :root { --orange: #F97316; --orange-hover: #EA580C; --border: #E5E7EB; --text: #1F2937; --muted: #6B7280; --bg: #FAFAFA; --radius: 6px; }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Montserrat', sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 24px; }
        .card { background: #fff; border: 1px solid var(--border); border-radius: var(--radius); padding: 40px; width: 100%; max-width: 560px; }
        .logo { display: flex; align-items: center; gap: 10px; margin-bottom: 24px; }
        .logo svg { width: 36px; height: 36px; }
        .logo span { font-size: 18px; font-weight: 700; }
        h1 { font-size: 22px; font-weight: 700; margin-bottom: 6px; }
        .subtitle { font-size: 14px; color: var(--muted); margin-bottom: 24px; line-height: 1.5; }
        .fingerprint { background: #F3F4F6; border-radius: var(--radius); padding: 12px 14px; font-family: 'Courier New', monospace; font-size: 12px; word-break: break-all; margin-bottom: 18px; }
        .fingerprint-label { font-size: 12px; font-weight: 600; color: var(--muted); margin-bottom: 6px; font-family: 'Montserrat', sans-serif; }
        textarea { width: 100%; min-height: 120px; padding: 12px; border: 1px solid var(--border); border-radius: var(--radius); font-family: 'Courier New', monospace; font-size: 12px; resize: vertical; }
        textarea:focus { outline: none; border-color: var(--orange); }
        .btn { display: inline-block; padding: 10px 18px; background: var(--orange); color: #fff; border: none; border-radius: var(--radius); font-family: 'Montserrat', sans-serif; font-size: 14px; font-weight: 600; cursor: pointer; margin-top: 12px; }
        .btn:hover { background: var(--orange-hover); }
        .error { background: #FEE2E2; color: #991B1B; padding: 10px 12px; border-radius: var(--radius); font-size: 13px; margin-bottom: 18px; }
        .ok { background: #D1FAE5; color: #065F46; padding: 10px 12px; border-radius: var(--radius); font-size: 13px; margin-bottom: 18px; }
        .help { margin-top: 18px; padding-top: 18px; border-top: 1px solid var(--border); font-size: 13px; color: var(--muted); line-height: 1.6; }
        .help code { background: #F3F4F6; padding: 2px 6px; border-radius: 3px; font-family: 'Courier New', monospace; font-size: 11.5px; }
    </style>
</head>
<body>
    <div class="card">
        <div class="logo">
            <svg viewBox="0 0 36 36" fill="none" xmlns="http://www.w3.org/2000/svg">
                <rect width="36" height="36" rx="8" fill="#F97316"/>
                <path d="M8 13L18 19.5L28 13" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
                <path d="M8 13V24C8 25.1 8.9 26 10 26H26C27.1 26 28 25.1 28 24V13" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
                <path d="M8 13L10 11H26L28 13" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
            <span>GraphenMail</span>
        </div>

        <h1>Activate this install</h1>
        <p class="subtitle">
            Paste your license key below. If you don't have one yet, send the
            Host Fingerprint to your vendor to receive a license.
        </p>

        {% if error %}<div class="error"><strong>{{ error }}</strong></div>{% endif %}
        {% if success %}<div class="ok">{{ success }}</div>{% endif %}

        <div class="fingerprint-label">Host Fingerprint (send this to your vendor)</div>
        <div class="fingerprint">{{ fingerprint }}</div>

        <form method="post" action="{{ url_for('license_gate.submit') }}">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            <label for="license" class="fingerprint-label">License Key</label>
            <textarea id="license" name="license" placeholder="Paste the single-line license key you received..." required></textarea>
            <button type="submit" class="btn">Activate</button>
        </form>

        <div class="help">
            <strong>Already have a license file?</strong> You can also drop it at
            <code>/etc/graphenmail/license.key</code> and restart the service.
            <br><br>
            <strong>Need help?</strong> Contact the vendor who sold you this copy.
        </div>
    </div>
</body>
</html>
"""


@bp.route("/", methods=["GET"])
def page():
    state = lic.validate(force=True)
    return render_template_string(
        GATE_TEMPLATE,
        fingerprint=state.host_fingerprint,
        error=state.error if not state.valid else None,
        success=None,
    )


@bp.route("/submit", methods=["POST"])
def submit():
    raw = (request.form.get("license", "") or "").strip()
    if not raw:
        flash("Please paste a license key.", "error")
        return redirect(url_for("license_gate.page"))

    try:
        lic.install_license(raw)
    except Exception as e:
        return render_template_string(
            GATE_TEMPLATE,
            fingerprint=lic.compute_host_fingerprint(),
            error=f"Could not save license file: {e}",
            success=None,
        )

    lic.invalidate_cache()
    state = lic.validate(force=True)
    if state.valid:
        return redirect("/")
    return render_template_string(
        GATE_TEMPLATE,
        fingerprint=state.host_fingerprint,
        error=state.error,
        success=None,
    )
