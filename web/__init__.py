"""
Flask app factory.
"""
import os
from datetime import timedelta

from flask import Flask, redirect, request, url_for
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_wtf.csrf import CSRFProtect
import config
import database
from logging_setup import setup_logging
import tasks
from licensing import validator as license_validator

csrf = CSRFProtect()


def _license_skip_enabled() -> bool:
    """Dev-only escape hatch. Lets local devs work without signing a license."""
    return os.getenv("GM_SKIP_LICENSE", "0") == "1"


def create_app() -> Flask:
    setup_logging()
    app = Flask(
        __name__,
        static_folder="static",
        template_folder="templates",
    )
    app.secret_key = config.get_secret_key()

    # CSRF protection for all POST/PUT/DELETE/PATCH forms.
    # Token lifetime matches session: 24h sliding window.
    app.config["WTF_CSRF_TIME_LIMIT"] = 60 * 60 * 24
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=7)
    csrf.init_app(app)

    database.init_db()
    tasks.init_tasks()

    # ── Rate Limiting ────────────────────────────────────────────
    limiter = Limiter(
        app=app,
        key_func=get_remote_address,
        default_limits=["200 per minute"],
        storage_uri="memory://",
    )
    # Stricter limits on auth endpoints
    limiter.limit("10 per minute")(lambda: None)  # placeholder, real limits below

    # ── Auth ─────────────────────────────────────────────────────
    from web.auth import init_auth
    init_auth(app)

    from web.routes.dashboard import bp as dashboard_bp
    from web.routes.campaigns import bp as campaigns_bp
    from web.routes.emails import bp as emails_bp
    from web.routes.verification import bp as verification_bp
    from web.routes.settings import bp as settings_bp
    from web.routes.admin_licenses import bp as admin_licenses_bp
    from web.routes.api import bp as api_bp

    from web.routes.license_gate import bp as license_gate_bp
    from web.routes.onboarding import bp as onboarding_bp, needs_onboarding

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(campaigns_bp, url_prefix="/campaigns")
    app.register_blueprint(emails_bp, url_prefix="/emails")
    app.register_blueprint(verification_bp, url_prefix="/verification")
    app.register_blueprint(settings_bp, url_prefix="/settings")
    app.register_blueprint(admin_licenses_bp)
    app.register_blueprint(api_bp, url_prefix="/api")
    app.register_blueprint(license_gate_bp)
    app.register_blueprint(onboarding_bp)
    # /api/* is read-only JSON; exempt from CSRF for potential non-browser callers.
    csrf.exempt(api_bp)

    # ── License Gate ─────────────────────────────────────────────
    @app.before_request
    def _enforce_license():
        if _license_skip_enabled():
            return None
        endpoint = request.endpoint or ""
        # Always allow license gate, static, logout, and Flask internals.
        if endpoint.startswith("license_gate.") or endpoint == "static" or endpoint == "auth.logout":
            return None
        state = license_validator.validate()
        if state.valid:
            return None
        return redirect(url_for("license_gate.page"))

    # ── Onboarding Gate ──────────────────────────────────────────
    @app.before_request
    def _enforce_onboarding():
        endpoint = request.endpoint or ""
        # Allow static, logout, license gate, onboarding itself, auth pages.
        if (endpoint.startswith("onboarding.")
                or endpoint.startswith("license_gate.")
                or endpoint.startswith("auth.")
                or endpoint == "static"):
            return None
        if needs_onboarding():
            return redirect(url_for("onboarding.step1"))
        return None

    # Rate-limit login attempts more strictly
    limiter.limit("5 per minute")(app.view_functions.get("auth.login", lambda: None))

    app.teardown_appcontext(lambda _exc: database.close_db())

    return app
