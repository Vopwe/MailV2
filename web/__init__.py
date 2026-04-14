"""
Flask app factory.
"""
from flask import Flask
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import config
import database
import tasks


def create_app() -> Flask:
    app = Flask(
        __name__,
        static_folder="static",
        template_folder="templates",
    )
    app.secret_key = config.get_secret_key()

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
    from web.routes.api import bp as api_bp

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(campaigns_bp, url_prefix="/campaigns")
    app.register_blueprint(emails_bp, url_prefix="/emails")
    app.register_blueprint(verification_bp, url_prefix="/verification")
    app.register_blueprint(settings_bp, url_prefix="/settings")
    app.register_blueprint(api_bp, url_prefix="/api")

    # Rate-limit login attempts more strictly
    limiter.limit("5 per minute")(app.view_functions.get("auth.login", lambda: None))

    app.teardown_appcontext(lambda _exc: database.close_db())

    return app
