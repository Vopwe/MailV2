"""
Admin-only license issuer UI.

Use only on the vendor/admin install that has access to the private signing key.
Do not enable or configure this on customer installs.
"""
from pathlib import Path

from flask import Blueprint, flash, redirect, render_template, request, url_for

import config
from licensing import issue as license_issue
from licensing import validator as license_validator
from web.auth import admin_required

bp = Blueprint("admin_licenses", __name__, url_prefix="/admin/licenses")

DEFAULT_FEATURES = "ai_urls,ip_rotation"
EXPIRY_PRESETS = {
    "perpetual": {"label": "Perpetual", "days": None, "months": None, "expires": None, "perpetual": True},
    "30_days": {"label": "30 days", "days": 30, "months": None, "expires": None, "perpetual": False},
    "1_month": {"label": "1 month", "days": None, "months": 1, "expires": None, "perpetual": False},
    "3_months": {"label": "3 months", "days": None, "months": 3, "expires": None, "perpetual": False},
    "1_year": {"label": "1 year", "days": 365, "months": None, "expires": None, "perpetual": False},
    "custom_date": {"label": "Custom date", "days": None, "months": None, "expires": None, "perpetual": False},
}


def _stored_signing_key_path() -> str:
    return (config.get_setting("license_signing_key_path", "") or "").strip()


def _default_form_values() -> dict:
    return {
        "customer": "",
        "host_fingerprint": "*",
        "features": DEFAULT_FEATURES,
        "signing_key_path": _stored_signing_key_path(),
        "expiry_preset": "1_month",
        "custom_expires_at": "",
    }


def _build_context(*, form_values: dict | None = None, license_text: str = "", resolved_expires_at: str | None = None) -> dict:
    form = _default_form_values()
    if form_values:
        form.update(form_values)

    signing_key_path = (form.get("signing_key_path", "") or "").strip()
    key_path = Path(signing_key_path) if signing_key_path else None
    key_exists = bool(key_path and key_path.exists())
    return {
        "form_values": form,
        "license_text": license_text,
        "resolved_expires_at": resolved_expires_at,
        "signing_key_status": {
            "path": signing_key_path,
            "exists": key_exists,
            "public_key_path": str(license_validator.PUBLIC_KEY_PATH),
        },
        "expiry_presets": EXPIRY_PRESETS,
    }


@bp.route("/", methods=["GET", "POST"])
@admin_required
def index():
    if request.method == "GET":
        return render_template("admin/license_lab.html", **_build_context())

    action = (request.form.get("action", "") or "").strip()
    if action == "save_config":
        signing_key_path = (request.form.get("signing_key_path", "") or "").strip()
        config.save_settings({"license_signing_key_path": signing_key_path})
        flash("License signing key path saved.", "success")
        return redirect(url_for("admin_licenses.index"))

    form_values = {
        "customer": (request.form.get("customer", "") or "").strip(),
        "host_fingerprint": (request.form.get("host_fingerprint", "") or "").strip() or "*",
        "features": (request.form.get("features", "") or "").strip(),
        "signing_key_path": (request.form.get("signing_key_path", "") or "").strip(),
        "expiry_preset": (request.form.get("expiry_preset", "1_month") or "1_month").strip(),
        "custom_expires_at": (request.form.get("custom_expires_at", "") or "").strip(),
    }

    config.save_settings({"license_signing_key_path": form_values["signing_key_path"]})

    try:
        if not form_values["customer"]:
            raise ValueError("Customer name is required.")
        if not form_values["signing_key_path"]:
            raise ValueError("Signing key path is required.")

        preset = EXPIRY_PRESETS.get(form_values["expiry_preset"])
        if preset is None:
            raise ValueError("Unknown expiry preset.")

        resolved_expires_at = license_issue.resolve_expiry(
            expires=form_values["custom_expires_at"] if form_values["expiry_preset"] == "custom_date" else preset["expires"],
            days=preset["days"],
            months=preset["months"],
            perpetual=preset["perpetual"],
        )
        features = [item.strip() for item in form_values["features"].split(",") if item.strip()]
        signing_key = license_issue.load_signing_key(form_values["signing_key_path"])
        license_text = license_issue.generate_license_text(
            signing_key=signing_key,
            customer=form_values["customer"],
            host_fingerprint=form_values["host_fingerprint"],
            expires_at=resolved_expires_at,
            features=features,
        )
        flash("License generated.", "success")
        return render_template(
            "admin/license_lab.html",
            **_build_context(
                form_values=form_values,
                license_text=license_text,
                resolved_expires_at=resolved_expires_at,
            ),
        )
    except (FileNotFoundError, ValueError) as e:
        flash(str(e), "error")
        return render_template("admin/license_lab.html", **_build_context(form_values=form_values))
