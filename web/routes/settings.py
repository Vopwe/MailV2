"""
Settings — Bing scraper config, crawl config, IP management, password, license.
"""
import re

from flask import Blueprint, render_template, request, redirect, url_for, flash
import config
from web.auth import get_app_password, set_app_password, check_password, is_admin_session
from licensing import validator as license_validator

bp = Blueprint("settings", __name__)
_SMTP_FQDN_RE = re.compile(r"^[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")


def _smtp_identity_status(settings: dict) -> dict:
    ehlo = (settings.get("smtp_ehlo_hostname", "") or "").strip()
    mail_from = (settings.get("smtp_mail_from", "") or "").strip()

    if not ehlo and not mail_from:
        return {
            "level": "warning",
            "message": "Verifier SMTP identity is using automatic fallbacks. Set both values for stable mailbox checks.",
        }
    if not ehlo or not mail_from:
        return {
            "level": "warning",
            "message": "Set both SMTP EHLO Hostname and SMTP MAIL FROM. Partial SMTP identity can reduce verification reliability.",
        }
    if not _SMTP_FQDN_RE.match(ehlo) or "@" not in mail_from or " " in mail_from:
        return {
            "level": "warning",
            "message": "SMTP identity looks misconfigured. Use a real hostname like mail.yourdomain.com and a mailbox like verify@yourdomain.com.",
        }
    return {
        "level": "success",
        "message": "Verifier SMTP identity looks configured. Keep PTR/rDNS aligned with the EHLO hostname.",
    }


@bp.route("/license", methods=["POST"])
def update_license():
    """Replace the installed license file from the Settings → License tab."""
    if not is_admin_session():
        flash("Only the admin can change the license.", "error")
        return redirect(url_for("settings.index"))
    raw = (request.form.get("license", "") or "").strip()
    if not raw:
        flash("Paste a license key to activate.", "error")
        return redirect(url_for("settings.index") + "#tab-license")
    try:
        license_validator.install_license(raw)
    except Exception as e:
        flash(f"Could not save license: {e}", "error")
        return redirect(url_for("settings.index") + "#tab-license")
    license_validator.invalidate_cache()
    state = license_validator.validate(force=True)
    if state.valid:
        flash(f"License activated for {state.customer or 'this install'}.", "success")
    else:
        flash(f"License rejected: {state.error}", "error")
    return redirect(url_for("settings.index") + "#tab-license")


@bp.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        admin = is_admin_session()

        if not admin:
            # Non-admin users may only manage their own password (Security tab).
            new_password = request.form.get("new_password", "").strip()
            remove_pw = request.form.get("remove_password", "")
            if new_password:
                current_pw = request.form.get("current_password", "").strip()
                if get_app_password() and not check_password(current_pw):
                    flash("Current password is incorrect. Password not changed.", "error")
                else:
                    set_app_password(new_password)
                    flash("Password updated.", "success")
            elif remove_pw == "1":
                flash("Only the admin can remove password protection.", "error")
            else:
                flash("No changes.", "success")
            return redirect(url_for("settings.index"))

        # Parse outbound IPs from textarea (one per line)
        ips_raw = request.form.get("outbound_ips", "").strip()
        outbound_ips = [ip.strip() for ip in ips_raw.splitlines() if ip.strip()]

        updates = {
            "bing_concurrency": int(request.form.get("bing_concurrency", 5)),
            "bing_delay_min": float(request.form.get("bing_delay_min", 2.0)),
            "bing_delay_max": float(request.form.get("bing_delay_max", 5.0)),
            "bing_results_per_page": int(request.form.get("bing_results_per_page", 50)),
            "ddg_concurrency": int(request.form.get("ddg_concurrency", 5)),
            "ddg_delay_min": float(request.form.get("ddg_delay_min", 1.0)),
            "ddg_delay_max": float(request.form.get("ddg_delay_max", 3.0)),
            "outbound_ips": outbound_ips,
            "verify_concurrency": int(request.form.get("verify_concurrency", 30)),
            "max_concurrent_requests": int(request.form.get("max_concurrent_requests", 30)),
            "request_timeout": int(request.form.get("request_timeout", 12)),
            "crawl_delay": float(request.form.get("crawl_delay", 0.2)),
            "max_pages_per_domain": int(request.form.get("max_pages_per_domain", 5)),
            "urls_per_batch": int(request.form.get("urls_per_batch", 40)),
            "verify_timeout": int(request.form.get("verify_timeout", 10)),
            "smtp_ehlo_hostname": request.form.get("smtp_ehlo_hostname", "").strip(),
            "smtp_mail_from": request.form.get("smtp_mail_from", "").strip(),
            "robots_txt_mode": request.form.get("robots_txt_mode", "soft").strip(),
            "openrouter_api_key": request.form.get("openrouter_api_key", "").strip(),
            "openrouter_model": request.form.get("openrouter_model", "openrouter/free").strip(),
        }
        config.save_settings(updates)
        try:
            from verification import verifier
            verifier.clear_mx_cache()
        except Exception:
            pass

        # Handle password change
        new_password = request.form.get("new_password", "").strip()
        if new_password:
            current_pw = request.form.get("current_password", "").strip()
            if get_app_password() and not check_password(current_pw):
                flash("Current password is incorrect. Password not changed.", "error")
            else:
                set_app_password(new_password)
                flash("Settings saved. Password updated.", "success")
                return redirect(url_for("settings.index"))

        # Handle password removal
        remove_pw = request.form.get("remove_password", "")
        if remove_pw == "1":
            config.save_settings({"app_password": "", "app_password_hash": ""})
            flash("Settings saved. Password protection removed.", "success")
            return redirect(url_for("settings.index"))

        flash("Settings saved successfully.", "success")
        return redirect(url_for("settings.index"))

    settings = config.get_all_settings()

    # Get IP rotator status
    ip_status = {
        "total_ips": 0,
        "available_ips": 0,
        "cooled_down_ips": 0,
        "cooldown_list": [],
        "unhealthy_ips": 0,
        "unhealthy_list": [],
    }
    try:
        from search.rotator import get_status
        ip_status = get_status()
    except Exception:
        pass

    has_password = bool(get_app_password())
    runtime_paths = config.get_runtime_paths()
    license_state = license_validator.validate().to_dict()
    smtp_status = _smtp_identity_status(settings) if is_admin_session() else None
    return render_template(
        "settings.html",
        settings=settings,
        has_password=has_password,
        ip_status=ip_status,
        runtime_paths=runtime_paths,
        license_state=license_state,
        smtp_status=smtp_status,
    )
