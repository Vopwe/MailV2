"""
Verification — trigger bulk verification from the UI.
Handles: verify selected, verify by campaign, verify all, re-verify unknown.
Tracks detailed verification stats for health monitoring.
"""
import json
import logging
from datetime import datetime
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from flask import Blueprint, render_template, request, redirect, url_for, flash
import database
import tasks
from verification.verifier import verify_emails_batch, clear_mx_cache

logger = logging.getLogger(__name__)
bp = Blueprint("verification", __name__)


def _redirect_with_task(target_url: str | None, task_id: str):
    """Attach the current verification task ID to the redirect target."""
    if not target_url:
        return redirect(url_for("verification.index", verification_task=task_id))

    parsed = urlparse(target_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["verification_task"] = task_id
    updated = parsed._replace(query=urlencode(query))
    return redirect(urlunparse(updated))


async def _run_verification(task_id: str, email_ids: list[int] | None,
                            campaign_id: int | None, include_unknown: bool = False,
                            include_all: bool = False):
    """Background verification task — no artificial limit."""
    clear_mx_cache()
    start_time = datetime.now()

    if email_ids:
        emails = database.get_emails_by_ids(email_ids)
    elif campaign_id:
        emails = database.get_unverified_emails(campaign_id=campaign_id,
                                                 include_unknown=include_unknown,
                                                 include_all=include_all)
    else:
        emails = database.get_unverified_emails(include_unknown=include_unknown,
                                                 include_all=include_all)

    if not emails:
        tasks.complete_task(task_id, "No emails to verify.")
        return

    total = len(emails)
    tasks.update_task(task_id, total=total,
                      message=f"Verifying {total} emails (0 valid, 0 invalid)...")

    def on_progress(done, total, counts):
        trap = counts.get('spam_trap', 0)
        tasks.update_task(
            task_id, progress=done, total=total,
            message=(f"Verified {done}/{total} — "
                     f"{counts['valid']} valid, {counts['invalid']} invalid, "
                     f"{counts['risky']} risky, {trap} traps, {counts['unknown']} unknown"),
        )

    results, vstats = await verify_emails_batch(emails, on_progress=on_progress)

    # Batch update database
    for r in results:
        database.update_email_verification(
            email_id=r["id"],
            verification=r["verification"],
            mx_valid=r["mx_valid"],
            smtp_valid=r["smtp_valid"],
            verification_method=r["verification_method"],
            mailbox_confidence=r["mailbox_confidence"],
            domain_confidence=r["domain_confidence"],
            is_catch_all=r["is_catch_all"],
        )

    # Save verification stats
    end_time = datetime.now()
    vstats["started_at"] = start_time.isoformat()
    vstats["completed_at"] = end_time.isoformat()
    vstats["duration_seconds"] = round((end_time - start_time).total_seconds(), 1)
    vstats["campaign_id"] = campaign_id
    database.save_verification_stats(vstats)

    valid = vstats["result_valid"]
    invalid = vstats["result_invalid"]
    risky = vstats["result_risky"]
    traps = vstats["result_spam_trap"]
    unknown = vstats["result_unknown"]
    tasks.complete_task(
        task_id,
        f"Done! {total} emails: {valid} valid, {invalid} invalid, {risky} risky, {traps} traps, {unknown} unknown"
    )


@bp.route("/bulk-delete", methods=["POST"])
def bulk_delete():
    statuses = request.form.getlist("statuses")
    allowed = {"invalid", "spam_trap"}
    statuses = [s for s in statuses if s in allowed]
    if not statuses:
        flash("No statuses selected.", "error")
        return redirect(url_for("verification.index"))
    count = database.bulk_delete_emails(statuses)
    # Update campaign counts
    for c in database.get_campaigns():
        database.update_campaign_counts(c["id"])
    flash(f"Deleted {count} emails ({', '.join(statuses)}).", "success")
    return redirect(url_for("verification.index"))


@bp.route("/", methods=["GET", "POST"])
def index():
    campaigns = database.get_campaigns()
    current_task_id = request.args.get("verification_task", "").strip() or None

    if request.method == "POST":
        action = request.form.get("action", "")
        email_ids = None
        campaign_id = None
        include_unknown = False
        include_all = False

        if action == "verify_selected":
            ids_raw = request.form.getlist("email_ids")
            email_ids = [int(i) for i in ids_raw if i.isdigit()]
            if not email_ids:
                flash("No emails selected.", "error")
                return redirect(request.referrer or url_for("verification.index"))

        elif action == "verify_campaign":
            campaign_id = request.form.get("campaign_id", type=int)

        elif action == "verify_all":
            pass

        elif action == "reverify_unknown":
            include_unknown = True

        elif action == "reverify_campaign":
            campaign_id = request.form.get("campaign_id", type=int)
            include_unknown = True

        elif action == "reverify_all":
            include_all = True

        elif action == "reverify_all_campaign":
            campaign_id = request.form.get("campaign_id", type=int)
            include_all = True

        task_id = tasks.create_task(task_type="verification")
        tasks.run_in_background(_run_verification, task_id, email_ids, campaign_id, include_unknown, include_all)
        flash(f"Verification started. Watch progress above.", "success")

        # Redirect back to referrer if came from emails page
        referrer = request.form.get("redirect_to", "")
        return _redirect_with_task(referrer, task_id)

    stats = database.get_stats()
    verify_history = database.get_verification_stats(limit=5)
    return render_template(
        "verification.html",
        campaigns=campaigns,
        stats=stats,
        task_id=current_task_id,
        verify_history=verify_history,
    )
