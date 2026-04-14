"""
Email database — browse, filter, export CSV.
"""
import csv
import io
from urllib.parse import urlencode
from flask import Blueprint, render_template, request, Response, redirect, url_for, flash
import database

bp = Blueprint("emails", __name__)

CLEANUP_ALLOWED_STATUSES = ("invalid", "spam_trap")


def _build_page_url(page: int) -> str:
    args = request.args.to_dict(flat=False)
    args["page"] = [str(page)]
    return f"?{urlencode(args, doseq=True)}"


def _parse_filters(values) -> dict:
    campaign_id = values.get("campaign_id", type=int) if hasattr(values, "get") else None
    if campaign_id is None:
        raw_campaign_id = values.get("campaign_id", "") if hasattr(values, "get") else ""
        campaign_id = int(raw_campaign_id) if str(raw_campaign_id).isdigit() else None

    return {
        "campaign_id": campaign_id,
        "niche": (values.get("niche", "") or "").strip() or None,
        "city": (values.get("city", "") or "").strip() or None,
        "country": (values.get("country", "") or "").strip() or None,
        "verification": (values.get("verification", "") or "").strip() or None,
        "domain": (values.get("domain", "") or "").strip() or None,
        "search": (values.get("search", "") or "").strip() or None,
    }


def _clean_filter_query(filters: dict) -> dict:
    return {key: value for key, value in filters.items() if value not in (None, "", [])}


@bp.route("/")
def list_emails():
    filters_dict = _parse_filters(request.args)
    page = request.args.get("page", 1, type=int)

    emails_list, total = database.get_emails(
        page=page, per_page=50, **filters_dict,
    )

    total_pages = (total + 49) // 50
    campaigns = database.get_campaigns()
    campaign_map = {c["id"]: c["name"] for c in campaigns}

    # Attach campaign name to each email row
    for e in emails_list:
        e["campaign_name"] = campaign_map.get(e.get("campaign_id"), "-")

    # Get distinct values for filter dropdowns
    niches = database.get_distinct_values("niche")
    cities = database.get_distinct_values("city")
    countries = database.get_distinct_values("country")
    cleanup_counts = database.get_email_status_counts(**filters_dict)
    cleanup_history = database.get_cleanup_runs(limit=8)
    prev_url = _build_page_url(page - 1) if page > 1 else None
    next_url = _build_page_url(page + 1) if page < total_pages else None

    return render_template("emails.html",
                           emails=emails_list, total=total,
                           page=page, total_pages=total_pages,
                           campaigns=campaigns, niches=niches,
                           cities=cities, countries=countries,
                           filters=request.args,
                           cleanup_counts=cleanup_counts,
                           cleanup_history=cleanup_history,
                           cleanup_statuses=CLEANUP_ALLOWED_STATUSES,
                           prev_url=prev_url,
                           next_url=next_url)


@bp.route("/cleanup", methods=["POST"])
def cleanup():
    statuses = [s for s in request.form.getlist("statuses") if s in CLEANUP_ALLOWED_STATUSES]
    if not statuses:
        flash("Select at least one cleanup status.", "error")
        return redirect(url_for("emails.list_emails", **_clean_filter_query(_parse_filters(request.form))))

    filters_dict = _parse_filters(request.form)
    preview_count = database.count_emails_for_cleanup(statuses=statuses, **filters_dict)
    if preview_count == 0:
        flash("No matching emails to delete for the current Database filters.", "warning")
        return redirect(url_for("emails.list_emails", **_clean_filter_query(filters_dict)))

    deleted_count = database.delete_emails_for_cleanup(statuses=statuses, **filters_dict)
    database.save_cleanup_run(
        statuses=statuses,
        filters=_clean_filter_query(filters_dict),
        preview_count=preview_count,
        deleted_count=deleted_count,
    )
    for campaign in database.get_campaigns():
        database.update_campaign_counts(campaign["id"])

    flash(f"Deleted {deleted_count} emails from the current Database view.", "success")
    return redirect(url_for("emails.list_emails", **_clean_filter_query(filters_dict)))


@bp.route("/export")
def export_csv():
    filters_dict = _parse_filters(request.args)
    exclude_providers = request.args.get("exclude_providers", "").strip()
    columns_param = request.args.get("columns", "").strip()

    rows = database.get_all_emails_filtered(**filters_dict)

    # Filter out excluded provider domains
    if exclude_providers:
        excluded = {d.strip().lower() for d in exclude_providers.split(",") if d.strip()}
        rows = [r for r in rows if r["domain"].lower() not in excluded]

    # Column mapping: internal key → CSV header
    all_columns = [
        ("email", "Email"),
        ("domain", "Domain"),
        ("source_url", "Source URL"),
        ("niche", "Niche"),
        ("city", "City"),
        ("country", "Country"),
        ("verification", "Verification"),
        ("verification_method", "Verification Method"),
        ("mailbox_confidence", "Mailbox Confidence"),
        ("domain_confidence", "Domain Confidence"),
        ("is_catch_all", "Catch-All"),
        ("mx_valid", "MX Valid"),
        ("smtp_valid", "SMTP Valid"),
        ("extracted_at", "Extracted At"),
    ]

    # Filter to selected columns (if specified), always include email
    if columns_param:
        selected = [c.strip() for c in columns_param.split(",") if c.strip()]
        if "email" not in selected:
            selected.insert(0, "email")
        columns = [(key, label) for key, label in all_columns if key in selected]
    else:
        columns = all_columns

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([label for _, label in columns])
    for row in rows:
        writer.writerow([row.get(key, "") for key, _ in columns])

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=emails_export.csv"},
    )
