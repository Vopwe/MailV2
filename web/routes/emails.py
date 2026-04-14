"""
Email database — browse, filter, export CSV.
"""
import csv
import io
from urllib.parse import urlencode
from flask import Blueprint, render_template, request, Response
import database

bp = Blueprint("emails", __name__)


def _build_page_url(page: int) -> str:
    args = request.args.to_dict(flat=False)
    args["page"] = [str(page)]
    return f"?{urlencode(args, doseq=True)}"


@bp.route("/")
def list_emails():
    # Gather filter params
    campaign_id = request.args.get("campaign_id", type=int)
    niche = request.args.get("niche", "").strip() or None
    city = request.args.get("city", "").strip() or None
    country = request.args.get("country", "").strip() or None
    verification = request.args.get("verification", "").strip() or None
    domain = request.args.get("domain", "").strip() or None
    search = request.args.get("search", "").strip() or None
    page = request.args.get("page", 1, type=int)

    emails_list, total = database.get_emails(
        campaign_id=campaign_id, niche=niche, city=city,
        country=country, verification=verification, domain=domain,
        search=search, page=page, per_page=50,
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
    prev_url = _build_page_url(page - 1) if page > 1 else None
    next_url = _build_page_url(page + 1) if page < total_pages else None

    return render_template("emails.html",
                           emails=emails_list, total=total,
                           page=page, total_pages=total_pages,
                           campaigns=campaigns, niches=niches,
                           cities=cities, countries=countries,
                           filters=request.args,
                           prev_url=prev_url,
                           next_url=next_url)


@bp.route("/export")
def export_csv():
    campaign_id = request.args.get("campaign_id", type=int)
    niche = request.args.get("niche", "").strip() or None
    city = request.args.get("city", "").strip() or None
    country = request.args.get("country", "").strip() or None
    verification = request.args.get("verification", "").strip() or None
    domain = request.args.get("domain", "").strip() or None
    exclude_providers = request.args.get("exclude_providers", "").strip()
    columns_param = request.args.get("columns", "").strip()

    rows = database.get_all_emails_filtered(
        campaign_id=campaign_id, niche=niche, city=city,
        country=country, verification=verification, domain=domain,
    )

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
