"""
Campaigns — create, list, run, detail, delete.
"""
import json
import logging
from flask import Blueprint, render_template, request, redirect, url_for, flash
import database
import config
import tasks
from web.routes._campaign_runner import run_campaign

logger = logging.getLogger(__name__)
bp = Blueprint("campaigns", __name__)


@bp.route("/")
def list_campaigns():
    campaigns = database.get_campaigns()
    return render_template("campaigns/list.html", campaigns=campaigns)


@bp.route("/new", methods=["GET", "POST"])
def new_campaign():
    locations = config.get_locations()

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        niches_raw = request.form.get("niches", "").strip()
        countries = request.form.getlist("countries")
        cities = request.form.getlist("cities")

        if not name or not niches_raw or not countries:
            flash("Please fill in all required fields.", "error")
            return render_template("campaigns/new.html", locations=locations)

        niches = [n.strip() for n in niches_raw.split(",") if n.strip()]

        if not cities:
            cities = ["*"]

        campaign_id = database.insert_campaign(name, niches, countries, cities)
        flash(f"Campaign '{name}' created successfully.", "success")
        return redirect(url_for("campaigns.detail", campaign_id=campaign_id))

    return render_template("campaigns/new.html", locations=locations)


@bp.route("/<int:campaign_id>")
def detail(campaign_id):
    campaign = database.get_campaign(campaign_id)
    if not campaign:
        flash("Campaign not found.", "error")
        return redirect(url_for("campaigns.list_campaigns"))

    requested_task_id = request.args.get("campaign_task", "").strip()
    task_id = requested_task_id
    current_task = tasks.get_task(task_id) if task_id else None
    if current_task and current_task.campaign_id != campaign_id:
        current_task = None
        task_id = ""

    if current_task is None:
        current_task = tasks.find_latest_task(task_type="campaign", campaign_id=campaign_id)
        if current_task and current_task.status == "running":
            task_id = current_task.task_id
        else:
            task_id = ""

    urls = database.get_urls(campaign_id)
    emails_list, total_emails = database.get_emails(campaign_id=campaign_id, per_page=50)
    crawl_stats = database.get_campaign_stats(campaign_id)

    # IP rotation status
    ip_status = {"total_ips": 0, "available_ips": 0, "cooled_down_ips": 0}
    try:
        from search.rotator import get_status, _load_ips
        ip_status = get_status()
        ip_status["configured_ips"] = _load_ips()
    except Exception:
        pass

    return render_template("campaigns/detail.html",
                           campaign=campaign, urls=urls,
                           emails=emails_list, total_emails=total_emails,
                           crawl_stats=crawl_stats, task_id=task_id,
                           current_task=current_task, ip_status=ip_status)


@bp.route("/<int:campaign_id>/run", methods=["POST"])
def run(campaign_id):
    campaign = database.get_campaign(campaign_id)
    if not campaign:
        flash("Campaign not found.", "error")
        return redirect(url_for("campaigns.list_campaigns"))

    if campaign["status"] in ("generating", "crawling"):
        flash("Campaign is already running.", "warning")
        return redirect(url_for("campaigns.detail", campaign_id=campaign_id))

    task_id = tasks.create_task(task_type="campaign", campaign_id=campaign_id)
    tasks.run_in_background(run_campaign, task_id, campaign_id)

    flash(f"Campaign started. Task ID: {task_id}", "success")
    return redirect(url_for("campaigns.detail", campaign_id=campaign_id, campaign_task=task_id))


@bp.route("/<int:campaign_id>/cancel", methods=["POST"])
def cancel(campaign_id):
    current = tasks.find_latest_task(
        task_type="campaign",
        campaign_id=campaign_id,
        statuses=("running",),
    )
    if current and tasks.cancel_task(current.task_id):
        flash("Cancellation requested. Campaign will stop at the next safe point.", "success")
    else:
        flash("No running campaign task to cancel.", "warning")
    return redirect(url_for("campaigns.detail", campaign_id=campaign_id))


@bp.route("/<int:campaign_id>/delete", methods=["POST"])
def delete(campaign_id):
    database.delete_campaign(campaign_id)
    flash("Campaign deleted.", "success")
    return redirect(url_for("campaigns.list_campaigns"))
