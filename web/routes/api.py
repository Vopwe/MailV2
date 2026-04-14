"""
JSON API endpoints — task status polling, locations data.
"""
from flask import Blueprint, jsonify
import tasks
import config

bp = Blueprint("api", __name__)


@bp.route("/tasks/<task_id>")
def task_status(task_id):
    task = tasks.get_task(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    return jsonify(task.to_dict())


@bp.route("/tasks")
def all_tasks():
    return jsonify(tasks.get_all_tasks())


@bp.route("/locations")
def locations():
    return jsonify(config.get_locations())


@bp.route("/locations/<country>/cities")
def cities_for_country(country):
    locs = config.get_locations()
    data = locs.get(country, {})
    return jsonify(data.get("cities", []))
