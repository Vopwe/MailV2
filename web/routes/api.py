"""
JSON API endpoints — task status polling, locations data, logs.
"""
import os
from flask import Blueprint, jsonify, request
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


@bp.route("/ip-status")
def ip_status():
    """IP rotation status — check if IPs are configured and rotating."""
    try:
        from search.rotator import get_status, get_available_ips, _load_ips
        status = get_status()
        status["configured_ips"] = _load_ips()
        status["currently_available"] = get_available_ips()
        return jsonify(status)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/logs")
def logs():
    """Return last N lines of server log."""
    lines = int(request.args.get("lines", 100))
    lines = min(lines, 500)

    # Try multiple common log locations
    log_paths = [
        os.path.join(config.BASE_DIR, "server.out.log"),
        os.path.join(config.BASE_DIR, "server.log"),
        "/var/log/graphenmail.log",
    ]

    for path in log_paths:
        if os.path.exists(path):
            try:
                with open(path, "r", errors="replace") as f:
                    all_lines = f.readlines()
                    tail = all_lines[-lines:]
                    return jsonify({
                        "file": path,
                        "total_lines": len(all_lines),
                        "showing": len(tail),
                        "lines": [l.rstrip() for l in tail],
                    })
            except Exception as e:
                return jsonify({"error": f"Can't read {path}: {e}"}), 500

    # If no file found, try journalctl
    try:
        import subprocess
        result = subprocess.run(
            ["journalctl", "-u", "graphenmail", "-n", str(lines), "--no-pager"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            return jsonify({
                "file": "journalctl -u graphenmail",
                "total_lines": lines,
                "showing": lines,
                "lines": result.stdout.strip().split("\n"),
            })
    except Exception:
        pass

    return jsonify({"error": "No log file found", "searched": log_paths}), 404

