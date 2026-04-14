"""
IP Rotator — round-robin across multiple outbound IPs.
Supports IPv4 and IPv6.  Gracefully degrades to single IP.
"""
import os
import time
import logging
import threading
from pathlib import Path

import config

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_index = 0
_cooldowns: dict[str, float] = {}  # ip -> timestamp when cooldown expires

COOLDOWN_SECONDS = 300  # 5 minutes


def _load_ips() -> list[str]:
    """Load outbound IPs from settings.json → env → ips.txt → empty."""
    # 1. settings.json
    ips = config.get_setting("outbound_ips", None)
    if ips and isinstance(ips, list):
        result = [ip.strip() for ip in ips if ip.strip()]
        if result:
            return result

    # 2. Environment variable
    env_ips = os.getenv("OUTBOUND_IPS", "")
    if env_ips:
        result = [ip.strip() for ip in env_ips.split(",") if ip.strip()]
        if result:
            return result

    # 3. ips.txt file in project root
    ips_file = Path(config.BASE_DIR) / "ips.txt"
    if ips_file.exists():
        result = [
            line.strip()
            for line in ips_file.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        if result:
            return result

    return []


def get_available_ips() -> list[str]:
    """Return all configured IPs, with cooled-down ones filtered out."""
    all_ips = _load_ips()
    if not all_ips:
        return []

    now = time.time()
    with _lock:
        available = [ip for ip in all_ips if _cooldowns.get(ip, 0) < now]

    # If all IPs are on cooldown, return them anyway (better than nothing)
    if not available:
        logger.warning("All IPs on cooldown — using them anyway")
        return all_ips

    return available


def get_next_ip() -> str | None:
    """Get next IP in round-robin rotation. Returns None if no IPs configured."""
    global _index
    available = get_available_ips()
    if not available:
        return None

    with _lock:
        ip = available[_index % len(available)]
        _index += 1

    return ip


def cooldown_ip(ip: str):
    """Put an IP on cooldown (e.g., after 429 or captcha detection)."""
    with _lock:
        _cooldowns[ip] = time.time() + COOLDOWN_SECONDS
    logger.warning(f"IP {ip} on cooldown for {COOLDOWN_SECONDS}s")


def get_ip_count() -> int:
    """Total configured IPs (including cooled-down)."""
    return len(_load_ips())


def get_status() -> dict:
    """Status summary for debugging."""
    all_ips = _load_ips()
    now = time.time()
    with _lock:
        cooled = [ip for ip in all_ips if _cooldowns.get(ip, 0) >= now]
    return {
        "total_ips": len(all_ips),
        "available_ips": len(all_ips) - len(cooled),
        "cooled_down_ips": len(cooled),
        "cooldown_list": cooled,
    }
