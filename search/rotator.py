"""
IP Rotator - round-robin across multiple outbound IPs.
Supports IPv4 and IPv6. Gracefully degrades to a single default route.
"""
import ipaddress
import logging
import os
import socket
import threading
import time
from pathlib import Path

import config

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_index = 0
_cooldowns: dict[str, float] = {}
_health_cache: dict[str, tuple[float, bool]] = {}

COOLDOWN_SECONDS = 300
DEAD_IP_RETRY_SECONDS = 60
HEALTH_CACHE_SECONDS = 300
HEALTHCHECK_TIMEOUT_SECONDS = 2.5
HEALTHCHECK_HOSTS = (
    "www.bing.com",
    "html.duckduckgo.com",
)


def _load_ips() -> list[str]:
    """Load outbound IPs from settings.json -> env -> ips.txt -> empty."""
    ips = config.get_setting("outbound_ips", None)
    if ips and isinstance(ips, list):
        result = [ip.strip() for ip in ips if ip.strip()]
        if result:
            return result

    env_ips = os.getenv("OUTBOUND_IPS", "")
    if env_ips:
        result = [ip.strip() for ip in env_ips.split(",") if ip.strip()]
        if result:
            return result

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


def _ip_family(ip: str) -> socket.AddressFamily | None:
    try:
        parsed = ipaddress.ip_address(ip)
    except ValueError:
        return None
    return socket.AF_INET6 if parsed.version == 6 else socket.AF_INET


def _source_address(ip: str, family: socket.AddressFamily):
    if family == socket.AF_INET6:
        return (ip, 0, 0, 0)
    return (ip, 0)


def _get_cached_health(ip: str, now: float | None = None) -> bool | None:
    now = time.time() if now is None else now
    with _lock:
        cached = _health_cache.get(ip)
        if not cached:
            return None
        expires_at, healthy = cached
        if expires_at < now:
            _health_cache.pop(ip, None)
            return None
        return healthy


def record_ip_healthy(ip: str):
    """Refresh health cache for an IP after a successful request."""
    with _lock:
        _health_cache[ip] = (time.time() + HEALTH_CACHE_SECONDS, True)


def mark_ip_unhealthy(ip: str, reason: str = "", retry_after: int = DEAD_IP_RETRY_SECONDS):
    """Temporarily suppress a dead or unreachable IP."""
    retry_after = max(5, int(retry_after))
    with _lock:
        _health_cache[ip] = (time.time() + retry_after, False)
    if reason:
        logger.warning("IP %s marked unhealthy for %ss: %s", ip, retry_after, reason)
    else:
        logger.warning("IP %s marked unhealthy for %ss", ip, retry_after)


def _probe_ip_health(ip: str) -> bool:
    family = _ip_family(ip)
    if family is None:
        mark_ip_unhealthy(ip, "invalid IP format")
        return False

    last_error = "no matching address family on health-check targets"
    for host in HEALTHCHECK_HOSTS:
        try:
            addrinfos = socket.getaddrinfo(host, 443, family, socket.SOCK_STREAM)
        except socket.gaierror as exc:
            last_error = str(exc)
            continue

        for _family, socktype, proto, _canonname, sockaddr in addrinfos:
            try:
                with socket.create_connection(
                    sockaddr,
                    timeout=HEALTHCHECK_TIMEOUT_SECONDS,
                    source_address=_source_address(ip, family),
                ):
                    record_ip_healthy(ip)
                    return True
            except OSError as exc:
                last_error = str(exc)

    mark_ip_unhealthy(ip, last_error)
    return False


def _is_ip_ready(ip: str, now: float) -> bool:
    with _lock:
        return _cooldowns.get(ip, 0) < now


def get_available_ips() -> list[str]:
    """Return healthy configured IPs. Empty list means use default route."""
    all_ips = _load_ips()
    if not all_ips:
        return []

    now = time.time()
    ready = [ip for ip in all_ips if _is_ip_ready(ip, now)]
    if not ready:
        logger.warning("All IPs on cooldown - using default route")
        return []

    healthy = []
    for ip in ready:
        cached = _get_cached_health(ip, now)
        if cached is True:
            healthy.append(ip)
        elif cached is None and _probe_ip_health(ip):
            healthy.append(ip)

    if healthy:
        return healthy

    logger.warning("No healthy outbound IPs available - using default route")
    return []


def get_next_ip() -> str | None:
    """Get next healthy IP in round-robin rotation. None means default route."""
    global _index
    available = get_available_ips()
    if not available:
        return None

    with _lock:
        ip = available[_index % len(available)]
        _index += 1

    return ip


def cooldown_ip(ip: str):
    """Put an IP on cooldown after a remote-side block like 429/captcha."""
    with _lock:
        _cooldowns[ip] = time.time() + COOLDOWN_SECONDS
    logger.warning("IP %s on cooldown for %ss", ip, COOLDOWN_SECONDS)


def get_ip_count() -> int:
    """Total configured IPs."""
    return len(_load_ips())


def get_status() -> dict:
    """Status summary for debugging."""
    all_ips = _load_ips()
    now = time.time()
    with _lock:
        cooled = [ip for ip in all_ips if _cooldowns.get(ip, 0) >= now]
        unhealthy = [
            ip
            for ip, (expires_at, healthy) in _health_cache.items()
            if not healthy and expires_at >= now
        ]
    return {
        "total_ips": len(all_ips),
        "available_ips": max(len(all_ips) - len(cooled) - len(unhealthy), 0),
        "cooled_down_ips": len(cooled),
        "cooldown_list": cooled,
        "unhealthy_ips": len(unhealthy),
        "unhealthy_list": unhealthy,
    }
