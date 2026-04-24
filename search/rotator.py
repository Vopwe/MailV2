"""
IP Rotator - result-aware outbound IP selection.
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
_ip_stats: dict[str, dict[str, float | int]] = {}

COOLDOWN_SECONDS = 300
DEAD_IP_RETRY_SECONDS = 60
HEALTH_CACHE_SECONDS = 300
HEALTHCHECK_TIMEOUT_SECONDS = 2.5
PROBE_SUCCESS_BONUS = 0.25
SEARCH_SUCCESS_BONUS_MAX = 1.25
EMPTY_RESULT_PENALTY = 1.5
UNHEALTHY_PENALTY = 3.0
COOLDOWN_PENALTY = 4.0
RECENT_USE_WINDOW_SECONDS = 20
RECENT_USE_PENALTY = 1.25
SCORE_TIE_MARGIN = 0.2
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


def _parse_health_cache_entry(entry) -> tuple[float, bool] | None:
    if isinstance(entry, dict):
        try:
            return float(entry.get("expires_at", 0)), bool(entry.get("healthy"))
        except (TypeError, ValueError):
            return None
    if isinstance(entry, (list, tuple)) and len(entry) >= 2:
        try:
            return float(entry[0]), bool(entry[1])
        except (TypeError, ValueError):
            return None
    return None


def _get_cached_health(ip: str, now: float | None = None) -> bool | None:
    now = time.time() if now is None else now
    with _lock:
        cached_entry = _health_cache.get(ip)
        if not cached_entry:
            return None
        cached = _parse_health_cache_entry(cached_entry)
        if not cached:
            _health_cache.pop(ip, None)
            return None
        expires_at, healthy = cached
        if expires_at < now:
            _health_cache.pop(ip, None)
            return None
        return healthy


def _ensure_ip_stats(ip: str) -> dict[str, float | int]:
    return _ip_stats.setdefault(
        ip,
        {
            "score": 0.0,
            "last_used": 0.0,
            "last_success_at": 0.0,
            "probe_successes": 0,
            "search_successes": 0,
            "empty_results": 0,
            "failures": 0,
        },
    )


def _adjust_score(ip: str, delta: float) -> dict[str, float | int]:
    stats = _ensure_ip_stats(ip)
    stats["score"] = max(float(stats["score"]) + delta, -20.0)
    return stats


def record_ip_healthy(ip: str, *, result_count: int | None = None):
    """Refresh health cache for an IP after a probe or successful search request."""
    now = time.time()
    with _lock:
        _health_cache[ip] = (now + HEALTH_CACHE_SECONDS, True)
        stats = _adjust_score(ip, PROBE_SUCCESS_BONUS if result_count is None else min(max(result_count, 1), SEARCH_SUCCESS_BONUS_MAX))
        if result_count is None:
            stats["probe_successes"] = int(stats["probe_successes"]) + 1
        else:
            stats["search_successes"] = int(stats["search_successes"]) + 1
            stats["last_success_at"] = now


def record_ip_empty(ip: str):
    """Penalize an IP that returned a valid response but no usable search results."""
    with _lock:
        stats = _adjust_score(ip, -EMPTY_RESULT_PENALTY)
        stats["empty_results"] = int(stats["empty_results"]) + 1


def mark_ip_unhealthy(ip: str, reason: str = "", retry_after: int = DEAD_IP_RETRY_SECONDS):
    """Temporarily suppress a dead or unreachable IP."""
    retry_after = max(5, int(retry_after))
    with _lock:
        _health_cache[ip] = (time.time() + retry_after, False)
        stats = _adjust_score(ip, -UNHEALTHY_PENALTY)
        stats["failures"] = int(stats["failures"]) + 1
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
    if not config.get_setting("search_ip_rotation_enabled", False):
        return []

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
    """Get best healthy IP using real search outcomes, not blind round-robin."""
    global _index
    available = get_available_ips()
    if not available:
        return None

    with _lock:
        now = time.time()
        for ip in available:
            _ensure_ip_stats(ip)
        ranked = sorted(
            available,
            key=lambda candidate: (
                -(
                    float(_ip_stats[candidate]["score"])
                    - (
                        RECENT_USE_PENALTY
                        if now - float(_ip_stats[candidate]["last_used"]) < RECENT_USE_WINDOW_SECONDS
                        else 0.0
                    )
                ),
                float(_ip_stats[candidate]["last_used"]),
                -float(_ip_stats[candidate]["last_success_at"]),
                int(_ip_stats[candidate]["failures"]),
                int(_ip_stats[candidate]["empty_results"]),
                candidate,
            ),
        )
        start_index = _index % len(ranked)
        best_score = (
            float(_ip_stats[ranked[0]]["score"])
            - (
                RECENT_USE_PENALTY
                if now - float(_ip_stats[ranked[0]]["last_used"]) < RECENT_USE_WINDOW_SECONDS
                else 0.0
            )
        )
        candidates = [
            ip
            for ip in ranked
            if (
                float(_ip_stats[ip]["score"])
                - (
                    RECENT_USE_PENALTY
                    if now - float(_ip_stats[ip]["last_used"]) < RECENT_USE_WINDOW_SECONDS
                    else 0.0
                )
            ) >= best_score - SCORE_TIE_MARGIN
        ]
        ip = candidates[start_index % len(candidates)] if candidates else ranked[0]
        _ensure_ip_stats(ip)["last_used"] = now
        _index += 1

    return ip


def cooldown_ip(ip: str):
    """Put an IP on cooldown after a remote-side block like 429/captcha."""
    with _lock:
        _cooldowns[ip] = time.time() + COOLDOWN_SECONDS
        stats = _adjust_score(ip, -COOLDOWN_PENALTY)
        stats["failures"] = int(stats["failures"]) + 1
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
        unhealthy = []
        for ip, raw_entry in list(_health_cache.items()):
            cached = _parse_health_cache_entry(raw_entry)
            if not cached:
                _health_cache.pop(ip, None)
                continue
            expires_at, healthy = cached
            if not healthy and expires_at >= now:
                unhealthy.append(ip)
        ranked_ips = sorted(
            (
                {
                    "ip": ip,
                    "score": round(float(_ensure_ip_stats(ip)["score"]), 2),
                    "last_used": float(_ensure_ip_stats(ip)["last_used"]),
                    "last_success_at": float(_ensure_ip_stats(ip)["last_success_at"]),
                    "search_successes": int(_ensure_ip_stats(ip)["search_successes"]),
                    "empty_results": int(_ensure_ip_stats(ip)["empty_results"]),
                    "failures": int(_ensure_ip_stats(ip)["failures"]),
                }
                for ip in all_ips
            ),
            key=lambda item: (-item["score"], item["last_used"], item["ip"]),
        )
    return {
        "enabled": bool(config.get_setting("search_ip_rotation_enabled", False)),
        "total_ips": len(all_ips),
        "available_ips": max(len(all_ips) - len(cooled) - len(unhealthy), 0),
        "cooled_down_ips": len(cooled),
        "cooldown_list": cooled,
        "unhealthy_ips": len(unhealthy),
        "unhealthy_list": unhealthy,
        "ranked_ips": ranked_ips,
    }
