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
import httpx

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_index = 0
_cooldowns: dict[str, float] = {}
_engine_cooldowns: dict[tuple[str, str], float] = {}
_engine_fallback_until: dict[str, float] = {}
_engine_failure_events: dict[str, list[float]] = {}
_health_cache: dict[str, tuple[float, bool]] = {}
_ip_stats: dict[str, dict[str, float | int]] = {}
_engine_stats: dict[str, dict[str, dict[str, float | int]]] = {}
_empty_streaks: dict[tuple[str, str], int] = {}

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
EMPTY_STREAK_COOLDOWN_AFTER = 2
ENGINE_FAILURE_WINDOW_SECONDS = 120
ENGINE_FAILURE_THRESHOLD = 6
ENGINE_FALLBACK_SECONDS = 300
HEALTHCHECK_HOSTS = (
    "www.bing.com",
    "html.duckduckgo.com",
)
VALIDATION_QUERY = "plumber los angeles contact"
VALIDATION_TIMEOUT_SECONDS = 12.0


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


def _ip_matches_family_mode(ip: str, mode: str | None = None) -> bool:
    raw_mode = mode or config.get_setting("search_ip_family_mode", "both") or "both"
    mode = raw_mode.strip().lower() if isinstance(raw_mode, str) else "both"
    if mode in ("", "both", "all"):
        return True
    family = _ip_family(ip)
    if family is None:
        return False
    if mode == "ipv4":
        return family == socket.AF_INET
    if mode == "ipv6":
        return family == socket.AF_INET6
    return True


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
    return _ip_stats.setdefault(ip, _new_stats())


def _new_stats() -> dict[str, float | int]:
    return {
        "score": 0.0,
        "last_used": 0.0,
        "last_success_at": 0.0,
        "probe_successes": 0,
        "search_successes": 0,
        "empty_results": 0,
        "failures": 0,
    }


def _ensure_engine_stats(engine: str, ip: str) -> dict[str, float | int]:
    engine = (engine or "search").strip().lower() or "search"
    stats_by_ip = _engine_stats.setdefault(engine, {})
    return stats_by_ip.setdefault(ip, _new_stats())


def _adjust_score(ip: str, delta: float) -> dict[str, float | int]:
    stats = _ensure_ip_stats(ip)
    stats["score"] = max(float(stats["score"]) + delta, -20.0)
    return stats


def _adjust_engine_score(engine: str, ip: str, delta: float) -> dict[str, float | int]:
    stats = _ensure_engine_stats(engine, ip)
    stats["score"] = max(float(stats["score"]) + delta, -20.0)
    return stats


def _register_engine_failure(engine: str):
    engine = (engine or "search").strip().lower() or "search"
    now = time.time()
    events = [
        ts for ts in _engine_failure_events.get(engine, [])
        if now - ts <= ENGINE_FAILURE_WINDOW_SECONDS
    ]
    events.append(now)
    _engine_failure_events[engine] = events
    if len(events) >= ENGINE_FAILURE_THRESHOLD:
        _engine_fallback_until[engine] = now + ENGINE_FALLBACK_SECONDS
        logger.warning(
            "%s rotation fallback active for %ss after %s recent IP failures",
            engine.upper(),
            ENGINE_FALLBACK_SECONDS,
            len(events),
        )


def record_ip_healthy(ip: str, *, result_count: int | None = None, engine: str | None = None):
    """Refresh health cache for an IP after a probe or successful search request."""
    now = time.time()
    with _lock:
        _health_cache[ip] = (now + HEALTH_CACHE_SECONDS, True)
        delta = PROBE_SUCCESS_BONUS if result_count is None else min(max(result_count, 1), SEARCH_SUCCESS_BONUS_MAX)
        stats = _adjust_score(ip, delta)
        if result_count is None:
            stats["probe_successes"] = int(stats["probe_successes"]) + 1
        else:
            stats["search_successes"] = int(stats["search_successes"]) + 1
            stats["last_success_at"] = now
        engine_name = engine or ("search" if result_count is not None else None)
        engine_name = (engine_name or "").strip().lower() or None
        if engine_name:
            engine_stats = _adjust_engine_score(engine_name, ip, delta)
            if result_count is None:
                engine_stats["probe_successes"] = int(engine_stats["probe_successes"]) + 1
            else:
                engine_stats["search_successes"] = int(engine_stats["search_successes"]) + 1
                engine_stats["last_success_at"] = now
                _empty_streaks.pop(((engine_name or "search").strip().lower() or "search", ip), None)


def record_ip_empty(ip: str, *, engine: str | None = None):
    """Penalize an IP that returned a valid response but no usable search results."""
    with _lock:
        stats = _adjust_score(ip, -EMPTY_RESULT_PENALTY)
        stats["empty_results"] = int(stats["empty_results"]) + 1
        engine_name = (engine or "search").strip().lower() or "search"
        if engine_name:
            engine_stats = _adjust_engine_score(engine_name, ip, -EMPTY_RESULT_PENALTY)
            engine_stats["empty_results"] = int(engine_stats["empty_results"]) + 1
            key = (engine_name, ip)
            _empty_streaks[key] = _empty_streaks.get(key, 0) + 1
            if _empty_streaks[key] >= EMPTY_STREAK_COOLDOWN_AFTER:
                _engine_cooldowns[key] = time.time() + DEAD_IP_RETRY_SECONDS
                _register_engine_failure(engine_name)
                logger.warning(
                    "%s IP %s sidelined for %ss after repeated empty search pages",
                    engine_name.upper(),
                    ip,
                    DEAD_IP_RETRY_SECONDS,
                )


def mark_ip_unhealthy(ip: str, reason: str = "", retry_after: int = DEAD_IP_RETRY_SECONDS, *, engine: str | None = None):
    """Temporarily suppress a dead or unreachable IP."""
    retry_after = max(5, int(retry_after))
    with _lock:
        _health_cache[ip] = (time.time() + retry_after, False)
        stats = _adjust_score(ip, -UNHEALTHY_PENALTY)
        stats["failures"] = int(stats["failures"]) + 1
        if engine:
            engine_name = (engine or "search").strip().lower() or "search"
            _engine_cooldowns[(engine_name, ip)] = time.time() + retry_after
            engine_stats = _adjust_engine_score(engine_name, ip, -UNHEALTHY_PENALTY)
            engine_stats["failures"] = int(engine_stats["failures"]) + 1
            _register_engine_failure(engine_name)
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


def _is_engine_ready(engine: str, ip: str, now: float) -> bool:
    with _lock:
        return _engine_cooldowns.get((engine, ip), 0) < now


def _engine_fallback_active(engine: str, now: float) -> bool:
    with _lock:
        until = _engine_fallback_until.get(engine, 0)
        if until >= now:
            return True
        if until:
            _engine_fallback_until.pop(engine, None)
            _engine_failure_events.pop(engine, None)
        return False


def get_available_ips(engine: str | None = None) -> list[str]:
    """Return healthy configured IPs. Empty list means use default route."""
    if not config.get_setting("search_ip_rotation_enabled", False):
        return []

    engine_name = (engine or "search").strip().lower() or "search"
    now = time.time()
    if engine and _engine_fallback_active(engine_name, now):
        logger.warning("%s rotation fallback active - using default route", engine_name.upper())
        return []

    all_ips = _load_ips()
    all_ips = [ip for ip in all_ips if _ip_matches_family_mode(ip)]
    if not all_ips:
        return []

    ready = [ip for ip in all_ips if _is_ip_ready(ip, now)]
    if engine:
        ready = [ip for ip in ready if _is_engine_ready(engine_name, ip, now)]
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
    return get_next_ip_for_engine("search")


def get_next_ip_for_engine(engine: str) -> str | None:
    """Get best healthy IP for one search engine."""
    global _index
    engine = (engine or "search").strip().lower() or "search"
    available = get_available_ips(engine=engine)
    if not available:
        return None

    with _lock:
        now = time.time()
        for ip in available:
            _ensure_ip_stats(ip)
            _ensure_engine_stats(engine, ip)
        ranked = sorted(
            available,
            key=lambda candidate: (
                -(
                    float(_engine_stats[engine][candidate]["score"])
                    - (
                        RECENT_USE_PENALTY
                        if now - float(_engine_stats[engine][candidate]["last_used"]) < RECENT_USE_WINDOW_SECONDS
                        else 0.0
                    )
                ),
                float(_engine_stats[engine][candidate]["last_used"]),
                -float(_engine_stats[engine][candidate]["last_success_at"]),
                int(_engine_stats[engine][candidate]["failures"]),
                int(_engine_stats[engine][candidate]["empty_results"]),
                candidate,
            ),
        )
        start_index = _index % len(ranked)
        best_score = (
            float(_engine_stats[engine][ranked[0]]["score"])
            - (
                RECENT_USE_PENALTY
                if now - float(_engine_stats[engine][ranked[0]]["last_used"]) < RECENT_USE_WINDOW_SECONDS
                else 0.0
            )
        )
        candidates = [
            ip
            for ip in ranked
            if (
                float(_engine_stats[engine][ip]["score"])
                - (
                    RECENT_USE_PENALTY
                    if now - float(_engine_stats[engine][ip]["last_used"]) < RECENT_USE_WINDOW_SECONDS
                    else 0.0
                )
            ) >= best_score - SCORE_TIE_MARGIN
        ]
        ip = candidates[start_index % len(candidates)] if candidates else ranked[0]
        _ensure_ip_stats(ip)["last_used"] = now
        _ensure_engine_stats(engine, ip)["last_used"] = now
        _index += 1

    return ip


def cooldown_ip(ip: str, *, engine: str | None = None):
    """Put an IP on cooldown after a remote-side block like 429/captcha."""
    with _lock:
        if engine:
            engine_name = (engine or "search").strip().lower() or "search"
            _engine_cooldowns[(engine_name, ip)] = time.time() + COOLDOWN_SECONDS
            _register_engine_failure(engine_name)
        else:
            _cooldowns[ip] = time.time() + COOLDOWN_SECONDS
        stats = _adjust_score(ip, -COOLDOWN_PENALTY)
        stats["failures"] = int(stats["failures"]) + 1
        if engine:
            engine_stats = _adjust_engine_score(engine_name, ip, -COOLDOWN_PENALTY)
            engine_stats["failures"] = int(engine_stats["failures"]) + 1
    if engine:
        logger.warning("%s IP %s on cooldown for %ss", engine_name.upper(), ip, COOLDOWN_SECONDS)
    else:
        logger.warning("IP %s on cooldown for %ss", ip, COOLDOWN_SECONDS)


def get_ip_count() -> int:
    """Total configured IPs."""
    return len([ip for ip in _load_ips() if _ip_matches_family_mode(ip)])


def _validate_bing_ip(ip: str) -> dict:
    from search.scraper import BING_SEARCH_URL, _filter_urls, _is_captcha_response, _parse_bing_results

    params = {
        "q": VALIDATION_QUERY,
        "count": "10",
        "setlang": "en",
        "mkt": "en-US",
        "cc": "US",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.bing.com/",
    }
    try:
        transport = httpx.HTTPTransport(local_address=ip)
        with httpx.Client(
            transport=transport,
            timeout=httpx.Timeout(VALIDATION_TIMEOUT_SECONDS),
            headers=headers,
            follow_redirects=True,
            http2=False,
            verify=config.tls_verify(),
        ) as client:
            resp = client.get(BING_SEARCH_URL, params=params)
        raw_count = 0
        filtered_count = 0
        captcha = False
        if resp.status_code == 200:
            captcha = _is_captcha_response(resp.text)
            raw_urls = _parse_bing_results(resp.text)
            raw_count = len(raw_urls)
            filtered_count = len(_filter_urls(raw_urls))
        return {
            "ok": resp.status_code == 200 and filtered_count > 0 and not captcha,
            "status_code": resp.status_code,
            "raw_results": raw_count,
            "results": filtered_count,
            "blocked": resp.status_code in (403, 429) or captcha,
            "error": None,
        }
    except Exception as exc:
        return {
            "ok": False,
            "status_code": None,
            "raw_results": 0,
            "results": 0,
            "blocked": False,
            "error": str(exc),
        }


def _validate_ddg_ip(ip: str) -> dict:
    from search.duckduckgo import DDG_URL, _filter_ddg_urls, _parse_ddg_results

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://duckduckgo.com/",
    }
    try:
        transport = httpx.HTTPTransport(local_address=ip)
        with httpx.Client(
            transport=transport,
            timeout=httpx.Timeout(VALIDATION_TIMEOUT_SECONDS),
            headers=headers,
            follow_redirects=True,
            verify=config.tls_verify(),
        ) as client:
            resp = client.post(DDG_URL, data={"q": VALIDATION_QUERY, "b": ""})
        raw_count = 0
        filtered_count = 0
        if resp.status_code == 200:
            raw_urls = _parse_ddg_results(resp.text)
            raw_count = len(raw_urls)
            filtered_count = len(_filter_ddg_urls(raw_urls))
        return {
            "ok": resp.status_code == 200 and filtered_count > 0,
            "status_code": resp.status_code,
            "raw_results": raw_count,
            "results": filtered_count,
            "blocked": resp.status_code in (403, 429),
            "error": None,
        }
    except Exception as exc:
        return {
            "ok": False,
            "status_code": None,
            "raw_results": 0,
            "results": 0,
            "blocked": False,
            "error": str(exc),
        }


def validate_ip_for_search(ip: str) -> dict:
    try:
        normalized_ip = ipaddress.ip_address(str(ip).strip()).compressed
    except ValueError:
        return {"ip": ip, "family": "invalid", "bing": {"ok": False, "error": "invalid IP"}, "ddg": {"ok": False, "error": "invalid IP"}}

    family = "ipv6" if ipaddress.ip_address(normalized_ip).version == 6 else "ipv4"
    bing = _validate_bing_ip(normalized_ip)
    ddg = _validate_ddg_ip(normalized_ip)
    if bing["ok"]:
        record_ip_healthy(normalized_ip, result_count=bing["results"], engine="bing")
    else:
        record_ip_empty(normalized_ip, engine="bing")
    if ddg["ok"]:
        record_ip_healthy(normalized_ip, result_count=ddg["results"], engine="ddg")
    else:
        record_ip_empty(normalized_ip, engine="ddg")
    return {
        "ip": normalized_ip,
        "family": family,
        "ok": bool(bing["ok"] or ddg["ok"]),
        "bing": bing,
        "ddg": ddg,
    }


def validate_rotation_pool(limit: int | None = None) -> dict:
    ips = [ip for ip in _load_ips() if _ip_matches_family_mode(ip)]
    if limit:
        ips = ips[:max(0, int(limit))]
    results = [validate_ip_for_search(ip) for ip in ips]
    return {
        "query": VALIDATION_QUERY,
        "family_mode": config.get_setting("search_ip_family_mode", "both"),
        "total": len(results),
        "usable": sum(1 for item in results if item.get("ok")),
        "bing_usable": sum(1 for item in results if item.get("bing", {}).get("ok")),
        "ddg_usable": sum(1 for item in results if item.get("ddg", {}).get("ok")),
        "results": results,
    }


def get_status() -> dict:
    """Status summary for debugging."""
    all_ips = [ip for ip in _load_ips() if _ip_matches_family_mode(ip)]
    now = time.time()
    with _lock:
        cooled = [ip for ip in all_ips if _cooldowns.get(ip, 0) >= now]
        engine_fallbacks = {
            engine: round(until - now)
            for engine, until in _engine_fallback_until.items()
            if until >= now
        }
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
        ranked_by_engine = {}
        for engine, stats_by_ip in _engine_stats.items():
            ranked_by_engine[engine] = sorted(
                (
                    {
                        "ip": ip,
                        "score": round(float(stats["score"]), 2),
                        "last_used": float(stats["last_used"]),
                        "last_success_at": float(stats["last_success_at"]),
                        "search_successes": int(stats["search_successes"]),
                        "empty_results": int(stats["empty_results"]),
                        "failures": int(stats["failures"]),
                        "cooldown": max(round(_engine_cooldowns.get((engine, ip), 0) - now), 0),
                    }
                    for ip, stats in stats_by_ip.items()
                    if ip in all_ips
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
        "engine_fallbacks": engine_fallbacks,
        "ranked_by_engine": ranked_by_engine,
    }
