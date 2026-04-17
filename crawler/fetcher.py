"""
Async HTTP fetcher — crawls domains and their sub-pages.
Truly parallel with asyncio.gather + semaphore throttling.
Smart sub-page discovery from homepage links.
Soft robots.txt mode: only blocks admin/private paths, never contact pages.
"""
import asyncio
import logging
import re
from urllib.parse import urljoin, urlparse
import httpx
from fake_useragent import UserAgent
import config

logger = logging.getLogger(__name__)
ua = UserAgent(fallback="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

# Patterns that indicate contact/about/team pages in any language
_DISCOVERY_PATTERNS = re.compile(
    r'(?i)(contact|about|team|staff|people|support|impressum|legal|'
    r'kontakt|equipe|equipo|sobre|chi-siamo|ueber-uns|über-uns|'
    r'qui-sommes-nous|notre-equipe|nuestro-equipo|our-team|meet-the-team|'
    r'get-in-touch|reach-us|write-to-us|email-us)',
)

# Paths we ALWAYS respect robots.txt for (truly private/admin areas)
_PRIVATE_PATH_PATTERNS = re.compile(
    r'(?i)(^/admin|^/wp-admin|^/cgi-bin|^/private|^/\.env|^/\.git|'
    r'^/phpmyadmin|^/cpanel|^/webmail|^/server-status|^/server-info|'
    r'^/api/|^/xmlrpc|^/_debug|^/debug)',
)

# ── Crawl Statistics ─────────────────────────────────────────────────
# Thread-safe stats accumulator for campaign-level monitoring
_crawl_stats_lock = asyncio.Lock()


def _new_crawl_stats() -> dict:
    return {
        "domains_total": 0,
        "domains_reachable": 0,
        "domains_unreachable": 0,
        "pages_fetched": 0,
        "pages_failed": 0,
        "pages_robots_blocked": 0,
        "pages_discovered": 0,
        "domains_with_emails": 0,
        "domains_without_emails": 0,
    }


_RETRYABLE_STATUS = {502, 503, 504}


async def fetch_page(client: httpx.AsyncClient, url: str) -> tuple[str, str | None, int | None]:
    """
    Fetch a single page with retry on transient failures.
    Retries: 3 attempts, exponential backoff (1s, 3s), on connect errors / timeouts / 5xx.
    Does not retry 4xx — terminal.
    Returns (url, html_or_none, status_code_or_none).
    """
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            resp = await client.get(url, follow_redirects=True)
            if resp.status_code in _RETRYABLE_STATUS and attempt < max_attempts:
                backoff = 3 ** (attempt - 1)
                logger.warning(f"Retry {attempt}/{max_attempts} for {url}: HTTP {resp.status_code}")
                await asyncio.sleep(backoff)
                continue
            content_type = resp.headers.get("content-type", "")
            if resp.status_code == 200 and "text/html" in content_type:
                return url, resp.text, resp.status_code
            return url, None, resp.status_code
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout) as e:
            if attempt < max_attempts:
                backoff = 3 ** (attempt - 1)
                logger.warning(f"Retry {attempt}/{max_attempts} for {url}: {type(e).__name__}")
                await asyncio.sleep(backoff)
                continue
            logger.debug(f"Failed to fetch {url} after {max_attempts} attempts: {e}")
            return url, None, None
        except (httpx.HTTPError, Exception) as e:
            logger.debug(f"Failed to fetch {url}: {e}")
            return url, None, None
    return url, None, None


async def _fetch_robots_txt(client: httpx.AsyncClient, base_url: str) -> set[str]:
    """Fetch robots.txt and return set of disallowed paths for * user-agent."""
    disallowed = set()
    try:
        resp = await client.get(base_url + "/robots.txt", follow_redirects=True)
        if resp.status_code != 200:
            return disallowed
        applies = False
        for line in resp.text.splitlines():
            line = line.strip()
            if line.lower().startswith("user-agent:"):
                agent = line.split(":", 1)[1].strip()
                applies = agent == "*"
            elif applies and line.lower().startswith("disallow:"):
                path = line.split(":", 1)[1].strip()
                if path:
                    disallowed.add(path)
    except Exception:
        pass
    return disallowed


def _should_block_path(path: str, disallowed: set[str], robots_mode: str) -> bool:
    """
    Decide whether to block a path based on robots.txt rules.

    Modes:
    - "off":   ignore robots.txt entirely (crawl everything)
    - "soft":  only block truly private/admin paths (default, best for email extraction)
    - "strict": block everything robots.txt says (reduces email yield significantly)
    """
    if robots_mode == "off":
        return False

    # Check if path is actually disallowed by robots.txt
    is_disallowed = False
    for rule in disallowed:
        if rule.endswith("*"):
            if path.startswith(rule[:-1]):
                is_disallowed = True
                break
        elif path == rule or path.startswith(rule.rstrip("/") + "/"):
            is_disallowed = True
            break

    if not is_disallowed:
        return False

    if robots_mode == "strict":
        return True

    # "soft" mode: only block if it's a truly private/admin path
    return bool(_PRIVATE_PATH_PATTERNS.search(path))


def _discover_sub_pages(html: str, base_url: str, domain: str) -> list[str]:
    """Parse homepage HTML to find links to contact/about/team pages."""
    discovered = []
    seen = set()
    for match in re.finditer(r'href=["\']([^"\']+)["\']', html):
        href = match.group(1)
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        if parsed.hostname and domain not in parsed.hostname:
            continue
        if parsed.scheme not in ("http", "https", ""):
            continue
        path = parsed.path.rstrip("/").lower()
        if not path or path == "/" or path in seen:
            continue
        seen.add(path)
        if _DISCOVERY_PATTERNS.search(path):
            discovered.append(full_url)
    return discovered


async def fetch_domain_pages(base_url: str, semaphore: asyncio.Semaphore,
                              domain_stats: dict | None = None) -> list[tuple[str, str]]:
    """
    Fetch a domain's main page + discovered sub-pages.
    Uses soft robots.txt mode by default.
    Returns list of (url, html) for successful pages.
    """
    results = []
    base_url = base_url.rstrip("/")
    timeout = float(config.get_setting("request_timeout", config.REQUEST_TIMEOUT))
    delay = float(config.get_setting("crawl_delay", config.CRAWL_DELAY))
    max_pages = int(config.get_setting("max_pages_per_domain", config.MAX_PAGES_PER_DOMAIN))
    robots_mode = config.get_setting("robots_txt_mode", "soft")  # off | soft | strict

    parsed = urlparse(base_url)
    domain = parsed.hostname or ""

    local_stats = {"pages_fetched": 0, "pages_failed": 0, "pages_blocked": 0, "pages_discovered": 0}

    async with semaphore:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            headers={"User-Agent": ua.random},
            http2=True,
            verify=config.tls_verify(),
        ) as client:
            # 1. Check robots.txt (even in soft mode, we parse it to block admin paths)
            disallowed = set()
            if robots_mode != "off":
                disallowed = await _fetch_robots_txt(client, base_url)

            # 2. Fetch homepage first
            visited = set()
            homepage_url, homepage_html, _ = await fetch_page(client, base_url)
            visited.add("/")
            if homepage_html:
                results.append((homepage_url, homepage_html))
                local_stats["pages_fetched"] += 1
            else:
                local_stats["pages_failed"] += 1
            await asyncio.sleep(delay)

            # 3. Build sub-page list: common paths + discovered from homepage
            sub_urls = []
            for path in config.COMMON_PATHS[1:]:
                full = base_url + path
                sub_urls.append((path, full))

            # Add discovered pages from homepage links
            if homepage_html:
                discovered = _discover_sub_pages(homepage_html, base_url, domain)
                local_stats["pages_discovered"] = len(discovered)
                for discovered_url in discovered:
                    path = urlparse(discovered_url).path
                    if path not in visited:
                        sub_urls.append((path, discovered_url))

            # 4. Crawl sub-pages up to max_pages limit
            for path, url in sub_urls:
                if len(results) >= max_pages:
                    break
                norm_path = path.rstrip("/") or "/"
                if norm_path in visited:
                    continue
                visited.add(norm_path)

                if _should_block_path(path, disallowed, robots_mode):
                    logger.debug(f"Blocked by robots.txt ({robots_mode}): {url}")
                    local_stats["pages_blocked"] += 1
                    continue

                fetched_url, html, status = await fetch_page(client, url)
                if html:
                    results.append((fetched_url, html))
                    local_stats["pages_fetched"] += 1
                else:
                    local_stats["pages_failed"] += 1
                await asyncio.sleep(delay)

    # Accumulate stats if tracker provided
    if domain_stats is not None:
        domain_stats.update(local_stats)
        domain_stats["reachable"] = len(results) > 0

    return results


async def _crawl_single(url_record: dict, semaphore: asyncio.Semaphore,
                         results: dict, counter: dict, total: int,
                         lock: asyncio.Lock, on_progress,
                         all_domain_stats: list) -> None:
    """Crawl a single URL record and store results + stats."""
    url_id = url_record["id"]
    base_url = url_record["url"]
    domain_stats = {}
    try:
        pages = await fetch_domain_pages(base_url, semaphore, domain_stats=domain_stats)
        results[url_id] = pages
    except Exception as e:
        logger.error(f"Error crawling {base_url}: {e}")
        results[url_id] = []
        domain_stats["reachable"] = False

    async with lock:
        all_domain_stats.append(domain_stats)
        counter["done"] += 1
        if on_progress:
            on_progress(counter["done"], total)


async def crawl_urls(urls: list[dict], on_progress=None) -> tuple[dict[int, list[tuple[str, str]]], dict]:
    """
    Crawl all URL records in PARALLEL using asyncio.gather.
    Semaphore controls max concurrent connections.
    Returns (results_dict, crawl_stats).
    """
    max_conns = int(config.get_setting("max_concurrent_requests", config.MAX_CONCURRENT_REQUESTS))
    semaphore = asyncio.Semaphore(max_conns)
    results = {}
    counter = {"done": 0}
    total = len(urls)
    lock = asyncio.Lock()
    all_domain_stats = []

    crawl_tasks = [
        _crawl_single(url_record, semaphore, results, counter, total, lock, on_progress, all_domain_stats)
        for url_record in urls
    ]

    await asyncio.gather(*crawl_tasks)

    # Aggregate stats
    stats = _new_crawl_stats()
    stats["domains_total"] = total
    for ds in all_domain_stats:
        if ds.get("reachable"):
            stats["domains_reachable"] += 1
        else:
            stats["domains_unreachable"] += 1
        stats["pages_fetched"] += ds.get("pages_fetched", 0)
        stats["pages_failed"] += ds.get("pages_failed", 0)
        stats["pages_robots_blocked"] += ds.get("pages_blocked", 0)
        stats["pages_discovered"] += ds.get("pages_discovered", 0)

    return results, stats
