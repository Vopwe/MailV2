"""
Bing Scraper — scrapes Bing search results for business URLs.
Drop-in replacement for ai/client.py::generate_urls().
Same function signature, same return type.
"""
import asyncio
import logging
import random
import re
import time

import httpx
import tldextract
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

import config
from search.rotator import get_next_ip, cooldown_ip
from search.queries import build_queries

logger = logging.getLogger(__name__)
ua = UserAgent(fallback="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

# Domains to skip (same list as original ai/client.py)
SKIP_DOMAINS = {
    "facebook.com", "instagram.com", "linkedin.com", "twitter.com",
    "x.com", "youtube.com", "tiktok.com", "yelp.com", "yellowpages.com",
    "google.com", "bing.com", "wikipedia.org", "reddit.com", "pinterest.com",
    "tripadvisor.com", "bbb.org", "trustpilot.com", "glassdoor.com",
    "craigslist.org", "amazon.com", "ebay.com", "etsy.com",
}

# Government TLD patterns
GOV_PATTERNS = re.compile(r"\.(gov|gov\.\w+|mil|edu)$", re.IGNORECASE)

# Captcha / rate-limit detection patterns in response body
CAPTCHA_PATTERNS = [
    "unusual traffic",
    "captcha",
    "blocked",
    "please verify",
    "are you a robot",
    "automated queries",
]

BING_SEARCH_URL = "https://www.bing.com/search"


def _is_captcha_response(html: str) -> bool:
    """Check if Bing returned a captcha/block page."""
    html_lower = html.lower()
    return any(pattern in html_lower for pattern in CAPTCHA_PATTERNS)


def _parse_bing_results(html: str) -> list[str]:
    """Parse organic result URLs from Bing search HTML."""
    urls = []
    soup = BeautifulSoup(html, "lxml")

    # Primary: Bing organic results in <li class="b_algo">
    for result in soup.select("li.b_algo"):
        link = result.select_one("h2 a[href]")
        if link:
            href = link.get("href", "")
            if href.startswith("http"):
                urls.append(href)

    # Fallback: any <a> with cite (URL display) nearby
    if not urls:
        for cite in soup.select("cite"):
            text = cite.get_text(strip=True)
            if text.startswith("http"):
                urls.append(text)
            elif "." in text and not text.startswith("<"):
                urls.append("https://" + text)

    return urls


def _filter_urls(raw_urls: list[str]) -> list[str]:
    """Filter, deduplicate, and normalize URLs."""
    seen_domains = set()
    valid = []

    for url in raw_urls:
        url = url.rstrip(".,;:)")
        ext = tldextract.extract(url)
        if not ext.domain or not ext.suffix:
            continue

        registered = f"{ext.domain}.{ext.suffix}"

        # Skip social / aggregator / search
        if registered in SKIP_DOMAINS:
            continue

        # Skip government
        fqdn = f"{ext.domain}.{ext.suffix}" if not ext.subdomain else f"{ext.subdomain}.{ext.domain}.{ext.suffix}"
        if GOV_PATTERNS.search(fqdn):
            continue

        # Deduplicate by registered domain
        if registered in seen_domains:
            continue
        seen_domains.add(registered)

        # Normalize
        if not url.startswith("http"):
            url = "https://" + url
        valid.append(url)

    return valid


async def _scrape_bing_page(query: str, first: int = 0) -> tuple[list[str], bool]:
    """
    Scrape a single Bing search results page.
    Returns (urls, was_blocked).
    """
    ip = get_next_ip()
    delay_min = float(config.get_setting("bing_delay_min", config.BING_DELAY_MIN))
    delay_max = float(config.get_setting("bing_delay_max", config.BING_DELAY_MAX))
    results_per_page = int(config.get_setting("bing_results_per_page", config.BING_RESULTS_PER_PAGE))

    params = {
        "q": query,
        "count": str(results_per_page),
        "setlang": "en",
    }
    if first > 0:
        params["first"] = str(first)

    headers = {
        "User-Agent": ua.random,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Referer": "https://www.bing.com/",
    }

    # Build transport with source IP binding if available
    transport = None
    if ip:
        try:
            transport = httpx.AsyncHTTPTransport(local_address=ip)
        except Exception as e:
            logger.debug(f"Failed to bind to IP {ip}: {e}")
            transport = None

    try:
        async with httpx.AsyncClient(
            transport=transport,
            timeout=httpx.Timeout(15.0),
            headers=headers,
            follow_redirects=True,
            http2=True,
            verify=False,
        ) as client:
            resp = await client.get(BING_SEARCH_URL, params=params)

            if resp.status_code == 429:
                logger.warning(f"Bing 429 rate limit on IP {ip}")
                if ip:
                    cooldown_ip(ip)
                return [], True

            if resp.status_code != 200:
                logger.warning(f"Bing returned {resp.status_code} for query: {query[:60]}")
                return [], False

            html = resp.text
            if _is_captcha_response(html):
                logger.warning(f"Bing captcha detected on IP {ip}")
                if ip:
                    cooldown_ip(ip)
                return [], True

            urls = _parse_bing_results(html)
            return urls, False

    except Exception as e:
        logger.error(f"Bing scrape error: {e}")
        return [], False
    finally:
        # Random delay between requests
        delay = random.uniform(delay_min, delay_max)
        await asyncio.sleep(delay)


async def _scrape_query(query_str: str, target_count: int) -> list[str]:
    """Scrape multiple Bing pages for a single query until target_count URLs or 3 pages."""
    all_urls = []
    max_pages = 3  # Bing pages 1-3

    for page_num in range(max_pages):
        first = page_num * 10  # Bing uses 'first' offset
        page_urls, was_blocked = await _scrape_bing_page(query_str, first=first)

        if was_blocked:
            logger.info(f"Blocked on page {page_num + 1} for: {query_str[:50]}")
            break

        all_urls.extend(page_urls)

        # Stop if no results on this page (exhausted)
        if not page_urls:
            break

        # Stop if we have enough
        if len(all_urls) >= target_count:
            break

    return all_urls


def generate_urls(niche: str, city: str, country: str, country_tld: str = ".com", count: int = 40) -> list[str]:
    """
    Generate URLs by scraping Bing search results.
    DROP-IN REPLACEMENT for ai/client.py::generate_urls().
    Same signature, same return type.
    """
    queries = build_queries(niche, city, country, country_tld, count)
    all_raw_urls = []

    # Run async scraping in a new event loop (called from sync context)
    async def _run():
        for q_info in queries:
            query_str = q_info["query"]
            target = q_info["results_needed"]
            logger.info(f"Bing scraping: {query_str}")
            urls = await _scrape_query(query_str, target)
            all_raw_urls.extend(urls)

            # Early exit if we have plenty
            if len(_filter_urls(all_raw_urls)) >= count:
                break

    # Handle case where we're already in an event loop
    try:
        loop = asyncio.get_running_loop()
        # We're inside an async context — just run directly
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            loop_new = asyncio.new_event_loop()
            pool.submit(loop_new.run_until_complete, _run()).result()
            loop_new.close()
    except RuntimeError:
        # No event loop running — create one
        asyncio.run(_run())

    filtered = _filter_urls(all_raw_urls)

    # Cap at requested count
    result = filtered[:count]
    logger.info(f"Bing scraper returned {len(result)} URLs for {niche} in {city}, {country}")
    return result
