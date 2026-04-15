"""
DuckDuckGo HTML scraper — secondary URL source.
DDG doesn't geo-target by IP, so European VPS IPs get proper results.
Uses DDG's HTML lite endpoint (no JS, no captcha, no rate-limit drama).
"""
import asyncio
import logging
import random
import re

import httpx
import tldextract
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

logger = logging.getLogger(__name__)
ua = UserAgent(fallback="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

DDG_URL = "https://html.duckduckgo.com/html/"

# Same skip list as Bing scraper
SKIP_DOMAINS = {
    "facebook.com", "instagram.com", "linkedin.com", "twitter.com",
    "x.com", "youtube.com", "tiktok.com", "yelp.com", "yellowpages.com",
    "google.com", "bing.com", "wikipedia.org", "reddit.com", "pinterest.com",
    "tripadvisor.com", "bbb.org", "trustpilot.com", "glassdoor.com",
    "craigslist.org", "amazon.com", "ebay.com", "etsy.com",
    "duckduckgo.com",
}

GOV_PATTERNS = re.compile(r"\.(gov|gov\.\w+|mil|edu)$", re.IGNORECASE)


def _parse_ddg_results(html: str) -> list[str]:
    """Parse URLs from DuckDuckGo HTML lite results."""
    urls = []
    soup = BeautifulSoup(html, "lxml")

    # DDG HTML lite: results are in <a class="result__url"> or <a class="result__a">
    for link in soup.select("a.result__a"):
        href = link.get("href", "")
        if href.startswith("http") and "duckduckgo.com" not in href:
            urls.append(href)

    # Fallback: result__url spans contain display URLs
    if not urls:
        for span in soup.select("a.result__url"):
            text = span.get_text(strip=True)
            if text and "." in text:
                if not text.startswith("http"):
                    text = "https://" + text
                urls.append(text)

    return urls


def _filter_ddg_urls(raw_urls: list[str]) -> list[str]:
    """Filter and deduplicate URLs."""
    seen_domains = set()
    valid = []

    for url in raw_urls:
        url = url.rstrip(".,;:)")
        ext = tldextract.extract(url)
        if not ext.domain or not ext.suffix:
            continue

        registered = f"{ext.domain}.{ext.suffix}"
        if registered in SKIP_DOMAINS:
            continue

        fqdn = f"{ext.domain}.{ext.suffix}" if not ext.subdomain else f"{ext.subdomain}.{ext.domain}.{ext.suffix}"
        if GOV_PATTERNS.search(fqdn):
            continue

        if registered in seen_domains:
            continue
        seen_domains.add(registered)

        if not url.startswith("http"):
            url = "https://" + url
        valid.append(url)

    return valid


async def _scrape_ddg_page(query: str) -> list[str]:
    """Scrape a single DDG HTML lite page. Returns list of URLs."""
    headers = {
        "User-Agent": ua.random,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://duckduckgo.com/",
    }

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(15.0),
            headers=headers,
            follow_redirects=True,
            verify=False,
        ) as client:
            # DDG HTML lite uses POST for searches
            resp = await client.post(DDG_URL, data={"q": query, "b": ""})

            if resp.status_code != 200:
                logger.warning(f"DDG returned {resp.status_code} for: {query[:60]}")
                return []

            urls = _parse_ddg_results(resp.text)
            logger.debug(f"DDG: {len(urls)} URLs for: {query[:50]}")
            return urls

    except Exception as e:
        logger.error(f"DDG scrape error: {e}")
        return []
    finally:
        import config
        delay_min = float(config.get_setting("ddg_delay_min", 1.0))
        delay_max = float(config.get_setting("ddg_delay_max", 3.0))
        await asyncio.sleep(random.uniform(delay_min, delay_max))


async def scrape_ddg(niche: str, city: str, country: str, count: int = 40) -> list[str]:
    """
    Scrape DuckDuckGo for business URLs.
    Returns filtered, deduplicated list of URLs.
    """
    queries = [
        f'{niche} in {city} {country}',
        f'{niche} {city} contact email',
        f'best {niche} {city} {country}',
        f'{niche} companies {city}',
        f'{niche} services {city} {country}',
        f'{niche} near {city}',
        f'{niche} {city} website',
        f'top {niche} {city}',
        f'"{niche}" "{city}" contact',
        f'{niche} agency {city} {country}',
    ]

    all_urls = []
    for i, q in enumerate(queries):
        logger.info(f"DDG query [{i+1}/{len(queries)}]: {q}")
        page_urls = await _scrape_ddg_page(q)
        all_urls.extend(page_urls)

        filtered = _filter_ddg_urls(all_urls)
        logger.info(f"  -> {len(page_urls)} raw, {len(filtered)} unique domains total")

        if len(filtered) >= count:
            break

    result = _filter_ddg_urls(all_urls)[:count]
    logger.info(f"DDG scraper returned {len(result)} URLs for {niche} in {city}, {country}")
    return result
