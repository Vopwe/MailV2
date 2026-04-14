"""
Bing Scraper — scrapes Bing search results for business URLs.
Drop-in replacement for ai/client.py::generate_urls().
Same function signature, same return type.
"""
import asyncio
import base64
import logging
import random
import re
import time
from urllib.parse import urlparse, parse_qs

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

# Captcha / rate-limit detection — phrases that ONLY appear on block pages,
# not in normal Bing search result HTML (which mentions "captcha" in scripts).
CAPTCHA_PATTERNS = [
    "unusual traffic from your computer",
    "are you a robot",
    "automated queries",
    "your request is blocked",
    "please verify you are a human",
    "suspected automated behavior",
]

BING_SEARCH_URL = "https://www.bing.com/search"

# Map country names to Bing market codes.
# This overrides Bing's IP-based geo-detection so European VPS IPs
# get results for the target country, not the server's country.
COUNTRY_TO_BING_MARKET = {
    "United States": ("en-US", "US"),
    "United Kingdom": ("en-GB", "GB"),
    "Canada": ("en-CA", "CA"),
    "Australia": ("en-AU", "AU"),
    "Germany": ("de-DE", "DE"),
    "France": ("fr-FR", "FR"),
    "Spain": ("es-ES", "ES"),
    "Italy": ("it-IT", "IT"),
    "Netherlands": ("nl-NL", "NL"),
    "Belgium": ("nl-BE", "BE"),
    "Switzerland": ("de-CH", "CH"),
    "Austria": ("de-AT", "AT"),
    "Sweden": ("sv-SE", "SE"),
    "Norway": ("nb-NO", "NO"),
    "Denmark": ("da-DK", "DK"),
    "Finland": ("fi-FI", "FI"),
    "Poland": ("pl-PL", "PL"),
    "Portugal": ("pt-PT", "PT"),
    "Brazil": ("pt-BR", "BR"),
    "Mexico": ("es-MX", "MX"),
    "Argentina": ("es-AR", "AR"),
    "Colombia": ("es-CO", "CO"),
    "Chile": ("es-CL", "CL"),
    "India": ("en-IN", "IN"),
    "Japan": ("ja-JP", "JP"),
    "South Korea": ("ko-KR", "KR"),
    "Singapore": ("en-SG", "SG"),
    "Malaysia": ("en-MY", "MY"),
    "Philippines": ("en-PH", "PH"),
    "New Zealand": ("en-NZ", "NZ"),
    "South Africa": ("en-ZA", "ZA"),
    "Ireland": ("en-IE", "IE"),
    "United Arab Emirates": ("en-AE", "AE"),
    "Saudi Arabia": ("ar-SA", "SA"),
    "Turkey": ("tr-TR", "TR"),
    "Indonesia": ("id-ID", "ID"),
    "Thailand": ("th-TH", "TH"),
    "Vietnam": ("vi-VN", "VN"),
    "Czech Republic": ("cs-CZ", "CZ"),
    "Romania": ("ro-RO", "RO"),
    "Hungary": ("hu-HU", "HU"),
    "Greece": ("el-GR", "GR"),
    "Israel": ("he-IL", "IL"),
    "Egypt": ("ar-EG", "EG"),
    "Nigeria": ("en-NG", "NG"),
    "Kenya": ("en-KE", "KE"),
    "Ghana": ("en-GH", "GH"),
    "Pakistan": ("en-PK", "PK"),
    "Bangladesh": ("en-BD", "BD"),
    "China": ("zh-CN", "CN"),
    "Taiwan": ("zh-TW", "TW"),
    "Hong Kong": ("zh-HK", "HK"),
    "Russia": ("ru-RU", "RU"),
    "Ukraine": ("uk-UA", "UA"),
}


def _get_bing_market(country: str) -> tuple[str, str]:
    """Resolve country name to (mkt, cc) for Bing. Falls back to en-US."""
    if country in COUNTRY_TO_BING_MARKET:
        return COUNTRY_TO_BING_MARKET[country]
    # Fuzzy match: check if country name is contained in a key
    country_lower = country.lower()
    for name, codes in COUNTRY_TO_BING_MARKET.items():
        if country_lower in name.lower() or name.lower() in country_lower:
            return codes
    return ("en-US", "US")


def _is_captcha_response(html: str) -> bool:
    """Check if Bing returned a captcha/block page.
    Must match a block-page phrase AND have no organic results."""
    html_lower = html.lower()
    has_block_phrase = any(p in html_lower for p in CAPTCHA_PATTERNS)
    has_organic = "b_algo" in html_lower
    # Only flag as captcha if we see a block phrase and NO organic results
    return has_block_phrase and not has_organic


def _decode_bing_redirect(href: str) -> str | None:
    """Decode Bing tracking URL (bing.com/ck/a?...&u=base64url) to real URL."""
    if "/ck/a?" not in href:
        return href if href.startswith("http") else None

    try:
        parsed = urlparse(href)
        params = parse_qs(parsed.query)
        encoded = params.get("u", [None])[0]
        if not encoded:
            return None
        # Bing uses URL-safe base64 with a prefix character (a1, L2, etc.)
        # Strip the prefix (first 2 chars)
        raw = encoded[2:] if len(encoded) > 2 else encoded
        # Add padding
        padding = 4 - len(raw) % 4
        if padding != 4:
            raw += "=" * padding
        decoded = base64.urlsafe_b64decode(raw).decode("utf-8", errors="ignore")
        if decoded.startswith("http"):
            return decoded
    except Exception:
        pass
    return None


def _parse_bing_results(html: str) -> list[str]:
    """Parse organic result URLs from Bing search HTML."""
    urls = []
    soup = BeautifulSoup(html, "lxml")

    for result in soup.select("li.b_algo"):
        url = None

        # Priority 1: <cite> element — Bing shows "domain › path › page"
        cite = result.select_one("cite")
        if cite:
            cite_text = cite.get_text(strip=True)
            # Bing uses " › " as path separator in display URLs
            if "›" in cite_text:
                parts = [p.strip() for p in cite_text.split("›")]
                # First part is domain (with or without https://)
                domain_part = parts[0]
                path_parts = parts[1:]
                reconstructed = domain_part.rstrip("/")
                if path_parts:
                    reconstructed += "/" + "/".join(path_parts)
                # Remove trailing ellipsis
                reconstructed = reconstructed.rstrip("…").rstrip(".")
                if not reconstructed.startswith("http"):
                    reconstructed = "https://" + reconstructed
                if "." in reconstructed:
                    url = reconstructed
            elif cite_text.startswith("http") and "." in cite_text:
                url = cite_text

        # Priority 2: decode tracking href
        if not url:
            link = result.select_one("h2 a[href]")
            if link:
                href = link.get("href", "")
                url = _decode_bing_redirect(href)

        # Priority 3: generic result anchors inside b_algo
        if not url:
            for link in result.select("a[href]"):
                href = link.get("href", "")
                decoded = _decode_bing_redirect(href)
                if decoded and decoded.startswith("http"):
                    url = decoded
                    break

        if url and url.startswith("http"):
            urls.append(url)

    # Fallback: cite tags outside b_algo
    if not urls:
        for cite in soup.select("cite"):
            text = cite.get_text(strip=True)
            if text.startswith("http"):
                urls.append(text)
            elif "." in text and not text.startswith("<"):
                urls.append("https://" + text)

    # Last-resort fallback: harvest external URLs from raw HTML.
    if not urls:
        for match in re.findall(r'https?://[^"\'>\s]+', html):
            if "bing.com" in match:
                continue
            urls.append(match.rstrip(".,;:)"))

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


async def _scrape_bing_page(query: str, first: int = 0, mkt: str = "en-US", cc: str = "US") -> tuple[list[str], bool]:
    """
    Scrape a single Bing search results page.
    Returns (urls, was_blocked).
    """
    ip = get_next_ip()
    delay_min = float(config.get_setting("bing_delay_min", config.BING_DELAY_MIN))
    delay_max = float(config.get_setting("bing_delay_max", config.BING_DELAY_MAX))

    results_per_page = int(config.get_setting("bing_results_per_page", config.BING_RESULTS_PER_PAGE))
    results_per_page = max(10, min(results_per_page, 50))

    # Bing returns ~10-50 results per page depending on count param.
    # mkt + cc override IP-based geo-detection (critical for European VPS IPs).
    params = {
        "q": query,
        "count": str(results_per_page),
        "setlang": mkt.split("-")[0],  # language part of market code
        "mkt": mkt,
        "cc": cc,
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
            http2=False,
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
            logger.info(f"Bing via IP {ip or 'default'}: {len(urls)} results")
            return urls, False

    except Exception as e:
        logger.error(f"Bing scrape error: {e}")
        return [], False
    finally:
        # Random delay between requests
        delay = random.uniform(delay_min, delay_max)
        await asyncio.sleep(delay)


async def _scrape_query(query_str: str, target_count: int, mkt: str = "en-US", cc: str = "US") -> list[str]:
    """Scrape multiple Bing pages for a single query until target_count URLs or exhausted."""
    all_urls = []
    results_per_page = int(config.get_setting("bing_results_per_page", config.BING_RESULTS_PER_PAGE))
    results_per_page = max(10, min(results_per_page, 50))
    max_pages = max(5, min(12, (target_count // max(results_per_page, 1)) + 3))

    for page_num in range(max_pages):
        first = page_num * results_per_page
        page_urls, was_blocked = await _scrape_bing_page(query_str, first=first, mkt=mkt, cc=cc)

        if was_blocked:
            logger.info(f"Blocked on page {page_num + 1} for: {query_str[:50]}")
            break

        all_urls.extend(page_urls)
        logger.debug(f"Page {page_num + 1}: got {len(page_urls)} URLs (total: {len(all_urls)})")

        # Stop if no results on this page (exhausted)
        if not page_urls:
            break

        # Stop if we have enough
        if len(all_urls) >= target_count:
            break

    return all_urls


def generate_urls(niche: str, city: str, country: str, country_tld: str = ".com", count: int = 40) -> list[tuple[str, str]]:
    """
    Generate URLs by scraping Bing + DuckDuckGo + OpenRouter AI.
    Bing and DDG run in parallel first. If they don't reach the target count,
    AI fills the remaining gap.

    Returns: list of (url, source) tuples where source is 'bing', 'ddg', or 'ai'.
    """
    from search.duckduckgo import scrape_ddg
    from search.ai_generator import generate_ai_urls

    # Resolve country to Bing market code to override IP-based geo-detection
    mkt, cc = _get_bing_market(country)
    logger.info(f"Bing market: {mkt} (cc={cc}) for country: {country}")

    queries = build_queries(niche, city, country, country_tld, count)
    bing_urls = []
    ddg_urls = []
    ai_urls = []

    async def _run():
        # Phase 1: Run Bing and DDG in parallel
        async def _bing_task():
            for i, q_info in enumerate(queries):
                query_str = q_info["query"]
                target = q_info["results_needed"]
                logger.info(f"Bing query [{i+1}/{len(queries)}]: {query_str}")
                urls = await _scrape_query(query_str, target, mkt=mkt, cc=cc)
                bing_urls.extend(urls)

                unique_count = len(_filter_urls(bing_urls))
                logger.info(f"  -> Bing: {len(urls)} raw, {unique_count} unique domains")

                if unique_count >= count:
                    break

        async def _ddg_task():
            result = await scrape_ddg(niche, city, country, count=count)
            ddg_urls.extend(result)

        await asyncio.gather(_bing_task(), _ddg_task())

        # Phase 2: Count what Bing+DDG found, use AI to fill the gap
        seen_domains = set()
        for url in ddg_urls:
            ext = tldextract.extract(url)
            domain = f"{ext.domain}.{ext.suffix}"
            if domain not in SKIP_DOMAINS:
                seen_domains.add(domain)
        for url in _filter_urls(bing_urls):
            ext = tldextract.extract(url)
            domain = f"{ext.domain}.{ext.suffix}"
            seen_domains.add(domain)

        remaining = count - len(seen_domains)
        if remaining > 0:
            logger.info(f"Bing+DDG found {len(seen_domains)} URLs, AI filling {remaining} more")
            result = await generate_ai_urls(niche, city, country, count=remaining)
            ai_urls.extend(result)
        else:
            logger.info(f"Bing+DDG found {len(seen_domains)} URLs — no AI needed")

    # Handle case where we're already in an event loop
    try:
        loop = asyncio.get_running_loop()
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            loop_new = asyncio.new_event_loop()
            pool.submit(loop_new.run_until_complete, _run()).result()
            loop_new.close()
    except RuntimeError:
        asyncio.run(_run())

    # Tag each URL with its source, dedup by domain keeping first occurrence
    tagged = []
    seen_domains = set()

    # DDG first (reliable from EU), then AI, then Bing
    for url in ddg_urls:
        ext = tldextract.extract(url)
        domain = f"{ext.domain}.{ext.suffix}"
        if domain not in seen_domains and domain not in SKIP_DOMAINS:
            seen_domains.add(domain)
            tagged.append((url, "ddg"))

    for url in ai_urls:
        ext = tldextract.extract(url)
        domain = f"{ext.domain}.{ext.suffix}"
        if domain not in seen_domains and domain not in SKIP_DOMAINS:
            seen_domains.add(domain)
            tagged.append((url, "ai"))

    for url in _filter_urls(bing_urls):
        ext = tldextract.extract(url)
        domain = f"{ext.domain}.{ext.suffix}"
        if domain not in seen_domains:
            seen_domains.add(domain)
            tagged.append((url, "bing"))

    result = tagged[:count]
    bing_count = sum(1 for _, s in result if s == "bing")
    ddg_count = sum(1 for _, s in result if s == "ddg")
    ai_count = sum(1 for _, s in result if s == "ai")
    logger.info(
        f"URL generation complete: {len(result)} URLs "
        f"(Bing: {bing_count}, DDG: {ddg_count}, AI: {ai_count}) "
        f"for {niche} in {city}, {country}"
    )
    return result


def generate_urls_report(niche: str, city: str, country: str, country_tld: str = ".com", count: int = 40) -> dict:
    """
    Generate URLs plus AI/source metadata for campaign health reporting.
    """
    from search.duckduckgo import scrape_ddg
    from search.ai_generator import generate_ai_urls_with_meta

    mkt, cc = _get_bing_market(country)
    logger.info(f"Bing market: {mkt} (cc={cc}) for country: {country}")

    queries = build_queries(niche, city, country, country_tld, count)
    bing_urls = []
    ddg_urls = []
    ai_urls = []
    ai_meta = {
        "status": "disabled",
        "requested_model": config.get_setting("openrouter_model", ""),
        "actual_model": None,
        "error": None,
    }

    async def _run():
        async def _bing_task():
            for i, q_info in enumerate(queries):
                query_str = q_info["query"]
                target = q_info["results_needed"]
                logger.info(f"Bing query [{i+1}/{len(queries)}]: {query_str}")
                urls = await _scrape_query(query_str, target, mkt=mkt, cc=cc)
                bing_urls.extend(urls)

                unique_count = len(_filter_urls(bing_urls))
                logger.info(f"  -> Bing: {len(urls)} raw, {unique_count} unique domains")

                if unique_count >= count:
                    break

        async def _ddg_task():
            result = await scrape_ddg(niche, city, country, count=count)
            ddg_urls.extend(result)

        await asyncio.gather(_bing_task(), _ddg_task())

        seen_domains = set()
        for url in ddg_urls:
            ext = tldextract.extract(url)
            domain = f"{ext.domain}.{ext.suffix}"
            if domain not in SKIP_DOMAINS:
                seen_domains.add(domain)
        for url in _filter_urls(bing_urls):
            ext = tldextract.extract(url)
            domain = f"{ext.domain}.{ext.suffix}"
            seen_domains.add(domain)

        remaining = count - len(seen_domains)
        if remaining > 0:
            logger.info(f"Bing+DDG found {len(seen_domains)} URLs, AI filling {remaining} more")
            result = await generate_ai_urls_with_meta(niche, city, country, count=remaining)
            ai_meta.update({
                "status": result["status"],
                "requested_model": result["requested_model"],
                "actual_model": result["actual_model"],
                "error": result["error"],
            })
            ai_urls.extend(result["urls"])
        else:
            ai_meta.update({
                "status": "ok" if config.get_setting("openrouter_api_key", "").strip() else "disabled",
                "requested_model": config.get_setting("openrouter_model", ""),
                "actual_model": None,
                "error": None,
            })
            logger.info(f"Bing+DDG found {len(seen_domains)} URLs — no AI needed")

    try:
        loop = asyncio.get_running_loop()
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            loop_new = asyncio.new_event_loop()
            pool.submit(loop_new.run_until_complete, _run()).result()
            loop_new.close()
    except RuntimeError:
        asyncio.run(_run())

    tagged = []
    seen_domains = set()

    for url in ddg_urls:
        ext = tldextract.extract(url)
        domain = f"{ext.domain}.{ext.suffix}"
        if domain not in seen_domains and domain not in SKIP_DOMAINS:
            seen_domains.add(domain)
            tagged.append((url, "ddg"))

    for url in ai_urls:
        ext = tldextract.extract(url)
        domain = f"{ext.domain}.{ext.suffix}"
        if domain not in seen_domains and domain not in SKIP_DOMAINS:
            seen_domains.add(domain)
            tagged.append((url, "ai"))

    for url in _filter_urls(bing_urls):
        ext = tldextract.extract(url)
        domain = f"{ext.domain}.{ext.suffix}"
        if domain not in seen_domains:
            seen_domains.add(domain)
            tagged.append((url, "bing"))

    result = tagged[:count]
    source_counts = {
        "bing": sum(1 for _, source in result if source == "bing"),
        "ddg": sum(1 for _, source in result if source == "ddg"),
        "ai": sum(1 for _, source in result if source == "ai"),
    }
    logger.info(
        "URL generation report: %s URLs (Bing: %s, DDG: %s, AI: %s) for %s in %s, %s",
        len(result),
        source_counts["bing"],
        source_counts["ddg"],
        source_counts["ai"],
        niche,
        city,
        country,
    )
    return {
        "tagged_urls": result,
        "sources": source_counts,
        "ai": ai_meta,
    }
