"""
Campaign execution logic — runs in background thread.
V2: Uses Bing scraper for URL generation instead of AI.
Parallel Bing scraping + async crawling.
"""
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import tldextract
import database
import config
import tasks
from search.scraper import generate_urls_report
from crawler.fetcher import crawl_urls
from crawler.extractor import extract_emails

logger = logging.getLogger(__name__)


def _generate_for_combo(combo, urls_per_batch):
    """Worker: generate URLs for a single (niche, city, country, tld) combo."""
    niche, city, country, country_tld = combo
    try:
        report = generate_urls_report(niche, city, country, country_tld, count=urls_per_batch)
        rows = []
        for url, source in report["tagged_urls"]:
            ext = tldextract.extract(url)
            domain = f"{ext.domain}.{ext.suffix}"
            rows.append({
                "url": url,
                "domain": domain,
                "niche": niche,
                "city": city,
                "country": country,
                "source": source,
            })
        return {
            "rows": rows,
            "report": report,
        }
    except Exception as e:
        logger.error(f"URL generation failed for {niche}/{city}/{country}: {e}")
        return {
            "rows": [],
            "report": {
                "sources": {"bing": 0, "ddg": 0, "ai": 0},
                "ai": {
                    "status": "error",
                    "requested_model": config.get_setting("openrouter_model", ""),
                    "actual_model": None,
                    "error": str(e),
                },
            },
        }


async def run_campaign(task_id: str, campaign_id: int):
    """Full campaign pipeline: scrape Bing for URLs → crawl → extract emails."""
    campaign = database.get_campaign(campaign_id)
    if not campaign:
        tasks.fail_task(task_id, "Campaign not found")
        return

    try:
        await _run_campaign_steps(task_id, campaign_id, campaign)
    except Exception:
        logger.exception("Campaign %s failed", campaign_id)
        database.update_campaign_status(campaign_id, "failed")
        database.update_campaign_counts(campaign_id)
        raise


async def _run_campaign_steps(task_id: str, campaign_id: int, campaign: dict):
    locations = config.get_locations()
    niches = campaign["niches"]
    countries = campaign["countries"]
    cities = campaign["cities"]
    urls_per_batch = int(config.get_setting("urls_per_batch", config.URLS_PER_BATCH))

    database.update_campaign_status(campaign_id, "generating")
    tasks.update_task(task_id, message="Scraping Bing for URLs...")

    combos = []
    for country in countries:
        country_data = locations.get(country, {})
        country_tld = country_data.get("tld", ".com")
        country_cities = country_data.get("cities", [])

        if "*" in cities:
            target_cities = country_cities[:20]
        else:
            target_cities = [c for c in cities if c in country_cities]
            if not target_cities:
                target_cities = cities

        for niche in niches:
            for city in target_cities:
                combos.append((niche, city, country, country_tld))

    total_combos = len(combos)
    bing_concurrency = int(config.get_setting("bing_concurrency", config.BING_CONCURRENCY))
    tasks.update_task(
        task_id,
        total=total_combos,
        message=f"Scraping Bing for {total_combos} combinations ({bing_concurrency} parallel)...",
    )

    all_url_rows = []
    url_generation_reports = []
    completed = 0
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=bing_concurrency) as executor:
        futures = {
            executor.submit(_generate_for_combo, combo, urls_per_batch): combo
            for combo in combos
        }
        for future in as_completed(futures):
            combo = futures[future]
            result = future.result()
            rows = result["rows"]
            url_generation_reports.append(result["report"])
            for row in rows:
                row["campaign_id"] = campaign_id
            all_url_rows.extend(rows)

            with lock:
                completed += 1
                niche, city, country, _ = combo
                tasks.update_task(
                    task_id,
                    progress=completed,
                    message=f"[{completed}/{total_combos}] Scraped: {niche} in {city}, {country}",
                )

    # Cross-campaign domain dedup: remove URLs already crawled elsewhere
    deduped_count = 0
    if all_url_rows:
        existing_domains = database.get_existing_domains(exclude_campaign_id=campaign_id)
        before = len(all_url_rows)
        all_url_rows = [r for r in all_url_rows if r["domain"] not in existing_domains]
        deduped_count = before - len(all_url_rows)
        if deduped_count:
            logger.info(f"Cross-campaign dedup: removed {deduped_count} duplicate domains")
            tasks.update_task(task_id, message=f"Removed {deduped_count} duplicate domains from other campaigns")

    if all_url_rows:
        database.insert_urls(all_url_rows)
    database.update_campaign_counts(campaign_id)

    database.update_campaign_status(campaign_id, "crawling")
    pending_urls = database.get_urls(campaign_id, status="pending")
    tasks.update_task(
        task_id,
        progress=0,
        total=len(pending_urls),
        message=f"Crawling {len(pending_urls)} URLs...",
    )

    def on_crawl_progress(done, total):
        tasks.update_task(task_id, progress=done, total=total,
                          message=f"Crawled {done}/{total} domains")

    crawl_results, crawl_stats = await crawl_urls(pending_urls, on_progress=on_crawl_progress)

    tasks.update_task(task_id, message="Extracting emails...")
    total_extracted = 0
    domains_with_emails = 0
    domains_without_emails = 0

    for url_record in pending_urls:
        url_id = url_record["id"]
        pages = crawl_results.get(url_id, [])

        if pages:
            database.update_url_status(url_id, "crawled")
            email_rows = []
            for page_url, html in pages:
                extracted = extract_emails(html, page_url)
                for em in extracted:
                    em["campaign_id"] = campaign_id
                    em["niche"] = url_record["niche"]
                    em["city"] = url_record["city"]
                    em["country"] = url_record["country"]
                    email_rows.append(em)

            if email_rows:
                database.insert_emails_bulk(email_rows)
                total_extracted += len(email_rows)
                domains_with_emails += 1
            else:
                domains_without_emails += 1
        else:
            database.update_url_status(url_id, "failed", error="No pages fetched")
            domains_without_emails += 1

    # Save crawl stats to campaign
    crawl_stats["domains_with_emails"] = domains_with_emails
    crawl_stats["domains_without_emails"] = domains_without_emails
    crawl_stats["total_emails_extracted"] = total_extracted
    if crawl_stats["domains_reachable"] > 0:
        crawl_stats["emails_per_domain"] = round(total_extracted / crawl_stats["domains_reachable"], 2)
    else:
        crawl_stats["emails_per_domain"] = 0
    crawl_stats["deduped_domains"] = deduped_count
    crawl_stats["url_generation"] = _build_url_generation_summary(url_generation_reports, all_url_rows)

    database.save_campaign_stats(campaign_id, crawl_stats)
    database.update_campaign_counts(campaign_id)
    database.update_campaign_status(campaign_id, "done")

    # Build detailed completion message
    msg = (
        f"Done! {total_extracted} emails from {len(pending_urls)} URLs. "
        f"Reachable: {crawl_stats['domains_reachable']}/{crawl_stats['domains_total']} | "
        f"Pages: {crawl_stats['pages_fetched']} fetched, {crawl_stats['pages_failed']} failed | "
        f"Domains with emails: {domains_with_emails}"
    )
    if crawl_stats['pages_robots_blocked'] > 0:
        msg += f" | robots.txt blocked: {crawl_stats['pages_robots_blocked']}"

    tasks.complete_task(task_id, msg)


def _build_url_generation_summary(reports: list[dict], rows: list[dict]) -> dict:
    source_counts = {"bing": 0, "ddg": 0, "ai": 0}
    for row in rows:
        source = row.get("source")
        if source in source_counts:
            source_counts[source] += 1

    ai_statuses = []
    requested_models = []
    actual_models = []
    ai_errors = []
    for report in reports:
        ai_report = report.get("ai", {})
        status = ai_report.get("status")
        requested_model = ai_report.get("requested_model")
        actual_model = ai_report.get("actual_model")
        error = ai_report.get("error")
        if status:
            ai_statuses.append(status)
        if requested_model and requested_model not in requested_models:
            requested_models.append(requested_model)
        if actual_model and actual_model not in actual_models:
            actual_models.append(actual_model)
        if error and error not in ai_errors:
            ai_errors.append(error)

    overall_ai_status = "disabled"
    if "error" in ai_statuses:
        overall_ai_status = "partial" if source_counts["ai"] > 0 else "error"
    elif "ok" in ai_statuses:
        overall_ai_status = "ok"

    return {
        "sources": source_counts,
        "total_urls_after_dedup": len(rows),
        "ai": {
            "status": overall_ai_status,
            "requested_models": requested_models,
            "actual_models": actual_models,
            "error": ai_errors[0] if ai_errors else None,
        },
    }
