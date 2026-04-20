"""
Campaign execution logic — runs in background thread.
V2: Uses Bing scraper for URL generation instead of AI.
Parallel Bing scraping + async crawling.
"""
import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import tldextract
import database
import config
import tasks
from search.scraper import generate_urls_report
from search.scraper import _normalize_tagged_urls
from search.ai_generator import generate_ai_urls_with_meta
from crawler.fetcher import crawl_urls
from crawler.extractor import extract_emails

logger = logging.getLogger(__name__)
PROGRESS_TOTAL_UNITS = 1000
URL_GENERATION_WEIGHT = 0.35
CRAWL_WEIGHT = 0.45
EXTRACTION_WEIGHT = 0.20


def _run_async_in_thread(coro):
    """Run async helper from worker thread regardless of event-loop state."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _fallback_ai_report(niche: str, city: str, country: str, count: int) -> dict:
    """V1-style safety net: if search scraping yields nothing, ask AI directly."""
    result = _run_async_in_thread(generate_ai_urls_with_meta(niche, city, country, count=count))
    tagged_urls = [(url, "ai") for url in result.get("urls", [])]
    return {
        "tagged_urls": tagged_urls,
        "sources": {
            "bing": 0,
            "ddg": 0,
            "ai": len(tagged_urls),
        },
        "ai": {
            "status": result.get("status", "error"),
            "requested_model": result.get("requested_model"),
            "actual_model": result.get("actual_model"),
            "error": result.get("error"),
        },
    }


def _rows_from_tagged_urls(tagged_urls, niche: str, city: str, country: str) -> list[dict]:
    rows = []
    for url, source in tagged_urls:
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
    return rows


def _top_up_combo_with_ai(report: dict, rows: list[dict], niche: str, city: str, country: str, target_count: int) -> tuple[dict, list[dict]]:
    missing = max(target_count - len(rows), 0)
    if missing <= 0:
        return report, rows

    logger.info(
        "Combo %s/%s/%s below target (%s/%s). Asking AI to fill remaining %s URLs",
        niche, city, country, len(rows), target_count, missing,
    )
    ai_report = _fallback_ai_report(niche, city, country, missing)
    ai_tagged = _normalize_tagged_urls(ai_report.get("tagged_urls", []), fallback_source="ai")
    if not ai_tagged:
        return report, rows

    seen_domains = {row["domain"] for row in rows}
    added = 0
    for row in _rows_from_tagged_urls(ai_tagged, niche, city, country):
        if row["domain"] in seen_domains:
            continue
        seen_domains.add(row["domain"])
        rows.append(row)
        added += 1
        if len(rows) >= target_count:
            break

    if added:
        report_sources = dict(report.get("sources", {}))
        report_sources.setdefault("bing", 0)
        report_sources.setdefault("ddg", 0)
        report_sources["ai"] = report_sources.get("ai", 0) + added
        report["sources"] = report_sources
        report["ai"] = ai_report.get("ai", report.get("ai", {}))
        logger.info(
            "AI top-up added %s URLs for combo %s/%s/%s",
            added, niche, city, country,
        )
    return report, rows


def _generate_for_combo(combo, urls_per_batch):
    """Worker: generate URLs for a single (niche, city, country, tld) combo."""
    niche, city, country, country_tld = combo
    try:
        report = generate_urls_report(niche, city, country, country_tld, count=urls_per_batch)
        tagged_urls = _normalize_tagged_urls(report.get("tagged_urls", []), fallback_source="unknown")
        if not tagged_urls:
            logger.warning(
                "Search URL generation returned 0 rows for %s/%s/%s, falling back to AI-only generator",
                niche, city, country,
            )
            report = _fallback_ai_report(niche, city, country, urls_per_batch)
            tagged_urls = _normalize_tagged_urls(report.get("tagged_urls", []), fallback_source="unknown")

        rows = _rows_from_tagged_urls(tagged_urls, niche, city, country)
        report, rows = _top_up_combo_with_ai(report, rows, niche, city, country, urls_per_batch)
        return {
            "rows": rows,
            "report": report,
        }
    except Exception as e:
        logger.error(f"URL generation failed for {niche}/{city}/{country}: {e}")
        try:
            report = _fallback_ai_report(niche, city, country, urls_per_batch)
            rows = _rows_from_tagged_urls(
                _normalize_tagged_urls(report.get("tagged_urls", []), fallback_source="unknown"),
                niche,
                city,
                country,
            )
            if rows:
                logger.info(
                    "AI-only fallback recovered combo %s/%s/%s with %s URLs",
                    niche, city, country, len(rows),
                )
                return {
                    "rows": rows,
                    "report": report,
                }
        except Exception as fallback_error:
            logger.error(
                "AI-only fallback also failed for %s/%s/%s: %s",
                niche, city, country, fallback_error,
            )
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


class CampaignCancelled(Exception):
    """Raised when user cancels the campaign mid-run."""


def _check_cancel(task_id: str, campaign_id: int):
    if tasks.is_cancelled(task_id):
        database.update_campaign_status(campaign_id, "cancelled")
        database.update_campaign_counts(campaign_id)
        tasks.mark_cancelled(task_id)
        raise CampaignCancelled()


def _overall_progress_units(phase: str, current: int, total: int) -> int:
    total = max(int(total or 0), 0)
    current = max(0, min(int(current or 0), total if total > 0 else 0))
    if phase == "generating":
        base = 0.0
        weight = URL_GENERATION_WEIGHT
    elif phase == "crawling":
        base = URL_GENERATION_WEIGHT
        weight = CRAWL_WEIGHT
    elif phase == "extracting":
        base = URL_GENERATION_WEIGHT + CRAWL_WEIGHT
        weight = EXTRACTION_WEIGHT
    else:
        base = 0.0
        weight = 1.0

    ratio = (current / total) if total > 0 else 0.0
    return min(PROGRESS_TOTAL_UNITS, round((base + (weight * ratio)) * PROGRESS_TOTAL_UNITS))


def _update_campaign_task(task_id: str, phase: str, current: int, total: int, message: str):
    tasks.update_task(
        task_id,
        progress=_overall_progress_units(phase, current, total),
        total=PROGRESS_TOTAL_UNITS,
        message=message,
    )


async def run_campaign(task_id: str, campaign_id: int):
    """Full campaign pipeline: scrape Bing for URLs → crawl → extract emails."""
    campaign = database.get_campaign(campaign_id)
    if not campaign:
        tasks.fail_task(task_id, "Campaign not found")
        return

    try:
        await _run_campaign_steps(task_id, campaign_id, campaign)
    except CampaignCancelled:
        logger.info("Campaign %s cancelled by user", campaign_id)
        return
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
    _update_campaign_task(task_id, "generating", 0, 1, "Generating URLs · Preparing search combinations...")

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
    logger.info(
        "Campaign %s starting URL generation: combos=%s niches=%s countries=%s cities=%s urls_per_batch=%s concurrency=%s",
        campaign_id,
        total_combos,
        len(niches),
        len(countries),
        len(cities),
        urls_per_batch,
        bing_concurrency,
    )
    _update_campaign_task(
        task_id,
        "generating",
        0,
        max(total_combos, 1),
        f"Generating URLs · 0/{total_combos} combinations ({bing_concurrency} parallel)...",
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
            _check_cancel(task_id, campaign_id)
            combo = futures[future]
            result = future.result()
            rows = result["rows"]
            url_generation_reports.append(result["report"])
            report_sources = result["report"].get("sources", {})
            logger.info(
                "Campaign %s combo complete: niche=%s city=%s country=%s urls=%s sources=%s ai_status=%s",
                campaign_id,
                combo[0],
                combo[1],
                combo[2],
                len(rows),
                report_sources,
                result["report"].get("ai", {}).get("status"),
            )
            for row in rows:
                row["campaign_id"] = campaign_id
            all_url_rows.extend(rows)

            with lock:
                completed += 1
                niche, city, country, _ = combo
                _update_campaign_task(
                    task_id,
                    "generating",
                    completed,
                    max(total_combos, 1),
                    f"Generating URLs · {completed}/{total_combos} combinations · {niche} in {city}, {country}",
                )

    generated_count = len(all_url_rows)
    logger.info("Campaign %s URL generation finished: generated_rows=%s", campaign_id, generated_count)

    deduped_count = 0
    if all_url_rows and config.get_setting("dedup_across_campaigns", False):
        existing_domains = database.get_existing_domains(exclude_campaign_id=campaign_id)
        before = len(all_url_rows)
        all_url_rows = [r for r in all_url_rows if r["domain"] not in existing_domains]
        deduped_count = before - len(all_url_rows)
        if deduped_count:
            logger.info(f"Cross-campaign dedup: removed {deduped_count} duplicate domains")
            _update_campaign_task(
                task_id,
                "generating",
                max(total_combos, 1),
                max(total_combos, 1),
                f"Generating URLs · Removed {deduped_count} duplicate domains from other campaigns",
            )

    logger.info(
        "Campaign %s URL queue summary: generated=%s after_dedup=%s deduped=%s",
        campaign_id,
        generated_count,
        len(all_url_rows),
        deduped_count,
    )

    if all_url_rows:
        database.insert_urls(all_url_rows)
        logger.info("Campaign %s inserted URL rows: count=%s", campaign_id, len(all_url_rows))
    else:
        logger.warning("Campaign %s generated zero URL rows before crawl stage", campaign_id)
    database.update_campaign_counts(campaign_id)

    database.update_campaign_status(campaign_id, "crawling")
    pending_urls = database.get_urls(campaign_id, status="pending")
    logger.info("Campaign %s pending crawl queue: count=%s", campaign_id, len(pending_urls))
    _update_campaign_task(
        task_id,
        "crawling",
        0,
        max(len(pending_urls), 1),
        f"Crawling URLs · 0/{len(pending_urls)} domains",
    )

    def on_crawl_progress(done, total):
        _update_campaign_task(
            task_id,
            "crawling",
            done,
            max(total, 1),
            f"Crawling URLs · {done}/{total} domains",
        )

    crawl_results, crawl_stats = await crawl_urls(pending_urls, on_progress=on_crawl_progress)
    logger.info(
        "Campaign %s crawl finished: domains_total=%s reachable=%s pages_fetched=%s pages_failed=%s robots_blocked=%s",
        campaign_id,
        crawl_stats.get("domains_total"),
        crawl_stats.get("domains_reachable"),
        crawl_stats.get("pages_fetched"),
        crawl_stats.get("pages_failed"),
        crawl_stats.get("pages_robots_blocked"),
    )

    _check_cancel(task_id, campaign_id)

    _update_campaign_task(
        task_id,
        "extracting",
        0,
        max(len(pending_urls), 1),
        f"Extracting Emails · 0/{len(pending_urls)} domains",
    )
    total_extracted = 0
    domains_with_emails = 0
    domains_without_emails = 0

    for index, url_record in enumerate(pending_urls, start=1):
        if tasks.is_cancelled(task_id):
            _check_cancel(task_id, campaign_id)
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

        if index == len(pending_urls) or index % 10 == 0:
            _update_campaign_task(
                task_id,
                "extracting",
                index,
                max(len(pending_urls), 1),
                f"Extracting Emails · {index}/{len(pending_urls)} domains · {total_extracted} emails found",
            )

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
    if not pending_urls:
        msg = "Done! No URLs were queued for crawling."
    else:
        msg = (
            f"Done! {total_extracted} emails from {len(pending_urls)} URLs. "
            f"Reachable: {crawl_stats['domains_reachable']}/{crawl_stats['domains_total']} | "
            f"Pages: {crawl_stats['pages_fetched']} fetched, {crawl_stats['pages_failed']} failed | "
            f"Domains with emails: {domains_with_emails}"
        )
    if crawl_stats['pages_robots_blocked'] > 0:
        msg += f" | robots.txt blocked: {crawl_stats['pages_robots_blocked']}"
    if deduped_count:
        msg += f" | skipped duplicates: {deduped_count}"

    logger.info("Campaign %s completed: %s", campaign_id, msg)
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
