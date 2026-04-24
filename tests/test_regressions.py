import asyncio
import csv
import io
import os
import re
import socket
import sqlite3
import subprocess
import shutil
import threading
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch
import uuid

import config
import database
import dns.resolver
import logging_setup
import networking
import tasks
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from licensing import validator as license_validator
from search.ai_generator import generate_ai_urls_with_meta
from search import rotator
from verification import verifier
from web import create_app
from web.routes import _campaign_runner
from web.routes._campaign_runner import run_campaign


@contextmanager
def isolated_db():
    original_path = config.DATABASE_PATH
    database.close_db()
    config.DATABASE_PATH = f"file:test-{uuid.uuid4().hex}?mode=memory&cache=shared"
    keeper = sqlite3.connect(config.DATABASE_PATH, uri=True)
    database.init_db()
    try:
        yield
    finally:
        database.close_db()
        keeper.close()
        config.DATABASE_PATH = original_path


@contextmanager
def isolated_config_paths():
    original_settings_path = config.SETTINGS_PATH
    original_secret_key_path = config.SECRET_KEY_PATH

    temp_dir = os.path.join(config.BASE_DIR, ".test-config", uuid.uuid4().hex)
    os.makedirs(temp_dir, exist_ok=True)
    config.SETTINGS_PATH = os.path.join(temp_dir, "settings.json")
    config.SECRET_KEY_PATH = os.path.join(temp_dir, ".flask_secret_key")
    try:
        yield
    finally:
        config.SETTINGS_PATH = original_settings_path
        config.SECRET_KEY_PATH = original_secret_key_path
        shutil.rmtree(temp_dir, ignore_errors=True)


@contextmanager
def isolated_license_paths():
    original_public_key_path = license_validator.PUBLIC_KEY_PATH
    original_cache = license_validator._cache
    original_public_key = license_validator._public_key
    original_env_license_path = os.environ.get("LICENSE_PATH")

    temp_dir = os.path.join(config.BASE_DIR, ".test-config", uuid.uuid4().hex)
    os.makedirs(temp_dir, exist_ok=True)
    license_validator.PUBLIC_KEY_PATH = Path(temp_dir) / "public_key.pem"
    os.environ["LICENSE_PATH"] = os.path.join(temp_dir, "license.key")
    license_validator.invalidate_cache()
    license_validator._public_key = None
    try:
        yield temp_dir
    finally:
        if original_env_license_path is None:
            os.environ.pop("LICENSE_PATH", None)
        else:
            os.environ["LICENSE_PATH"] = original_env_license_path
        license_validator.PUBLIC_KEY_PATH = original_public_key_path
        license_validator._cache = original_cache
        license_validator._public_key = original_public_key
        shutil.rmtree(temp_dir, ignore_errors=True)


class RegressionTests(unittest.TestCase):
    def setUp(self):
        self._config_context = isolated_config_paths()
        self._config_context.__enter__()
        self._db_context = isolated_db()
        self._db_context.__enter__()
        self._license_context = isolated_license_paths()
        self._license_context.__enter__()
        tasks._tasks.clear()
        os.environ["GM_SKIP_LICENSE"] = "1"

    def tearDown(self):
        os.environ.pop("GM_SKIP_LICENSE", None)
        os.environ.pop("GM_SMTP_EHLO_HOSTNAME", None)
        os.environ.pop("SMTP_EHLO_HOSTNAME", None)
        os.environ.pop("GM_SMTP_MAIL_FROM", None)
        os.environ.pop("SMTP_MAIL_FROM", None)
        os.environ.pop("ADMIN_PASSWORD", None)
        os.environ.pop("GM_ADMIN_PASSWORD_HASH", None)
        tasks._tasks.clear()
        self._license_context.__exit__(None, None, None)
        self._db_context.__exit__(None, None, None)
        self._config_context.__exit__(None, None, None)

    def _write_signing_key_pair(self) -> Path:
        signing_key = Ed25519PrivateKey.generate()
        signing_key_path = Path(os.environ["LICENSE_PATH"]).with_name("signing_key.pem")
        signing_key_path.write_bytes(
            signing_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
        license_validator.PUBLIC_KEY_PATH.write_bytes(
            signing_key.public_key().public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
        )
        return signing_key_path

    def test_get_db_is_thread_local(self):
        main_conn = database.get_db()
        thread_conn_ids = []

        def worker():
            conn = database.get_db()
            thread_conn_ids.append(id(conn))
            database.close_db()

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join()

        self.assertEqual(len(thread_conn_ids), 1)
        self.assertNotEqual(id(main_conn), thread_conn_ids[0])

    def test_get_task_falls_back_to_db_when_memory_cache_misses(self):
        task_id = tasks.create_task(task_type="campaign", campaign_id=42)
        tasks._tasks.clear()

        task = tasks.get_task(task_id)

        self.assertIsNotNone(task)
        self.assertEqual(task.task_id, task_id)
        self.assertEqual(task.campaign_id, 42)

    def test_update_task_persists_latest_progress_for_other_workers(self):
        task_id = tasks.create_task(task_type="campaign", campaign_id=99)
        tasks.update_task(task_id, progress=3, total=10, message="Running step 3")
        tasks._tasks.clear()

        task = tasks.get_task(task_id)

        self.assertIsNotNone(task)
        self.assertEqual(task.progress, 3)
        self.assertEqual(task.total, 10)
        self.assertEqual(task.message, "Running step 3")

    def test_complete_task_marks_progress_fully_done(self):
        task_id = tasks.create_task(task_type="campaign", campaign_id=55)
        tasks.update_task(task_id, progress=350, total=1000, message="Crawling")

        tasks.complete_task(task_id, "Done")

        task = tasks.get_task(task_id)
        self.assertIsNotNone(task)
        self.assertEqual(task.status, "completed")
        self.assertEqual(task.progress, 1000)
        self.assertEqual(task.total, 1000)
        self.assertEqual(task.to_dict()["percent"], 100)

    def test_cancelled_task_marks_progress_fully_done(self):
        task_id = tasks.create_task(task_type="campaign", campaign_id=56)
        tasks.update_task(task_id, progress=350, total=1000, message="Crawling")

        tasks.mark_cancelled(task_id, "Cancelled")

        task = tasks.get_task(task_id)
        self.assertIsNotNone(task)
        self.assertEqual(task.status, "cancelled")
        self.assertEqual(task.progress, 1000)
        self.assertEqual(task.total, 1000)
        self.assertEqual(task.to_dict()["percent"], 100)

    def test_task_status_api_reads_from_db_when_memory_cache_is_empty(self):
        config.save_settings({"onboarded": True})
        app = create_app()
        app.testing = True
        client = app.test_client()

        task_id = tasks.create_task(task_type="campaign", campaign_id=7)
        tasks.update_task(task_id, progress=4, total=12, message="Still running")
        tasks._tasks.clear()

        response = client.get(f"/api/tasks/{task_id}")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["task_id"], task_id)
        self.assertEqual(payload["progress"], 4)
        self.assertEqual(payload["message"], "Still running")

    def test_init_tasks_keeps_fresh_running_task_running(self):
        task_id = tasks.create_task(task_type="campaign", campaign_id=77)
        tasks._tasks.clear()

        tasks.init_tasks()

        task = tasks.get_task(task_id)
        self.assertIsNotNone(task)
        self.assertEqual(task.status, "running")
        self.assertEqual(task.error, "")

    def test_init_tasks_marks_only_stale_running_task_failed_and_updates_campaign(self):
        campaign_id = database.insert_campaign(
            "Restarted campaign",
            ["agency"],
            ["USA"],
            ["Seattle"],
        )
        database.update_campaign_status(campaign_id, "generating")
        stale_time = (datetime.now() - timedelta(seconds=tasks.STALE_TASK_SECONDS + 30)).isoformat()
        database.upsert_task(
            task_id="stalecase123",
            task_type="campaign",
            campaign_id=campaign_id,
            status="running",
            progress=10,
            total=50,
            started_at=stale_time,
            updated_at=stale_time,
        )
        tasks._tasks.clear()

        tasks.init_tasks()

        task = tasks.get_task("stalecase123")
        campaign = database.get_campaign(campaign_id)
        self.assertIsNotNone(task)
        self.assertEqual(task.status, "failed")
        self.assertEqual(task.error, "Server restarted during task")
        self.assertEqual(campaign["status"], "failed")

    def test_campaign_failure_marks_campaign_failed(self):
        campaign_id = database.insert_campaign(
            "Broken campaign",
            ["plumber"],
            ["USA"],
            ["Seattle"],
        )
        task_id = tasks.create_task("campaign")

        with patch("web.routes._campaign_runner.config.get_locations", return_value={"USA": {"tld": ".com", "cities": ["Seattle"]}}), \
             patch("web.routes._campaign_runner._generate_for_combo", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                asyncio.run(run_campaign(task_id, campaign_id))

        campaign = database.get_campaign(campaign_id)
        self.assertEqual(campaign["status"], "failed")

    def test_campaign_does_not_dedup_domains_from_other_campaigns_by_default(self):
        campaign_id = database.insert_campaign(
            "Repeat domain campaign",
            ["agency"],
            ["USA"],
            ["Seattle"],
        )
        other_campaign_id = database.insert_campaign(
            "Older campaign",
            ["agency"],
            ["USA"],
            ["Seattle"],
        )
        database.insert_urls([
            {
                "campaign_id": other_campaign_id,
                "url": "https://example.com/contact",
                "domain": "example.com",
                "niche": "agency",
                "city": "Seattle",
                "country": "USA",
                "source": "bing",
            }
        ])

        task_id = tasks.create_task("campaign", campaign_id=campaign_id)
        fake_report = {
            "tagged_urls": [("https://example.com/contact", "bing")],
            "sources": {"bing": 1, "ddg": 0, "ai": 0},
            "ai": {"status": "disabled", "requested_model": None, "actual_model": None, "error": None},
        }
        fake_stats = {
            "domains_reachable": 1,
            "domains_total": 1,
            "pages_fetched": 1,
            "pages_failed": 0,
            "pages_discovered": 0,
            "pages_robots_blocked": 0,
        }

        with patch("web.routes._campaign_runner.config.get_locations", return_value={"USA": {"tld": ".com", "cities": ["Seattle"]}}), \
             patch("web.routes._campaign_runner.generate_urls_report", return_value=fake_report), \
             patch("web.routes._campaign_runner.crawl_urls", new=AsyncMock(return_value=({}, fake_stats))):
            asyncio.run(run_campaign(task_id, campaign_id))

        urls = database.get_urls(campaign_id)
        stats = database.get_campaign_stats(campaign_id)

        self.assertEqual(len(urls), 1)
        self.assertEqual(stats["deduped_domains"], 0)

    def test_generate_urls_report_tolerates_malformed_tagged_items(self):
        from search import scraper

        malformed = [
            "https://alpha.example",
            ("https://beta.example", "ddg", "extra"),
            ("https://gamma.example",),
            {"url": "https://bad.example"},
        ]

        normalized = scraper._normalize_tagged_urls(malformed)

        self.assertEqual(
            normalized,
            [
                ("https://alpha.example", "unknown"),
                ("https://beta.example", "ddg"),
                ("https://gamma.example", "unknown"),
            ],
        )

    def test_campaign_combo_accepts_string_tagged_urls(self):
        fake_report = {
            "tagged_urls": ["https://alpha.example", ("https://beta.example", "ddg", "extra")],
            "sources": {"bing": 0, "ddg": 1, "ai": 0},
            "ai": {"status": "error", "requested_model": "", "actual_model": None, "error": "broken"},
        }

        with patch("web.routes._campaign_runner.generate_urls_report", return_value=fake_report):
            result = _campaign_runner._generate_for_combo(("agency", "Seattle", "USA", ".com"), 5)

        self.assertEqual(len(result["rows"]), 2)
        self.assertEqual(result["rows"][0]["url"], "https://alpha.example")
        self.assertEqual(result["rows"][0]["source"], "unknown")
        self.assertEqual(result["rows"][1]["url"], "https://beta.example")
        self.assertEqual(result["rows"][1]["source"], "ddg")

    def test_campaign_combo_falls_back_to_ai_when_search_returns_zero_urls(self):
        fake_report = {
            "tagged_urls": [],
            "sources": {"bing": 0, "ddg": 0, "ai": 0},
            "ai": {"status": "error", "requested_model": "", "actual_model": None, "error": "no search urls"},
        }
        ai_result = {
            "urls": ["https://fallback-one.example", "https://fallback-two.example"],
            "status": "ok",
            "requested_model": "openrouter/free",
            "actual_model": "openrouter/free",
            "error": None,
        }

        with patch("web.routes._campaign_runner.generate_urls_report", return_value=fake_report), \
             patch("web.routes._campaign_runner.generate_ai_urls_with_meta", new=AsyncMock(return_value=ai_result)):
            result = _campaign_runner._generate_for_combo(("agency", "Seattle", "USA", ".com"), 5)

        self.assertEqual(len(result["rows"]), 2)
        self.assertEqual(result["rows"][0]["source"], "ai")
        self.assertEqual(result["report"]["sources"]["ai"], 2)

    def test_campaign_combo_tops_up_partial_search_results_with_ai(self):
        fake_report = {
            "tagged_urls": [
                ("https://alpha-one.com", "bing"),
                ("https://beta-two.com", "ddg"),
            ],
            "sources": {"bing": 1, "ddg": 1, "ai": 0},
            "ai": {"status": "disabled", "requested_model": "openrouter/free", "actual_model": None, "error": None},
        }
        ai_result = {
            "urls": [
                "https://gamma-three.com",
                "https://delta-four.com",
                "https://beta-two.com/contact",
            ],
            "status": "ok",
            "requested_model": "openrouter/free",
            "actual_model": "openrouter/free",
            "error": None,
        }

        with patch("web.routes._campaign_runner.generate_urls_report", return_value=fake_report), \
             patch("web.routes._campaign_runner.generate_ai_urls_with_meta", new=AsyncMock(return_value=ai_result)):
            result = _campaign_runner._generate_for_combo(("agency", "Seattle", "USA", ".com"), 4)

        self.assertEqual(len(result["rows"]), 4)
        self.assertEqual([row["source"] for row in result["rows"]], ["bing", "ddg", "ai", "ai"])
        self.assertEqual(result["report"]["sources"]["ai"], 2)
        self.assertEqual(result["report"]["ai"]["status"], "ok")

    def test_campaign_combo_search_only_skips_ai_fallback_and_top_up(self):
        fake_report = {
            "tagged_urls": [("https://alpha-one.com", "bing")],
            "sources": {"bing": 1, "ddg": 0, "ai": 0},
            "ai": {"status": "disabled", "requested_model": "openrouter/free", "actual_model": None, "error": None},
        }

        with patch("web.routes._campaign_runner.generate_urls_report", return_value=fake_report), \
             patch("web.routes._campaign_runner.generate_ai_urls_with_meta", new=AsyncMock(side_effect=AssertionError("AI should not run"))):
            result = _campaign_runner._generate_for_combo(("agency", "Seattle", "USA", ".com"), 4, "search_only")

        self.assertEqual(len(result["rows"]), 1)
        self.assertEqual(result["rows"][0]["source"], "bing")
        self.assertEqual(result["report"]["sources"]["ai"], 0)

    def test_campaign_combo_ai_only_skips_search_generation(self):
        ai_result = {
            "urls": ["https://alpha-ai.com", "https://beta-ai.com"],
            "status": "ok",
            "requested_model": "openrouter/free",
            "actual_model": "openrouter/free",
            "error": None,
        }

        with patch("web.routes._campaign_runner.generate_urls_report", side_effect=AssertionError("search should not run")), \
             patch("web.routes._campaign_runner.generate_ai_urls_with_meta", new=AsyncMock(return_value=ai_result)):
            result = _campaign_runner._generate_for_combo(("agency", "Seattle", "USA", ".com"), 5, "ai_only")

        self.assertEqual(len(result["rows"]), 2)
        self.assertEqual([row["source"] for row in result["rows"]], ["ai", "ai"])

    def test_campaign_progress_units_are_monotonic_across_phases(self):
        start_generate = _campaign_runner._overall_progress_units("generating", 0, 10)
        end_generate = _campaign_runner._overall_progress_units("generating", 10, 10)
        start_crawl = _campaign_runner._overall_progress_units("crawling", 0, 10)
        end_crawl = _campaign_runner._overall_progress_units("crawling", 10, 10)
        start_extract = _campaign_runner._overall_progress_units("extracting", 0, 10)
        end_extract = _campaign_runner._overall_progress_units("extracting", 10, 10)

        self.assertLess(start_generate, end_generate)
        self.assertEqual(end_generate, start_crawl)
        self.assertLess(start_crawl, end_crawl)
        self.assertEqual(end_crawl, start_extract)
        self.assertLess(start_extract, end_extract)
        self.assertEqual(end_extract, _campaign_runner.PROGRESS_TOTAL_UNITS)

    def test_pagination_urls_preserve_structured_query_params(self):
        app = create_app()
        app.testing = True
        campaign_id = database.insert_campaign("Emails", ["agency"], ["USA"], ["Seattle"])

        for index in range(101):
            database.insert_email(
                email=f"user{index}@example.com",
                domain="example.com",
                source_url="https://example.com/contact?page=99",
                source_domain="example.com",
                campaign_id=campaign_id,
                niche="agency",
                city="Seattle",
                country="USA",
            )

        client = app.test_client()
        response = client.get("/emails?foo=landing+page%3D1&page=2", follow_redirects=True)
        html = response.get_data(as_text=True)

        self.assertIn('href="?foo=landing+page%3D1&amp;page=1"', html)
        self.assertIn('href="?foo=landing+page%3D1&amp;page=3"', html)

    def test_dns_fallback_does_not_mark_known_provider_valid(self):
        result = verifier._dns_based_verify("user@gmail.com", "gmail.com", "mx.google.com")

        self.assertEqual(result["verification"], "risky")
        self.assertEqual(result["verification_method"], "dns_provider")
        self.assertEqual(result["domain_confidence"], "high")

    def test_catch_all_is_downgraded_from_valid(self):
        with patch("verification.verifier._get_mx_cached", return_value=(True, "mx.example.com")), \
             patch("verification.verifier._test_smtp_availability", new=AsyncMock(return_value=True)), \
             patch("verification.verifier.check_smtp", new=AsyncMock(side_effect=["valid", "valid"])):
            result = asyncio.run(verifier.verify_email("user@example.com"))

        self.assertEqual(result["verification"], "risky")
        self.assertEqual(result["verification_method"], "smtp_catch_all")
        self.assertEqual(result["is_catch_all"], 1)

    def test_safe_role_inbox_is_not_hard_flagged_as_spam_trap(self):
        self.assertIsNone(verifier.check_spam_trap("abuse@example.com", "example.com"))

    def test_export_can_include_verification_metadata(self):
        app = create_app()
        app.testing = True
        client = app.test_client()
        campaign_id = database.insert_campaign("Export", ["agency"], ["USA"], ["Seattle"])

        database.insert_email(
            email="user@example.com",
            domain="example.com",
            source_url="https://example.com/contact",
            source_domain="example.com",
            campaign_id=campaign_id,
            niche="agency",
            city="Seattle",
            country="USA",
        )
        email_row = database.get_all_emails_filtered()[0]
        database.update_email_verification(
            email_id=email_row["id"],
            verification="risky",
            mx_valid=1,
            smtp_valid=None,
            verification_method="dns_provider",
            mailbox_confidence="unknown",
            domain_confidence="high",
            is_catch_all=0,
        )

        response = client.get(
            "/emails/export?columns=email,verification_method,mailbox_confidence,domain_confidence,is_catch_all"
        )
        reader = csv.reader(io.StringIO(response.get_data(as_text=True)))
        rows = list(reader)

        self.assertEqual(
            rows[0],
            ["Email", "Verification Method", "Mailbox Confidence", "Domain Confidence", "Catch-All"],
        )
        self.assertEqual(rows[1], ["user@example.com", "dns_provider", "unknown", "high", "0"])

    def test_login_session_survives_across_app_instances(self):
        config.save_settings({"app_password": "letmein", "app_password_hash": ""})

        app_one = create_app()
        app_one.testing = True
        client_one = app_one.test_client()

        login_response = client_one.post("/login", data={"password": "letmein"})
        self.assertEqual(login_response.status_code, 302)

        session_cookie = client_one.get_cookie(app_one.config["SESSION_COOKIE_NAME"])
        self.assertIsNotNone(session_cookie)

        app_two = create_app()
        app_two.testing = True
        client_two = app_two.test_client()
        client_two.set_cookie(
            key=session_cookie.key,
            value=session_cookie.value,
            domain=session_cookie.domain or "localhost",
            path=session_cookie.path or "/",
        )

        response = client_two.get("/", follow_redirects=False)

        self.assertEqual(response.status_code, 200)

    def test_campaign_run_redirects_with_task_for_detail_polling(self):
        config.save_settings({"onboarded": True})
        app = create_app()
        app.testing = True
        app.config["WTF_CSRF_ENABLED"] = False
        client = app.test_client()
        campaign_id = database.insert_campaign("Live progress", ["agency"], ["USA"], ["Seattle"])

        with patch("web.routes.campaigns.tasks.run_in_background") as run_in_background:
            response = client.post(f"/campaigns/{campaign_id}/run", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        location = response.headers["Location"]
        self.assertIn(f"/campaigns/{campaign_id}", location)
        self.assertIn("campaign_task=", location)
        run_in_background.assert_called_once()

    def test_new_campaign_persists_source_mode(self):
        config.save_settings({"onboarded": True})
        app = create_app()
        app.testing = True
        app.config["WTF_CSRF_ENABLED"] = False
        client = app.test_client()

        response = client.post(
            "/campaigns/new",
            data={
                "name": "Mode test",
                "niches": "agency",
                "countries": ["USA"],
                "cities": ["Seattle"],
                "source_mode": "search_only",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        campaign = database.get_campaign(1)
        self.assertEqual(campaign["source_mode"], "search_only")

    def test_campaign_run_resets_stale_generating_status_when_no_running_task(self):
        config.save_settings({"onboarded": True})
        app = create_app()
        app.testing = True
        app.config["WTF_CSRF_ENABLED"] = False
        client = app.test_client()
        campaign_id = database.insert_campaign("Retry me", ["agency"], ["USA"], ["Seattle"])
        database.update_campaign_status(campaign_id, "generating")

        with patch("web.routes.campaigns.tasks.find_latest_task", return_value=None), \
             patch("web.routes.campaigns.tasks.run_in_background") as run_in_background:
            response = client.post(f"/campaigns/{campaign_id}/run", follow_redirects=False)

        campaign = database.get_campaign(campaign_id)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(campaign["status"], "failed")
        run_in_background.assert_called_once()

    def test_smtp_probe_tries_multiple_hosts_before_reporting_unavailable(self):
        attempts = []

        class _ConnectionStub:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        def fake_create_connection(address, timeout=None):
            attempts.append((address, timeout))
            if address[0] == "mx1.example.com":
                raise OSError("first host failed")
            return _ConnectionStub()

        with patch("verification.verifier.socket.create_connection", side_effect=fake_create_connection):
            reachable = verifier._probe_smtp_connectivity(("mx1.example.com", "mx2.example.com"), timeout=3.0)

        self.assertTrue(reachable)
        self.assertEqual(
            attempts,
            [(("mx1.example.com", 25), 3.0), (("mx2.example.com", 25), 3.0)],
        )

    def test_database_cleanup_uses_current_filters_and_records_run(self):
        app = create_app()
        app.testing = True
        client = app.test_client()
        campaign_id = database.insert_campaign("Cleanup", ["agency"], ["USA"], ["Seattle", "Portland"])

        rows = [
            ("bad-sea@example.com", "invalid", "Seattle"),
            ("trap-sea@example.com", "spam_trap", "Seattle"),
            ("bad-portland@example.com", "invalid", "Portland"),
            ("good-sea@example.com", "valid", "Seattle"),
        ]
        for email, status, city in rows:
            database.insert_email(
                email=email,
                domain="example.com",
                source_url="https://example.com/contact",
                source_domain="example.com",
                campaign_id=campaign_id,
                niche="agency",
                city=city,
                country="USA",
            )
            email_row = database.get_all_emails_filtered(search=email)[0]
            database.update_email_verification(
                email_id=email_row["id"],
                verification=status,
                mx_valid=1,
                smtp_valid=1,
                verification_method="smtp" if status != "spam_trap" else "spam_trap",
                mailbox_confidence="high",
                domain_confidence="high",
                is_catch_all=0,
            )

        response = client.post(
            "/emails/cleanup",
            data={
                "campaign_id": str(campaign_id),
                "city": "Seattle",
                "statuses": ["invalid", "spam_trap"],
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        remaining = database.get_all_emails_filtered(campaign_id=campaign_id)
        remaining_addresses = {row["email"] for row in remaining}
        self.assertEqual(remaining_addresses, {"bad-portland@example.com", "good-sea@example.com"})

        cleanup_runs = database.get_cleanup_runs(limit=1)
        self.assertEqual(cleanup_runs[0]["preview_count"], 2)
        self.assertEqual(cleanup_runs[0]["deleted_count"], 2)
        self.assertEqual(cleanup_runs[0]["filters"]["city"], "Seattle")

    def test_openrouter_retries_fallback_model_and_returns_meta(self):
        class _Response:
            def __init__(self, status_code, text="", payload=None):
                self.status_code = status_code
                self.text = text
                self._payload = payload or {}

            def json(self):
                return self._payload

        class _ClientStub:
            def __init__(self, responses):
                self._responses = list(responses)

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def post(self, *args, **kwargs):
                return self._responses.pop(0)

        responses = [
            _Response(404, text="model not found"),
            _Response(
                200,
                payload={"choices": [{"message": {"content": "https://alpha.example\nhttps://beta.example"}}]},
            ),
        ]

        with patch("search.ai_generator._get_api_key", return_value="sk-test"), \
             patch("search.ai_generator._candidate_models", return_value=["bad-model", "good-model"]), \
             patch("search.ai_generator.httpx.AsyncClient", return_value=_ClientStub(responses)):
            result = asyncio.run(generate_ai_urls_with_meta("agency", "Seattle", "USA", count=2))

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["requested_model"], "openrouter/free")
        self.assertEqual(result["actual_model"], "good-model")
        self.assertEqual(result["urls"], ["https://alpha.example", "https://beta.example"])

    def test_campaign_stats_include_url_generation_summary(self):
        campaign_id = database.insert_campaign("AI stats", ["agency"], ["USA"], ["Seattle"])
        task_id = tasks.create_task("campaign", campaign_id=campaign_id)

        fake_report = {
            "tagged_urls": [("https://alpha.example", "ai"), ("https://beta.example", "ddg")],
            "sources": {"bing": 0, "ddg": 1, "ai": 1},
            "ai": {
                "status": "error",
                "requested_model": "bad-model",
                "actual_model": None,
                "error": "HTTP 404",
            },
        }
        fake_stats = {
            "domains_reachable": 0,
            "domains_total": 2,
            "pages_fetched": 0,
            "pages_failed": 2,
            "pages_discovered": 0,
            "pages_robots_blocked": 0,
        }

        with patch("web.routes._campaign_runner.config.get_locations", return_value={"USA": {"tld": ".com", "cities": ["Seattle"]}}), \
             patch("web.routes._campaign_runner.generate_urls_report", return_value=fake_report), \
             patch("web.routes._campaign_runner.crawl_urls", new=AsyncMock(return_value=({}, fake_stats))):
            asyncio.run(run_campaign(task_id, campaign_id))

        stats = database.get_campaign_stats(campaign_id)
        self.assertEqual(stats["url_generation"]["sources"]["ai"], 1)
        self.assertEqual(stats["url_generation"]["sources"]["ddg"], 1)
        self.assertEqual(stats["url_generation"]["ai"]["status"], "partial")
        self.assertEqual(stats["url_generation"]["ai"]["error"], "HTTP 404")
        self.assertEqual(stats["url_generation"]["ai"]["requested_models"], ["bad-model"])

    def test_logging_setup_adds_server_log_handlers(self):
        logging_setup.setup_logging()
        base_files = {
            os.path.basename(getattr(handler, "baseFilename", ""))
            for handler in logging_setup.logging.getLogger().handlers
            if getattr(handler, "baseFilename", None)
        }
        self.assertIn("server.out.log", base_files)
        self.assertIn("server.err.log", base_files)

    def test_openrouter_partial_status_when_short_of_target(self):
        class _Response:
            def __init__(self, payload):
                self.status_code = 200
                self.text = ""
                self._payload = payload

            def json(self):
                return self._payload

        class _ClientStub:
            def __init__(self, responses):
                self._responses = list(responses)

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def post(self, *args, **kwargs):
                return self._responses.pop(0)

        responses = [
            _Response({"choices": [{"message": {"content": "https://alpha.example\nhttps://beta.example"}}], "model": "model-a"}),
            _Response({"choices": [{"message": {"content": "https://alpha.example\nhttps://beta.example"}}], "model": "model-a"}),
            _Response({"choices": [{"message": {"content": "https://alpha.example\nhttps://beta.example"}}], "model": "model-a"}),
        ]

        with patch("search.ai_generator._get_api_key", return_value="sk-test"), \
             patch("search.ai_generator._candidate_models", return_value=["model-a"]), \
             patch("search.ai_generator.httpx.AsyncClient", return_value=_ClientStub(responses)):
            result = asyncio.run(generate_ai_urls_with_meta("agency", "Seattle", "USA", count=4))

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["urls"], ["https://alpha.example", "https://beta.example"])

    def test_plaintext_master_key_no_longer_validates(self):
        license_validator.install_license("GRAPHENMAIL-MASTER-ADMIN-2026")

        state = license_validator.validate(force=True)

        self.assertFalse(state.valid)

    def test_signed_wildcard_license_validates_with_bundled_public_key(self):
        signing_key_path = self._write_signing_key_pair()

        from licensing.issue import cmd_sign, build_parser

        parser = build_parser()
        args = parser.parse_args(
            [
                "sign",
                "--signing-key",
                str(signing_key_path),
                "--customer",
                "admin",
                "--host-fingerprint",
                "*",
                "--features",
                "ai_urls,ip_rotation",
            ]
        )

        stdout = io.StringIO()
        with patch("sys.stdout", stdout):
            exit_code = cmd_sign(args)

        self.assertEqual(exit_code, 0)
        license_validator.install_license(stdout.getvalue().strip())

        state = license_validator.validate(force=True)

        self.assertTrue(state.valid)
        self.assertEqual(state.customer, "admin")

    def test_issue_sign_supports_months_expiry(self):
        signing_key_path = self._write_signing_key_pair()

        from licensing.issue import cmd_sign, build_parser

        parser = build_parser()
        args = parser.parse_args(
            [
                "sign",
                "--signing-key",
                str(signing_key_path),
                "--customer",
                "month-test",
                "--host-fingerprint",
                "*",
                "--months",
                "1",
            ]
        )

        stdout = io.StringIO()
        with patch("sys.stdout", stdout):
            exit_code = cmd_sign(args)

        self.assertEqual(exit_code, 0)
        license_validator.install_license(stdout.getvalue().strip())
        state = license_validator.validate(force=True)

        self.assertTrue(state.valid)
        self.assertIsNotNone(state.expires_at)

    def test_rotator_skips_dead_ipv6_and_caches_healthy_ip(self):
        class _ConnectionStub:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        attempts = []

        def fake_getaddrinfo(host, port, family, socktype):
            self.assertEqual(family, socket.AF_INET6)
            return [(family, socktype, 6, "", ("2606:4700::1111", port, 0, 0))]

        def fake_create_connection(address, timeout=None, source_address=None):
            attempts.append(source_address[0])
            if source_address[0] == "2001:db8::1":
                raise OSError("network unreachable")
            return _ConnectionStub()

        rotator._index = 0
        rotator._cooldowns.clear()
        rotator._health_cache.clear()
        rotator._ip_stats.clear()

        with patch("search.rotator.config.get_setting", return_value=["2001:db8::1", "2001:db8::2"]), \
             patch.object(rotator, "HEALTHCHECK_HOSTS", ("www.bing.com",)), \
             patch("search.rotator.socket.getaddrinfo", side_effect=fake_getaddrinfo), \
             patch("search.rotator.socket.create_connection", side_effect=fake_create_connection):
            first = rotator.get_next_ip()
            second = rotator.get_next_ip()

        self.assertEqual(first, "2001:db8::2")
        self.assertEqual(second, "2001:db8::2")
        self.assertEqual(attempts, ["2001:db8::1", "2001:db8::2"])
        self.assertFalse(rotator._get_cached_health("2001:db8::1"))
        self.assertTrue(rotator._get_cached_health("2001:db8::2"))

    def test_rotator_uses_default_route_when_rotation_disabled(self):
        rotator._index = 0
        rotator._cooldowns.clear()
        rotator._health_cache.clear()
        rotator._ip_stats.clear()

        with patch("search.rotator.config.get_setting", side_effect=lambda key, default=None: {
            "search_ip_rotation_enabled": False,
            "outbound_ips": ["2001:db8::1", "2001:db8::2"],
        }.get(key, default)):
            self.assertEqual(rotator.get_available_ips(), [])
            self.assertIsNone(rotator.get_next_ip())
            status = rotator.get_status()

        self.assertFalse(status["enabled"])
        self.assertEqual(status["total_ips"], 2)

    def test_rotator_prefers_ip_with_real_results_over_probe_only_ip(self):
        rotator._index = 0
        rotator._cooldowns.clear()
        rotator._health_cache.clear()
        rotator._ip_stats.clear()

        with patch("search.rotator.config.get_setting", side_effect=lambda key, default=None: {
            "search_ip_rotation_enabled": True,
            "outbound_ips": ["2001:db8::1", "2001:db8::2"],
        }.get(key, default)):
            rotator.record_ip_healthy("2001:db8::1")
            rotator.record_ip_healthy("2001:db8::2")
            rotator.record_ip_healthy("2001:db8::2", result_count=6)

            chosen = rotator.get_next_ip()
            status = rotator.get_status()

        self.assertEqual(chosen, "2001:db8::2")
        self.assertEqual(status["ranked_ips"][0]["ip"], "2001:db8::2")
        self.assertGreater(status["ranked_ips"][0]["score"], status["ranked_ips"][1]["score"])

    def test_rotator_deprioritizes_empty_result_ip_without_cooldown(self):
        rotator._index = 0
        rotator._cooldowns.clear()
        rotator._health_cache.clear()
        rotator._ip_stats.clear()

        with patch("search.rotator.config.get_setting", side_effect=lambda key, default=None: {
            "search_ip_rotation_enabled": True,
            "outbound_ips": ["2001:db8::10", "2001:db8::11"],
        }.get(key, default)):
            rotator.record_ip_healthy("2001:db8::10")
            rotator.record_ip_healthy("2001:db8::11")
            rotator.record_ip_empty("2001:db8::10")
            rotator.record_ip_healthy("2001:db8::11", result_count=2)

            chosen = rotator.get_next_ip()
            status = rotator.get_status()

        self.assertEqual(chosen, "2001:db8::11")
        self.assertEqual(status["ranked_ips"][0]["ip"], "2001:db8::11")
        self.assertEqual(status["ranked_ips"][1]["ip"], "2001:db8::10")
        self.assertEqual(status["cooled_down_ips"], 0)

    def test_rotator_does_not_stick_to_single_recent_winner(self):
        rotator._index = 0
        rotator._cooldowns.clear()
        rotator._health_cache.clear()
        rotator._ip_stats.clear()

        with patch("search.rotator.config.get_setting", side_effect=lambda key, default=None: {
            "search_ip_rotation_enabled": True,
            "outbound_ips": ["2001:db8::21", "2001:db8::22", "2001:db8::23"],
        }.get(key, default)):
            rotator.record_ip_healthy("2001:db8::21")
            rotator.record_ip_healthy("2001:db8::22")
            rotator.record_ip_healthy("2001:db8::23")
            rotator.record_ip_healthy("2001:db8::21", result_count=5)

            first = rotator.get_next_ip()
            second = rotator.get_next_ip()
            third = rotator.get_next_ip()

        self.assertEqual(first, "2001:db8::21")
        self.assertIn(second, {"2001:db8::22", "2001:db8::23"})
        self.assertNotEqual(second, first)
        self.assertIn(third, {"2001:db8::21", "2001:db8::22", "2001:db8::23"})

    def test_bing_bound_ip_falls_back_to_default_route_and_marks_ip_unhealthy(self):
        from search import scraper

        class _Response:
            def __init__(self, status_code=200, text=""):
                self.status_code = status_code
                self.text = text

        class _ClientStub:
            def __init__(self, responses):
                self._responses = list(responses)

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def get(self, *args, **kwargs):
                response = self._responses.pop(0)
                if isinstance(response, Exception):
                    raise response
                return response

        client_calls = []
        clients = [
            _ClientStub([_Response(200, text="BOUND")]),
            _ClientStub([_Response(200, text="DEFAULT")]),
        ]

        def fake_async_client(*args, **kwargs):
            client_calls.append(kwargs.get("transport"))
            return clients.pop(0)

        def fake_get_setting(_key, default=None):
            return default

        with patch("search.scraper.get_next_ip", return_value="2001:db8::9"), \
             patch("search.scraper.httpx.AsyncHTTPTransport", side_effect=lambda local_address=None: {"local_address": local_address}), \
             patch("search.scraper.httpx.AsyncClient", side_effect=fake_async_client), \
             patch("search.scraper._is_captcha_response", return_value=False), \
             patch("search.scraper._parse_bing_results", side_effect=lambda html: [] if html == "BOUND" else ["https://example.com"]), \
             patch("search.scraper.config.get_setting", side_effect=fake_get_setting), \
             patch("search.scraper.asyncio.sleep", new=AsyncMock()), \
             patch("search.scraper.mark_ip_unhealthy") as mark_ip_unhealthy:
            urls, was_blocked = asyncio.run(scraper._scrape_bing_page("agency seattle"))

        self.assertEqual(urls, ["https://example.com"])
        self.assertFalse(was_blocked)
        self.assertIsNotNone(client_calls[0])
        self.assertIsNone(client_calls[1])
        mark_ip_unhealthy.assert_called_once_with("2001:db8::9", "0 parsed URLs while default route recovered")

    def test_ddg_bound_ip_falls_back_to_default_route_and_marks_ip_unhealthy(self):
        from search import duckduckgo

        class _Response:
            def __init__(self, status_code=200, text=""):
                self.status_code = status_code
                self.text = text

        class _ClientStub:
            def __init__(self, responses):
                self._responses = list(responses)

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def post(self, *args, **kwargs):
                response = self._responses.pop(0)
                if isinstance(response, Exception):
                    raise response
                return response

        client_calls = []
        clients = [
            _ClientStub([_Response(500, text="BOUND")]),
            _ClientStub([_Response(200, text="DEFAULT")]),
        ]

        def fake_async_client(*args, **kwargs):
            client_calls.append(kwargs.get("transport"))
            return clients.pop(0)

        with patch("search.rotator.get_next_ip", return_value="2001:db8::10"), \
             patch("search.duckduckgo.httpx.AsyncHTTPTransport", side_effect=lambda local_address=None: {"local_address": local_address}), \
             patch("search.duckduckgo.httpx.AsyncClient", side_effect=fake_async_client), \
             patch("search.duckduckgo._parse_ddg_results", side_effect=lambda html: ["https://example.com"] if html == "DEFAULT" else []), \
             patch("search.duckduckgo.asyncio.sleep", new=AsyncMock()), \
             patch("search.rotator.mark_ip_unhealthy") as mark_ip_unhealthy:
            urls = asyncio.run(duckduckgo._scrape_ddg_page("agency seattle"))

        self.assertEqual(urls, ["https://example.com"])
        self.assertIsNotNone(client_calls[0])
        self.assertIsNone(client_calls[1])
        mark_ip_unhealthy.assert_called_once_with("2001:db8::10", "HTTP 500")

    def test_verifier_uses_configured_ipv6_safe_identity(self):
        verifier.clear_mx_cache()
        os.environ["GM_SMTP_EHLO_HOSTNAME"] = "mx.example.com"
        os.environ["GM_SMTP_MAIL_FROM"] = "verify@example.com"

        with patch("verification.verifier.socket.getfqdn", side_effect=AssertionError("should not probe fqdn")):
            self.assertEqual(verifier._get_ehlo_hostname(), "mx.example.com")
            self.assertEqual(verifier._get_mail_from_address(), "verify@example.com")

    def test_verifier_accepts_aaaa_only_domains_in_dns_fallback(self):
        def fake_resolve(domain, record_type):
            if record_type == "A":
                raise dns.resolver.NoAnswer()
            if record_type == "AAAA":
                return [object()]
            raise AssertionError(f"Unexpected record type: {record_type}")

        with patch("verification.verifier.dns.resolver.resolve", side_effect=fake_resolve):
            self.assertTrue(verifier._check_domain_a_record("example.com"))

    def test_networking_build_rotation_plan_reports_missing_candidate_ips(self):
        def fake_run(command, capture_output=True, text=True, check=False):
            if command == ["ip", "route", "show", "default"]:
                return subprocess.CompletedProcess(command, 0, stdout="default via 80.96.113.1 dev eth0\n", stderr="")
            if command == ["ip", "-j", "addr", "show", "dev", "eth0"]:
                return subprocess.CompletedProcess(
                    command,
                    0,
                    stdout='[{"ifname":"eth0","addr_info":[{"family":"inet","local":"80.96.113.252","scope":"global"},{"family":"inet6","local":"2001:678:6d4:6570::42","scope":"global"}]}]',
                    stderr="",
                )
            raise AssertionError(f"Unexpected command: {command}")

        plan = networking.build_rotation_plan(
            candidate_ips=["2001:678:6d4:6570::42", "2001:678:6d4:6570::150", "80.96.113.252"],
            configured_ips=["80.96.113.252", "80.96.113.140"],
            runner=fake_run,
        )

        self.assertEqual(plan["interface"], "eth0")
        self.assertEqual(plan["candidate_assigned_ips"], ["2001:678:6d4:6570::42", "80.96.113.252"])
        self.assertEqual(plan["candidate_missing_ips"], ["2001:678:6d4:6570::150"])
        self.assertEqual(plan["configured_missing_ips"], ["80.96.113.140"])
        self.assertIn("2001:678:6d4:6570::150/64", plan["netplan_snippet"])
        self.assertIn("80.96.113.252/24", plan["netplan_snippet"])

    def test_networking_build_rotation_plan_uses_configured_ips_when_candidates_missing(self):
        def fake_run(command, capture_output=True, text=True, check=False):
            if command == ["ip", "route", "show", "default"]:
                return subprocess.CompletedProcess(command, 0, stdout="default via 80.96.113.1 dev eth0\n", stderr="")
            if command == ["ip", "-j", "addr", "show", "dev", "eth0"]:
                return subprocess.CompletedProcess(
                    command,
                    0,
                    stdout='[{"ifname":"eth0","addr_info":[{"family":"inet","local":"80.96.113.252","prefixlen":24,"scope":"global"},{"family":"inet6","local":"2001:678:6d4:6570::42","prefixlen":64,"scope":"global"}]}]',
                    stderr="",
                )
            raise AssertionError(f"Unexpected command: {command}")

        plan = networking.build_rotation_plan(
            candidate_ips=[],
            configured_ips=["80.96.113.252", "2001:678:6d4:6570::150"],
            runner=fake_run,
        )

        self.assertEqual(plan["candidate_ips"], ["80.96.113.252", "2001:678:6d4:6570::150"])
        self.assertEqual(plan["candidate_assigned_ips"], ["80.96.113.252"])
        self.assertEqual(plan["candidate_missing_ips"], ["2001:678:6d4:6570::150"])
        self.assertIn("2001:678:6d4:6570::150/64", plan["netplan_snippet"])

    def test_admin_settings_page_shows_smtp_identity_fields(self):
        config.save_settings({"onboarded": True})
        app = create_app()
        app.testing = True
        client = app.test_client()

        with client.session_transaction() as session_state:
            session_state["authenticated"] = True
            session_state["is_admin"] = True

        response = client.get("/settings/", follow_redirects=False)
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn('name="smtp_ehlo_hostname"', html)
        self.assertIn('name="smtp_mail_from"', html)
        self.assertIn('name="search_ip_rotation_enabled"', html)
        self.assertIn('name="rotation_candidate_ips"', html)
        self.assertIn('id="network-helper-command"', html)
        self.assertIn("Verifier SMTP identity is using automatic fallbacks", html)

    def test_admin_can_save_search_ip_rotation_toggle(self):
        config.save_settings({"onboarded": True})
        app = create_app()
        app.testing = True
        app.config["WTF_CSRF_ENABLED"] = False
        client = app.test_client()

        with client.session_transaction() as session_state:
            session_state["authenticated"] = True
            session_state["is_admin"] = True

        response = client.post(
            "/settings/",
            data={
                "search_ip_rotation_enabled": "1",
                "outbound_ips": "2001:db8::1\n2001:db8::2",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(config.get_setting("search_ip_rotation_enabled", False))
        self.assertEqual(config.get_setting("outbound_ips", []), ["2001:db8::1", "2001:db8::2"])

    def test_admin_can_sync_outbound_ips_from_assigned_rotation_candidates(self):
        config.save_settings({"onboarded": True})
        app = create_app()
        app.testing = True
        app.config["WTF_CSRF_ENABLED"] = False
        client = app.test_client()

        with client.session_transaction() as session_state:
            session_state["authenticated"] = True
            session_state["is_admin"] = True

        with patch("web.routes.settings.networking.build_rotation_plan", return_value={
            "candidate_assigned_ips": ["2001:db8::42", "80.96.113.252"],
        }):
            response = client.post(
                "/settings/",
                data={
                    "search_ip_rotation_enabled": "1",
                    "rotation_network_interface": "eth0",
                    "rotation_candidate_ips": "2001:db8::42\n2001:db8::150\n80.96.113.252",
                    "sync_outbound_ips_from_candidates": "1",
                },
                follow_redirects=False,
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(config.get_setting("rotation_network_interface", ""), "eth0")
        self.assertEqual(
            config.get_setting("rotation_candidate_ips", []),
            ["2001:db8::42", "2001:db8::150", "80.96.113.252"],
        )
        self.assertEqual(config.get_setting("outbound_ips", []), ["2001:db8::42", "80.96.113.252"])

    def test_admin_save_uses_outbound_ips_as_candidates_when_candidate_box_empty(self):
        config.save_settings({"onboarded": True})
        app = create_app()
        app.testing = True
        app.config["WTF_CSRF_ENABLED"] = False
        client = app.test_client()

        with client.session_transaction() as session_state:
            session_state["authenticated"] = True
            session_state["is_admin"] = True

        response = client.post(
            "/settings/",
            data={
                "search_ip_rotation_enabled": "1",
                "outbound_ips": "2001:db8::42\n2001:db8::150",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(config.get_setting("rotation_candidate_ips", []), ["2001:db8::42", "2001:db8::150"])

    def test_admin_ip_status_api_reports_assigned_and_missing_ips(self):
        config.save_settings({
            "onboarded": True,
            "rotation_network_interface": "eth0",
            "rotation_candidate_ips": ["2001:db8::42", "2001:db8::150"],
            "outbound_ips": ["2001:db8::42"],
        })
        app = create_app()
        app.testing = True
        client = app.test_client()

        with client.session_transaction() as session_state:
            session_state["authenticated"] = True
            session_state["is_admin"] = True

        with patch("web.routes.api.networking.build_rotation_plan", return_value={
            "interface": "eth0",
            "supported": True,
            "error": None,
            "assigned_ipv4": ["80.96.113.252"],
            "assigned_ipv6": ["2001:db8::42"],
            "assigned_ips": ["80.96.113.252", "2001:db8::42"],
            "candidate_ips": ["2001:db8::42", "2001:db8::150"],
            "candidate_assigned_ips": ["2001:db8::42"],
            "candidate_missing_ips": ["2001:db8::150"],
            "configured_ips": ["2001:db8::42"],
            "configured_assigned_ips": ["2001:db8::42"],
            "configured_missing_ips": [],
            "recommended_outbound_ips": ["2001:db8::42"],
            "netplan_snippet": "network:\n  version: 2\n",
            "desired_netplan_ips": ["80.96.113.252", "2001:db8::42", "2001:db8::150"],
            "configure_command": "sudo python3 /opt/graphenmail/deploy/configure_ip_pool.py --apply --enable-rotation",
        }), patch("search.rotator.get_status", return_value={"enabled": True, "total_ips": 1, "available_ips": 1, "cooled_down_ips": 0, "cooldown_list": [], "unhealthy_ips": 0, "unhealthy_list": [], "ranked_ips": []}), patch("search.rotator.get_available_ips", return_value=["2001:db8::42"]), patch("search.rotator._load_ips", return_value=["2001:db8::42"]):
            response = client.get("/api/ip-status")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["assigned_ips"], ["80.96.113.252", "2001:db8::42"])
        self.assertEqual(payload["candidate_missing_ips"], ["2001:db8::150"])
        self.assertEqual(payload["configure_command"], "sudo python3 /opt/graphenmail/deploy/configure_ip_pool.py --apply --enable-rotation")

    def test_onboarding_password_creation_grants_admin_session(self):
        app = create_app()
        app.testing = True
        app.config["WTF_CSRF_ENABLED"] = False
        client = app.test_client()

        response = client.post(
            "/onboarding/",
            data={"password": "supersecret"},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        with client.session_transaction() as session_state:
            self.assertTrue(session_state.get("authenticated"))
            self.assertTrue(session_state.get("is_admin"))

    def test_onboarding_password_remains_admin_after_relogin(self):
        app = create_app()
        app.testing = True
        app.config["WTF_CSRF_ENABLED"] = False
        client = app.test_client()

        client.post(
            "/onboarding/",
            data={"password": "supersecret"},
            follow_redirects=False,
        )
        client.get("/logout", follow_redirects=False)
        login_response = client.post("/login", data={"password": "supersecret"}, follow_redirects=False)

        self.assertEqual(login_response.status_code, 302)
        with client.session_transaction() as session_state:
            self.assertTrue(session_state.get("authenticated"))
            self.assertTrue(session_state.get("is_admin"))

    def test_non_admin_cannot_save_smtp_identity_fields(self):
        config.save_settings({"onboarded": True})
        app = create_app()
        app.testing = True
        app.config["WTF_CSRF_ENABLED"] = False
        client = app.test_client()

        with client.session_transaction() as session_state:
            session_state["authenticated"] = True
            session_state["is_admin"] = False

        response = client.post(
            "/settings/",
            data={
                "smtp_ehlo_hostname": "mail.example.com",
                "smtp_mail_from": "verify@example.com",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(config.get_setting("smtp_ehlo_hostname", ""), "")
        self.assertEqual(config.get_setting("smtp_mail_from", ""), "")

    def test_non_admin_cannot_remove_global_password(self):
        config.save_settings({"onboarded": True, "app_password": "", "app_password_hash": "hash"})
        app = create_app()
        app.testing = True
        app.config["WTF_CSRF_ENABLED"] = False
        client = app.test_client()

        with client.session_transaction() as session_state:
            session_state["authenticated"] = True
            session_state["is_admin"] = False

        response = client.post(
            "/settings/",
            data={"remove_password": "1"},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(config.get_setting("app_password_hash", ""), "hash")

    def test_admin_saving_smtp_identity_clears_verifier_identity_cache(self):
        config.save_settings({"onboarded": True})
        verifier._ehlo_hostname = "stale.example.com"
        verifier._mail_from_address = "stale@example.com"

        app = create_app()
        app.testing = True
        app.config["WTF_CSRF_ENABLED"] = False
        client = app.test_client()

        with client.session_transaction() as session_state:
            session_state["authenticated"] = True
            session_state["is_admin"] = True

        response = client.post(
            "/settings/",
            data={
                "smtp_ehlo_hostname": "mail.example.com",
                "smtp_mail_from": "verify@example.com",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(config.get_setting("smtp_ehlo_hostname", ""), "mail.example.com")
        self.assertEqual(config.get_setting("smtp_mail_from", ""), "verify@example.com")
        self.assertIsNone(verifier._ehlo_hostname)
        self.assertIsNone(verifier._mail_from_address)

    def test_admin_password_change_updates_admin_login(self):
        config.save_settings({"onboarded": True})
        app = create_app()
        app.testing = True
        app.config["WTF_CSRF_ENABLED"] = False
        client = app.test_client()

        with client.session_transaction() as session_state:
            session_state["authenticated"] = True
            session_state["is_admin"] = True

        response = client.post(
            "/settings/",
            data={
                "new_password": "newadminpass",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        client.get("/logout", follow_redirects=False)
        login_response = client.post("/login", data={"password": "newadminpass"}, follow_redirects=False)

        self.assertEqual(login_response.status_code, 302)
        with client.session_transaction() as session_state:
            self.assertTrue(session_state.get("is_admin"))

    def test_non_admin_cannot_access_license_lab(self):
        config.save_settings({"onboarded": True})
        app = create_app()
        app.testing = True
        client = app.test_client()

        with client.session_transaction() as session_state:
            session_state["authenticated"] = True
            session_state["is_admin"] = False

        response = client.get("/admin/licenses/", follow_redirects=False)

        self.assertEqual(response.status_code, 404)

    def test_admin_license_lab_generates_signed_wildcard_license(self):
        config.save_settings({"onboarded": True})
        signing_key_path = self._write_signing_key_pair()
        config.save_settings({"license_signing_key_path": str(signing_key_path)})

        app = create_app()
        app.testing = True
        app.config["WTF_CSRF_ENABLED"] = False
        client = app.test_client()

        with client.session_transaction() as session_state:
            session_state["authenticated"] = True
            session_state["is_admin"] = True

        response = client.post(
            "/admin/licenses/",
            data={
                "action": "generate",
                "customer": "Admin",
                "signing_key_path": str(signing_key_path),
                "host_fingerprint": "*",
                "expiry_preset": "perpetual",
                "features": "ai_urls,ip_rotation",
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        match = re.search(r"<textarea[^>]*readonly[^>]*>\s*([^<]+)\s*</textarea>", html)
        self.assertIsNotNone(match)

        license_validator.install_license(match.group(1).strip())
        state = license_validator.validate(force=True)

        self.assertTrue(state.valid)
        self.assertEqual(state.customer, "Admin")


if __name__ == "__main__":
    unittest.main()
