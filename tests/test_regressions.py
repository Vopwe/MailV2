import asyncio
import csv
import io
import os
import socket
import sqlite3
import shutil
import threading
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import AsyncMock, patch
import uuid

import config
import database
import dns.resolver
import logging_setup
import tasks
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from licensing import validator as license_validator
from search.ai_generator import generate_ai_urls_with_meta
from search import rotator
from verification import verifier
from web import create_app
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
    original_env_master_key = os.environ.get("GRAPHENMAIL_MASTER_KEY")

    temp_dir = os.path.join(config.BASE_DIR, ".test-config", uuid.uuid4().hex)
    os.makedirs(temp_dir, exist_ok=True)
    license_validator.PUBLIC_KEY_PATH = Path(temp_dir) / "public_key.pem"
    os.environ["LICENSE_PATH"] = os.path.join(temp_dir, "license.key")
    os.environ.pop("GRAPHENMAIL_MASTER_KEY", None)
    license_validator.invalidate_cache()
    license_validator._public_key = None
    try:
        yield temp_dir
    finally:
        if original_env_license_path is None:
            os.environ.pop("LICENSE_PATH", None)
        else:
            os.environ["LICENSE_PATH"] = original_env_license_path
        if original_env_master_key is None:
            os.environ.pop("GRAPHENMAIL_MASTER_KEY", None)
        else:
            os.environ["GRAPHENMAIL_MASTER_KEY"] = original_env_master_key
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
        tasks._tasks.clear()
        self._license_context.__exit__(None, None, None)
        self._db_context.__exit__(None, None, None)
        self._config_context.__exit__(None, None, None)

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
        app = create_app()
        app.testing = True
        client = app.test_client()
        campaign_id = database.insert_campaign("Live progress", ["agency"], ["USA"], ["Seattle"])

        with patch("web.routes.campaigns.tasks.run_in_background") as run_in_background:
            response = client.post(f"/campaigns/{campaign_id}/run", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        location = response.headers["Location"]
        self.assertIn(f"/campaigns/{campaign_id}", location)
        self.assertIn("campaign_task=", location)
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

    def test_master_admin_key_works_without_public_key_file(self):
        license_validator.install_license(license_validator.MASTER_ADMIN_KEY)

        state = license_validator.validate(force=True)

        self.assertTrue(state.valid)
        self.assertEqual(state.customer, "admin-master")

    def test_signed_wildcard_license_validates_with_bundled_public_key(self):
        signing_key = Ed25519PrivateKey.generate()
        public_key_bytes = signing_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        signing_key_path = Path(os.environ["LICENSE_PATH"]).with_name("signing_key.pem")
        signing_key_path.write_bytes(
            signing_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
        license_validator.PUBLIC_KEY_PATH.write_bytes(public_key_bytes)

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
        self.assertIn("Verifier SMTP identity is using automatic fallbacks", html)

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


if __name__ == "__main__":
    unittest.main()
