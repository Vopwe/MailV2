"""
Configuration — Bing scraper settings, crawl behavior.
Supports runtime overrides via settings.json.
"""
import json
import os
import secrets
import time

# ─── Feature Flags ────────────────────────────────────────────────────

def tls_verify() -> bool:
    """
    Whether outbound httpx clients should verify TLS certificates.
    Default True. Only set CRAWL_TLS_VERIFY=false on networks that proxy
    outbound TLS via an internal CA (rare).
    """
    return os.getenv("CRAWL_TLS_VERIFY", "true").strip().lower() not in ("false", "0", "no")


# ─── Paths ────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SETTINGS_PATH = os.path.join(BASE_DIR, "settings.json")
SECRET_KEY_PATH = os.path.join(BASE_DIR, ".flask_secret_key")
DATABASE_PATH = os.getenv("EMAIL_DB_PATH", os.path.join(BASE_DIR, "emails.db"))
LOCATIONS_PATH = os.path.join(BASE_DIR, "data", "locations.json")
DISPOSABLE_PATH = os.path.join(BASE_DIR, "data", "disposable_domains.txt")
SPAM_TRAP_DOMAINS_PATH = os.path.join(BASE_DIR, "data", "spam_trap_domains.txt")

# ─── Bing Scraper Defaults ───────────────────────────────────────────
BING_CONCURRENCY = 5
BING_DELAY_MIN = 2.0
BING_DELAY_MAX = 5.0
BING_RESULTS_PER_PAGE = 50
SEARCH_IP_ROTATION_ENABLED = False
OUTBOUND_IPS = []  # Configured via settings.json or ips.txt

# ─── DDG Scraper Defaults ────────────────────────────────────────────
DDG_CONCURRENCY = 5
DDG_DELAY_MIN = 1.0
DDG_DELAY_MAX = 3.0

# ─── Crawl Defaults ──────────────────────────────────────────────────
MAX_CONCURRENT_REQUESTS = 30
REQUEST_TIMEOUT = 12
CRAWL_DELAY = 0.2
MAX_PAGES_PER_DOMAIN = 5

COMMON_PATHS = [
    "/", "/contact", "/contact-us", "/about", "/about-us",
    "/team", "/our-team", "/staff", "/people", "/support",
    "/impressum", "/legal",
]

# ─── Email Verification ──────────────────────────────────────────────
VERIFY_TIMEOUT = 10
SKIP_GENERIC_EMAILS = True

# System/bounce prefixes — these are NOT read by real people, always flagged
GENERIC_PREFIXES = [
    "noreply", "no-reply", "no_reply", "donotreply", "do-not-reply",
    "mailer-daemon", "daemon", "bounce", "bounces",
    "postmaster", "hostmaster", "usenet", "news", "root",
    "nobody", "devnull", "null", "void",
]

# Known spam trap / honeypot prefixes
SPAM_TRAP_PREFIXES = [
    "spamtrap", "spam-trap", "spam_trap",
    "honeypot", "honey-pot", "honey_pot",
    "trap", "spam",
    "antispam", "anti-spam",
    "phishing", "malware",
    "blackhole", "black-hole",
    "junk", "quarantine",
    "seedlist", "seed-list",
    "example", "sample",
    "tempmail", "temp-mail",
]

# Role inboxes that should not be auto-flagged as traps.
SAFE_ROLE_PREFIXES = [
    "abuse", "admin", "billing", "careers", "compliance",
    "contact", "customerservice", "help", "hr", "info",
    "legal", "marketing", "office", "operations", "postmaster",
    "privacy", "sales", "service", "support",
]

# Ambiguous local parts that are suspicious, but not enough for a hard spam-trap label.
SOFT_RISK_PREFIXES = [
    "test", "testing", "tester",
    "asdf", "qwerty", "aaa", "zzz", "xxx",
]

# ─── URLs per Bing scrape batch ──────────────────────────────────────
URLS_PER_BATCH = 40

# ─── Verification Concurrency ────────────────────────────────────────
VERIFY_CONCURRENCY = 30


# ─── Runtime Settings (settings.json) ────────────────────────────────

def _load_settings() -> dict:
    if os.path.exists(SETTINGS_PATH):
        try:
            with open(SETTINGS_PATH, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


# Sensitive settings that should prefer env vars over settings.json.
# Env var name is GM_<KEY_UPPER>. Useful for container / systemd EnvironmentFile deploys.
_ENV_OVERRIDABLE_KEYS = {
    "admin_password_hash",
    "license_signing_key_path",
    "openrouter_api_key",
    "app_password_hash",
}


def get_setting(key: str, default=None):
    """
    Get a runtime setting.
    Lookup order for sensitive keys: env var GM_<KEY> → settings.json → default.
    Non-sensitive keys: settings.json → default.
    """
    if key in _ENV_OVERRIDABLE_KEYS:
        env_val = os.getenv(f"GM_{key.upper()}")
        if env_val:
            return env_val
    settings = _load_settings()
    return settings.get(key, default)


def save_settings(updates: dict):
    """Merge updates into settings.json."""
    settings = _load_settings()
    settings.update(updates)
    with open(SETTINGS_PATH, "w") as f:
        json.dump(settings, f, indent=2)


def _read_secret_key() -> str | None:
    try:
        with open(SECRET_KEY_PATH, "r", encoding="utf-8") as f:
            secret = f.read().strip()
            return secret or None
    except FileNotFoundError:
        return None


def get_secret_key() -> str:
    """
    Return a stable Flask secret key shared across all app workers.

    APP_SECRET_KEY / FLASK_SECRET_KEY can override the on-disk key for deployments
    that prefer environment-based secrets.
    """
    env_secret = os.getenv("APP_SECRET_KEY") or os.getenv("FLASK_SECRET_KEY")
    if env_secret:
        return env_secret

    lock_path = f"{SECRET_KEY_PATH}.lock"

    existing_secret = _read_secret_key()
    if existing_secret:
        return existing_secret

    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        for _ in range(50):
            existing_secret = _read_secret_key()
            if existing_secret:
                return existing_secret
            time.sleep(0.1)
        raise RuntimeError(f"Timed out waiting for secret key: {SECRET_KEY_PATH}")

    try:
        existing_secret = _read_secret_key()
        if existing_secret:
            return existing_secret

        secret = secrets.token_hex(32)
        fd = os.open(SECRET_KEY_PATH, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(secret)
        return secret
    finally:
        os.close(lock_fd)
        try:
            os.unlink(lock_path)
        except (FileNotFoundError, PermissionError):
            pass


def get_all_settings() -> dict:
    """Return all runtime settings merged with defaults."""
    defaults = {
        "bing_concurrency": BING_CONCURRENCY,
        "bing_delay_min": BING_DELAY_MIN,
        "bing_delay_max": BING_DELAY_MAX,
        "bing_results_per_page": BING_RESULTS_PER_PAGE,
        "search_ip_rotation_enabled": SEARCH_IP_ROTATION_ENABLED,
        "search_ip_family_mode": "both",
        "ddg_concurrency": DDG_CONCURRENCY,
        "ddg_delay_min": DDG_DELAY_MIN,
        "ddg_delay_max": DDG_DELAY_MAX,
        "outbound_ips": OUTBOUND_IPS,
        "rotation_candidate_ips": [],
        "rotation_network_interface": "",
        "sync_outbound_ips_from_candidates": True,
        "verify_concurrency": VERIFY_CONCURRENCY,
        "max_concurrent_requests": MAX_CONCURRENT_REQUESTS,
        "request_timeout": REQUEST_TIMEOUT,
        "crawl_delay": CRAWL_DELAY,
        "max_pages_per_domain": MAX_PAGES_PER_DOMAIN,
        "urls_per_batch": URLS_PER_BATCH,
        "dedup_across_campaigns": False,
        "verify_timeout": VERIFY_TIMEOUT,
        "smtp_ehlo_hostname": "",
        "smtp_mail_from": "",
        "robots_txt_mode": "soft",
        "openrouter_api_key": "",
        "openrouter_model": "openrouter/free",
    }
    settings = _load_settings()
    defaults.update(settings)
    return defaults


def get_runtime_paths() -> dict:
    return {
        "base_dir": BASE_DIR,
        "settings": SETTINGS_PATH,
        "database": DATABASE_PATH,
        "server_out_log": os.path.join(BASE_DIR, "server.out.log"),
        "server_err_log": os.path.join(BASE_DIR, "server.err.log"),
    }


# ─── Location Data ────────────────────────────────────────────────────

_locations_cache = None

def get_locations() -> dict:
    global _locations_cache
    if _locations_cache is None:
        with open(LOCATIONS_PATH, "r", encoding="utf-8") as f:
            _locations_cache = json.load(f)
    return _locations_cache


# ─── Disposable Domains ──────────────────────────────────────────────

_disposable_cache = None

def get_disposable_domains() -> set:
    global _disposable_cache
    if _disposable_cache is None:
        try:
            with open(DISPOSABLE_PATH, "r") as f:
                _disposable_cache = {line.strip().lower() for line in f if line.strip()}
        except FileNotFoundError:
            _disposable_cache = set()
    return _disposable_cache


# ─── Spam Trap Domains ───────────────────────────────────────────────

_spam_trap_cache = None

def get_spam_trap_domains() -> set:
    global _spam_trap_cache
    if _spam_trap_cache is None:
        try:
            with open(SPAM_TRAP_DOMAINS_PATH, "r") as f:
                _spam_trap_cache = {
                    line.strip().lower()
                    for line in f
                    if line.strip() and not line.strip().startswith("#")
                }
        except FileNotFoundError:
            _spam_trap_cache = set()
    return _spam_trap_cache
