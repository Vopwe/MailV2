"""
Email verification — syntax, MX, SMTP, disposable + spam trap detection.
Fully parallel with asyncio.gather + domain MX cache.

SMTP-safe design:
- Per-MX-host rate limiting (max 3 concurrent, delay between checks)
- Connection reuse (batch RCPT TO checks per MX host)
- Proper EHLO hostname + valid MAIL FROM
- Backoff on temporary rejections (421/450/451)
- Reduced rate for major providers (Gmail, Outlook, Yahoo)

When port 25 is blocked, falls back to MX + DNS-based scoring.
"""
import re
import asyncio
import logging
import os
import socket
import threading
import uuid
import dns.resolver
import aiosmtplib
import config

logger = logging.getLogger(__name__)

EMAIL_PATTERN = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
)

# Public email providers — SKIP SMTP verification entirely.
# These don't allow RCPT TO checks for individual mailboxes.
# Mark as "valid" (domain is legit) with low mailbox confidence.
PUBLIC_PROVIDERS = {
    "gmail.com", "googlemail.com",
    "yahoo.com", "yahoo.co.uk", "yahoo.co.in", "yahoo.fr", "yahoo.ca",
    "ymail.com", "rocketmail.com",
    "outlook.com", "hotmail.com", "hotmail.co.uk", "hotmail.fr",
    "live.com", "live.co.uk", "msn.com",
    "aol.com", "aol.co.uk",
    "icloud.com", "me.com", "mac.com",
    "protonmail.com", "proton.me", "pm.me",
    "zoho.com", "zohomail.com",
    "yandex.com", "yandex.ru",
    "mail.com", "email.com",
    "gmx.com", "gmx.net", "gmx.de",
    "fastmail.com", "fastmail.fm",
    "tutanota.com", "tutamail.com", "tuta.io",
    "hey.com",
    "mail.ru", "inbox.ru", "list.ru", "bk.ru",
    "163.com", "126.com", "qq.com",
    "naver.com", "daum.net", "hanmail.net",
    "web.de", "t-online.de", "freenet.de",
    "orange.fr", "wanadoo.fr", "laposte.net", "sfr.fr",
    "btinternet.com", "sky.com", "virginmedia.com",
    "comcast.net", "att.net", "verizon.net", "sbcglobal.net",
    "cox.net", "charter.net", "earthlink.net",
    "bellsouth.net", "rogers.com", "shaw.ca", "telus.net",
    "optusnet.com.au", "bigpond.com", "bigpond.net.au",
}

# MX hosts that indicate business email (Google Workspace, Microsoft 365, etc.)
BUSINESS_MX_PATTERNS = [
    "google.com", "googlemail.com",
    "outlook.com", "microsoft.com",
    "pphosted.com", "mimecast",
    "messagelabs.com",
    "zoho.com",
    "emailsrvr.com",
    "secureserver.net",
]

# MX hosts for large providers (Google Workspace, M365) — still used by businesses
# These need slightly slower rate limiting even though personal accounts are skipped
MAJOR_MX_PATTERNS = [
    "google.com", "googlemail.com",     # Google Workspace
    "outlook.com", "microsoft.com",     # Microsoft 365
]

# ── Domain MX Cache ─────────────────────────────────────────────────
_mx_cache: dict[str, tuple[bool, str | None]] = {}
_mx_lock = threading.Lock()

# ── Catch-all cache ─────────────────────────────────────────────────
_catch_all_cache: dict[str, bool] = {}
_catch_all_lock = threading.Lock()

# ── Per-MX-host semaphores (rate limiting per mail server) ──────────
_mx_semaphores: dict[str, asyncio.Semaphore] = {}
_mx_sem_lock = asyncio.Lock()

# ── SMTP availability cache ─────────────────────────────────────────
_smtp_available: bool | None = None
_smtp_test_done = threading.Event()
_smtp_test_started = threading.Event()
SMTP_PROBE_HOSTS = (
    "gmail-smtp-in.l.google.com",
    "alt1.gmail-smtp-in.l.google.com",
    "aspmx.l.google.com",
)

# ── Cached sets for spam trap checks ────────────────────────────────
_safe_roles_set: set | None = None
_soft_risk_set: set | None = None

# ── VPS hostname (resolved once) ────────────────────────────────────
_ehlo_hostname: str | None = None
_mail_from_address: str | None = None


def _smtp_identity_setting(key: str) -> str:
    for env_name in (f"GM_{key.upper()}", key.upper()):
        value = os.getenv(env_name, "").strip()
        if value:
            return value
    return str(config.get_setting(key.lower(), "") or "").strip()


def _clean_hostname(value: str) -> str:
    return value.strip().strip(".").lower()


def _looks_like_hostname(value: str) -> bool:
    value = _clean_hostname(value)
    if not value or any(ch.isspace() for ch in value):
        return False
    if any(ch in "[]:/" for ch in value):
        return False
    return True


def _looks_like_fqdn(value: str) -> bool:
    value = _clean_hostname(value)
    return _looks_like_hostname(value) and "." in value


def _get_ehlo_hostname() -> str:
    """Get a stable EHLO hostname without relying on IPv4 probes."""
    global _ehlo_hostname
    if _ehlo_hostname is None:
        configured = _smtp_identity_setting("smtp_ehlo_hostname")
        for candidate in (configured,):
            cleaned = _clean_hostname(candidate)
            if _looks_like_hostname(cleaned):
                _ehlo_hostname = cleaned
                break
        if _ehlo_hostname is None:
            for candidate in (socket.getfqdn(), socket.gethostname()):
                cleaned = _clean_hostname(candidate)
                if _looks_like_hostname(cleaned):
                    _ehlo_hostname = cleaned
                    break
        if _ehlo_hostname is None:
            _ehlo_hostname = "localhost"
    return _ehlo_hostname


def _get_mail_from_address() -> str:
    """Get a MAIL FROM identity decoupled from EHLO/IP selection."""
    global _mail_from_address
    if _mail_from_address is None:
        configured = _smtp_identity_setting("smtp_mail_from")
        if configured and "@" in configured and " " not in configured:
            _mail_from_address = configured
        else:
            ehlo_host = _get_ehlo_hostname()
            if _looks_like_fqdn(ehlo_host):
                _mail_from_address = f"verify@{ehlo_host}"
            else:
                _mail_from_address = "postmaster@localhost.localdomain"
    return _mail_from_address


def _result_template() -> dict:
    return {
        "verification": "unknown",
        "verification_method": "pending",
        "mailbox_confidence": "unknown",
        "domain_confidence": "unknown",
        "mx_valid": None,
        "smtp_valid": None,
        "is_catch_all": 0,
    }


def _prefix_matches(local_part: str, prefix: str) -> bool:
    return local_part == prefix or (
        local_part.startswith(prefix)
        and (len(local_part) == len(prefix) or not local_part[len(prefix)].isalpha())
    )


def _is_major_mx(mx_host: str) -> bool:
    """Check if an MX host belongs to a major provider (needs slower rate)."""
    mx_lower = mx_host.lower()
    return any(pattern in mx_lower for pattern in MAJOR_MX_PATTERNS)


async def _get_mx_semaphore(mx_host: str) -> asyncio.Semaphore:
    """Get or create a per-MX-host semaphore. Major providers get lower concurrency."""
    mx_key = mx_host.lower().rstrip(".")
    async with _mx_sem_lock:
        if mx_key not in _mx_semaphores:
            if _is_major_mx(mx_key):
                _mx_semaphores[mx_key] = asyncio.Semaphore(2)  # Max 2 for Gmail/Outlook/Yahoo
            else:
                _mx_semaphores[mx_key] = asyncio.Semaphore(3)  # Max 3 for others
        return _mx_semaphores[mx_key]


def _get_mx_cached(domain: str) -> tuple[bool, str | None]:
    """Check MX with per-domain caching."""
    with _mx_lock:
        if domain in _mx_cache:
            return _mx_cache[domain]

    result = _check_mx_raw(domain)

    with _mx_lock:
        _mx_cache[domain] = result
    return result


def _check_mx_raw(domain: str) -> tuple[bool, str | None]:
    """Raw MX lookup. Returns (has_mx, best_mx_host)."""
    try:
        answers = dns.resolver.resolve(domain, "MX")
        if answers:
            best = sorted(answers, key=lambda r: r.preference)[0]
            return True, str(best.exchange).rstrip(".")
        return False, None
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers,
            dns.exception.DNSException, dns.exception.Timeout):
        return False, None


def _check_domain_a_record(domain: str) -> bool:
    """Check if domain has an A or AAAA record (website exists)."""
    for record_type in ("A", "AAAA"):
        try:
            dns.resolver.resolve(domain, record_type)
            return True
        except Exception:
            continue
    return False


def _probe_smtp_connectivity(
    hosts: tuple[str, ...] = SMTP_PROBE_HOSTS,
    timeout: float = 5.0,
) -> bool:
    for host in hosts:
        try:
            with socket.create_connection((host, 25), timeout=timeout):
                return True
        except OSError as exc:
            logger.debug("SMTP probe failed for %s: %s", host, exc)
    return False


async def _test_smtp_availability() -> bool:
    """Test outbound SMTP reachability with a lightweight TCP probe."""
    global _smtp_available

    if _smtp_available is not None:
        return _smtp_available

    if _smtp_test_started.is_set():
        _smtp_test_done.wait(timeout=15)
        return _smtp_available if _smtp_available is not None else False

    _smtp_test_started.set()
    try:
        _smtp_available = await asyncio.to_thread(_probe_smtp_connectivity)
        if _smtp_available:
            logger.info("SMTP port 25 is reachable - full verification available")
        else:
            logger.info("SMTP connectivity check failed - using MX + DNS-based verification")
        return _smtp_available
    finally:
        _smtp_test_done.set()
    return _smtp_available


def check_syntax(email: str) -> bool:
    if not EMAIL_PATTERN.match(email):
        return False
    local, domain = email.split("@")
    if len(local) > 64 or len(domain) > 253:
        return False
    if ".." in email or local.startswith(".") or local.endswith("."):
        return False
    return True


def check_disposable(domain: str) -> bool:
    """Returns True if domain is disposable."""
    disposable = config.get_disposable_domains()
    return domain.lower() in disposable


def check_spam_trap(email: str, domain: str) -> str | None:
    """
    Heuristic spam trap detection.
    Returns "spam_trap", "risky", or None.
    """
    global _safe_roles_set, _soft_risk_set
    local_part = email.split("@")[0].lower()
    domain_lower = domain.lower()
    if _safe_roles_set is None:
        _safe_roles_set = set(config.SAFE_ROLE_PREFIXES)
    if _soft_risk_set is None:
        _soft_risk_set = set(config.SOFT_RISK_PREFIXES)
    safe_roles = _safe_roles_set
    soft_risk_prefixes = _soft_risk_set

    # 1. Known spam trap domains
    trap_domains = config.get_spam_trap_domains()
    if domain_lower in trap_domains:
        return "spam_trap"

    if local_part in safe_roles:
        return None

    # 2. Known trap prefixes
    for prefix in config.SPAM_TRAP_PREFIXES:
        if _prefix_matches(local_part, prefix):
            return "spam_trap"

    for prefix in soft_risk_prefixes:
        if _prefix_matches(local_part, prefix):
            return "risky"

    # 3. Suspicious patterns
    stripped = local_part.replace(".", "").replace("_", "").replace("-", "")

    if stripped.isdigit() and len(stripped) >= 5:
        return "risky"

    if len(stripped) >= 10 and all(c in "0123456789abcdef" for c in stripped):
        return "risky"

    keyboard_patterns = [
        "asdfgh", "qwerty", "zxcvbn", "poiuyt", "lkjhgf",
        "mnbvcx", "ytrewq", "fghjkl", "abcdef", "aaaaaa",
        "bbbbbb", "cccccc", "xxxxxx", "zzzzzz",
    ]
    for pat in keyboard_patterns:
        if pat in stripped:
            return "risky"

    if len(stripped) >= 6:
        prev = ""
        count = 0
        for c in stripped:
            if c == prev:
                count += 1
                if count >= 4:
                    return "risky"
            else:
                prev = c
                count = 1

    return None


async def _smtp_check_single(email: str, mx_host: str) -> str:
    """
    Single SMTP verification with proper EHLO, valid MAIL FROM, and backoff.
    Returns 'valid', 'invalid', or 'unknown'.
    """
    timeout = int(config.get_setting("verify_timeout", config.VERIFY_TIMEOUT))
    ehlo_host = _get_ehlo_hostname()

    # Per-MX rate limiting
    mx_sem = await _get_mx_semaphore(mx_host)

    async with mx_sem:
        # Delay between checks to same MX host (prevents burst)
        delay = 1.5 if _is_major_mx(mx_host) else 0.5
        await asyncio.sleep(delay)

        try:
            smtp = aiosmtplib.SMTP(
                hostname=mx_host,
                port=25,
                timeout=timeout,
            )
            await smtp.connect()

            # Proper EHLO with real hostname
            code, _ = await smtp.execute_command(f"EHLO {ehlo_host}".encode())
            if code >= 500:
                # Fallback to HELO
                code, _ = await smtp.execute_command(f"HELO {ehlo_host}".encode())

            # MAIL FROM should be a stable mailbox identity, not an IP literal.
            mail_from = _get_mail_from_address()
            code, _ = await smtp.execute_command(f"MAIL FROM:<{mail_from}>".encode())
            if code >= 500:
                try:
                    await smtp.quit()
                except Exception:
                    pass
                return "unknown"

            # RCPT TO — the actual mailbox check
            code, message = await smtp.execute_command(f"RCPT TO:<{email}>".encode())

            try:
                await smtp.quit()
            except Exception:
                pass

            if code == 250:
                return "valid"
            elif code in (550, 551, 552, 553, 554):
                return "invalid"
            elif code in (421, 450, 451, 452):
                # Temporary rejection — greylisting or rate limit
                logger.debug(f"Temp rejection {code} for {email} via {mx_host}: {message}")
                return "unknown"
            else:
                return "unknown"

        except (aiosmtplib.SMTPException, asyncio.TimeoutError, OSError, Exception) as e:
            logger.debug(f"SMTP check failed for {email} via {mx_host}: {e}")
            return "unknown"


async def _smtp_batch_check(emails: list[str], mx_host: str) -> dict[str, str]:
    """
    Batch RCPT TO checks on a single SMTP connection for emails sharing the same MX.
    Falls back to individual checks if connection reuse fails.
    Returns {email: 'valid'|'invalid'|'unknown'}.
    """
    if len(emails) <= 1:
        # Not worth batching for single emails
        if emails:
            result = await _smtp_check_single(emails[0], mx_host)
            return {emails[0]: result}
        return {}

    timeout = int(config.get_setting("verify_timeout", config.VERIFY_TIMEOUT))
    ehlo_host = _get_ehlo_hostname()
    mx_sem = await _get_mx_semaphore(mx_host)
    results = {}

    delay = 1.5 if _is_major_mx(mx_host) else 0.5

    async with mx_sem:
        await asyncio.sleep(delay)

        try:
            smtp = aiosmtplib.SMTP(
                hostname=mx_host,
                port=25,
                timeout=timeout,
            )
            await smtp.connect()

            code, _ = await smtp.execute_command(f"EHLO {ehlo_host}".encode())
            if code >= 500:
                code, _ = await smtp.execute_command(f"HELO {ehlo_host}".encode())

            mail_from = _get_mail_from_address()
            code, _ = await smtp.execute_command(f"MAIL FROM:<{mail_from}>".encode())
            if code >= 500:
                try:
                    await smtp.quit()
                except Exception:
                    pass
                # Fall back to individual checks
                for email in emails:
                    results[email] = await _smtp_check_single(email, mx_host)
                return results

            # Batch RCPT TO checks — max 10 per connection to avoid suspicion
            batch_size = 5 if _is_major_mx(mx_host) else 10
            for i, email in enumerate(emails[:batch_size]):
                code, message = await smtp.execute_command(f"RCPT TO:<{email}>".encode())

                if code == 250:
                    results[email] = "valid"
                elif code in (550, 551, 552, 553, 554):
                    results[email] = "invalid"
                elif code in (421, 450, 451, 452):
                    results[email] = "unknown"
                    # Server is pushing back — stop batching, RSET and quit
                    if code == 421:
                        break
                else:
                    results[email] = "unknown"

                # Small delay between RCPT TO commands
                await asyncio.sleep(0.2)

            # RSET before quit to be polite
            try:
                await smtp.execute_command(b"RSET")
                await smtp.quit()
            except Exception:
                pass

        except (aiosmtplib.SMTPException, asyncio.TimeoutError, OSError, Exception) as e:
            logger.debug(f"Batch SMTP failed for {mx_host}: {e}")

    # Any emails not checked in the batch — do individually
    for email in emails:
        if email not in results:
            results[email] = await _smtp_check_single(email, mx_host)

    return results


async def detect_catch_all(domain: str, mx_host: str) -> bool:
    """Check if a domain is catch-all. Cached per domain."""
    with _catch_all_lock:
        if domain in _catch_all_cache:
            return _catch_all_cache[domain]

    probe_email = f"__gm_probe_{uuid.uuid4().hex[:8]}@{domain}"
    is_catch_all = await _smtp_check_single(probe_email, mx_host) == "valid"

    with _catch_all_lock:
        _catch_all_cache[domain] = is_catch_all
    return is_catch_all


def _dns_based_verify(email: str, domain: str, mx_host: str | None) -> dict:
    """Fallback verification when port 25 is blocked."""
    result = _result_template()
    result["verification_method"] = "dns"
    result["mx_valid"] = 1 if mx_host else 0
    result["smtp_valid"] = None

    # Public providers already handled before this function is called
    if domain.lower() in PUBLIC_PROVIDERS:
        result["verification"] = "valid"
        result["verification_method"] = "public_provider"
        result["domain_confidence"] = "high"
        result["mailbox_confidence"] = "low"
        return result

    if not mx_host:
        result["verification"] = "invalid"
        result["domain_confidence"] = "low"
        return result

    mx_lower = mx_host.lower()

    for pattern in BUSINESS_MX_PATTERNS:
        if pattern in mx_lower:
            result["verification"] = "risky"
            result["verification_method"] = "dns_business_mx"
            result["domain_confidence"] = "high"
            return result

    has_website = _check_domain_a_record(domain)

    if has_website:
        result["verification"] = "risky"
        result["verification_method"] = "dns_domain"
        result["domain_confidence"] = "medium"
        return result

    result["verification"] = "risky"
    result["verification_method"] = "dns_mx_only"
    result["domain_confidence"] = "low"
    return result


async def verify_email(email: str, smtp_result_override: str | None = None) -> dict:
    """
    Full verification pipeline for a single email.
    If smtp_result_override is provided, skips the SMTP call (used by batch mode).
    """
    result = _result_template()

    # Stage 1: Syntax
    if not check_syntax(email):
        result["verification"] = "invalid"
        result["verification_method"] = "syntax"
        result["mailbox_confidence"] = "low"
        return result

    domain = email.split("@")[1]

    # Stage 2: Disposable check
    if check_disposable(domain):
        result["verification"] = "risky"
        result["verification_method"] = "heuristic_disposable"
        result["domain_confidence"] = "low"
        return result

    # Stage 2.5: Spam trap heuristics
    trap_result = check_spam_trap(email, domain)
    if trap_result == "spam_trap":
        result["verification"] = "spam_trap"
        result["verification_method"] = "heuristic_spam_trap"
        result["mailbox_confidence"] = "low"
        return result
    if trap_result == "risky":
        result["verification"] = "risky"
        result["verification_method"] = "heuristic_risky_local"
        result["mailbox_confidence"] = "low"
        return result

    # Stage 3: Public provider — skip SMTP, domain is legit
    if domain.lower() in PUBLIC_PROVIDERS:
        result["verification"] = "valid"
        result["verification_method"] = "public_provider"
        result["domain_confidence"] = "high"
        result["mailbox_confidence"] = "low"
        result["mx_valid"] = 1
        return result

    # Stage 4: MX check (cached per domain)
    has_mx, mx_host = _get_mx_cached(domain)
    result["mx_valid"] = 1 if has_mx else 0

    if not has_mx:
        result["verification"] = "invalid"
        result["verification_method"] = "dns_no_mx"
        result["domain_confidence"] = "low"
        return result

    # Stage 5: SMTP or DNS-based verification
    smtp_ok = await _test_smtp_availability()

    if smtp_ok:
        # Use pre-computed SMTP result if available (from batch mode)
        if smtp_result_override is not None:
            smtp_result = smtp_result_override
        else:
            smtp_result = await _smtp_check_single(email, mx_host)

        result["smtp_valid"] = 1 if smtp_result == "valid" else (0 if smtp_result == "invalid" else None)
        result["verification_method"] = "smtp"
        result["domain_confidence"] = "high"

        if smtp_result == "valid":
            if await detect_catch_all(domain, mx_host):
                result["verification"] = "risky"
                result["verification_method"] = "smtp_catch_all"
                result["mailbox_confidence"] = "low"
                result["is_catch_all"] = 1
            else:
                result["verification"] = "valid"
                result["mailbox_confidence"] = "high"
        elif smtp_result == "invalid":
            result["verification"] = "invalid"
            result["mailbox_confidence"] = "high"
        else:
            result["verification"] = "unknown"
    else:
        dns_result = _dns_based_verify(email, domain, mx_host)
        result.update(dns_result)

    return result


def _new_verify_stats() -> dict:
    """Template for verification run statistics."""
    return {
        "total": 0,
        "smtp_available": None,
        # Result counts
        "result_valid": 0,
        "result_invalid": 0,
        "result_risky": 0,
        "result_spam_trap": 0,
        "result_unknown": 0,
        # Method counts (how the decision was made)
        "method_syntax": 0,
        "method_disposable": 0,
        "method_spam_trap": 0,
        "method_risky_local": 0,
        "method_public_provider": 0,
        "method_dns_no_mx": 0,
        "method_smtp": 0,
        "method_smtp_catch_all": 0,
        "method_dns_business_mx": 0,
        "method_dns_domain": 0,
        "method_dns_mx_only": 0,
        # SMTP stats
        "smtp_checked": 0,
        "smtp_valid": 0,
        "smtp_invalid": 0,
        "smtp_unknown": 0,
        "smtp_catch_all_domains": 0,
        # Domain stats
        "unique_domains": 0,
        "unique_mx_hosts": 0,
        "public_provider_count": 0,
        "no_mx_count": 0,
    }


async def verify_emails_batch(emails: list[dict], on_progress=None) -> tuple[list[dict], dict]:
    """
    Verify a batch of email records using grouped batch SMTP checks.
    Groups emails by MX host for connection reuse, then verifies in parallel.
    Returns (results, verify_stats).
    """
    concurrency = int(config.get_setting("verify_concurrency", config.VERIFY_CONCURRENCY))
    results = []
    counter = {"done": 0, "valid": 0, "invalid": 0, "risky": 0, "spam_trap": 0, "unknown": 0}
    total = len(emails)
    lock = asyncio.Lock()
    vstats = _new_verify_stats()
    vstats["total"] = total

    # Pre-test SMTP availability
    smtp_ok = await _test_smtp_availability()
    vstats["smtp_available"] = smtp_ok

    # Track unique domains/mx hosts
    seen_domains = set()
    seen_mx_hosts = set()

    # ── Phase 1: Pre-filter (syntax, disposable, spam trap, MX) ──────
    # These don't need SMTP at all
    smtp_needed = []  # Records that need SMTP checking
    for record in emails:
        email = record["email"]
        r = _result_template()

        if not check_syntax(email):
            r["verification"] = "invalid"
            r["verification_method"] = "syntax"
            r["mailbox_confidence"] = "low"
            r["id"] = record["id"]
            results.append(r)
            vstats["method_syntax"] += 1
            vstats["result_invalid"] += 1
            async with lock:
                counter["done"] += 1
                counter["invalid"] += 1
                if on_progress:
                    on_progress(counter["done"], total, counter)
            continue

        domain = email.split("@")[1]
        seen_domains.add(domain)

        if check_disposable(domain):
            r["verification"] = "risky"
            r["verification_method"] = "heuristic_disposable"
            r["domain_confidence"] = "low"
            r["id"] = record["id"]
            results.append(r)
            vstats["method_disposable"] += 1
            vstats["result_risky"] += 1
            async with lock:
                counter["done"] += 1
                counter["risky"] += 1
                if on_progress:
                    on_progress(counter["done"], total, counter)
            continue

        trap = check_spam_trap(email, domain)
        if trap == "spam_trap":
            r["verification"] = "spam_trap"
            r["verification_method"] = "heuristic_spam_trap"
            r["mailbox_confidence"] = "low"
            r["id"] = record["id"]
            results.append(r)
            vstats["method_spam_trap"] += 1
            vstats["result_spam_trap"] += 1
            async with lock:
                counter["done"] += 1
                counter["spam_trap"] += 1
                if on_progress:
                    on_progress(counter["done"], total, counter)
            continue
        if trap == "risky":
            r["verification"] = "risky"
            r["verification_method"] = "heuristic_risky_local"
            r["mailbox_confidence"] = "low"
            r["id"] = record["id"]
            results.append(r)
            vstats["method_risky_local"] += 1
            vstats["result_risky"] += 1
            async with lock:
                counter["done"] += 1
                counter["risky"] += 1
                if on_progress:
                    on_progress(counter["done"], total, counter)
            continue

        # Public providers — skip SMTP entirely, mark as valid (domain is legit)
        if domain.lower() in PUBLIC_PROVIDERS:
            r["verification"] = "valid"
            r["verification_method"] = "public_provider"
            r["domain_confidence"] = "high"
            r["mailbox_confidence"] = "low"
            r["mx_valid"] = 1
            r["id"] = record["id"]
            results.append(r)
            vstats["method_public_provider"] += 1
            vstats["public_provider_count"] += 1
            vstats["result_valid"] += 1
            async with lock:
                counter["done"] += 1
                counter["valid"] += 1
                if on_progress:
                    on_progress(counter["done"], total, counter)
            continue

        has_mx, mx_host = _get_mx_cached(domain)
        if not has_mx:
            r["verification"] = "invalid"
            r["verification_method"] = "dns_no_mx"
            r["mx_valid"] = 0
            r["domain_confidence"] = "low"
            r["id"] = record["id"]
            results.append(r)
            vstats["method_dns_no_mx"] += 1
            vstats["no_mx_count"] += 1
            vstats["result_invalid"] += 1
            async with lock:
                counter["done"] += 1
                counter["invalid"] += 1
                if on_progress:
                    on_progress(counter["done"], total, counter)
            continue

        if mx_host:
            seen_mx_hosts.add(mx_host)
        smtp_needed.append({"record": record, "domain": domain, "mx_host": mx_host})

    if not smtp_needed:
        vstats["unique_domains"] = len(seen_domains)
        vstats["unique_mx_hosts"] = len(seen_mx_hosts)
        return results, vstats

    # ── Phase 2: SMTP verification (grouped by MX host) ─────────────
    if smtp_ok:
        # Group emails by MX host for batch checking
        mx_groups: dict[str, list[dict]] = {}
        for item in smtp_needed:
            mx = item["mx_host"]
            if mx not in mx_groups:
                mx_groups[mx] = []
            mx_groups[mx].append(item)

        # Process each MX group with connection reuse
        global_sem = asyncio.Semaphore(concurrency)

        async def process_mx_group(mx_host: str, items: list[dict]):
            async with global_sem:
                email_list = [it["record"]["email"] for it in items]
                smtp_results = await _smtp_batch_check(email_list, mx_host)

                for item in items:
                    email = item["record"]["email"]
                    domain = item["domain"]
                    smtp_result = smtp_results.get(email, "unknown")

                    r = _result_template()
                    r["id"] = item["record"]["id"]
                    r["mx_valid"] = 1
                    r["smtp_valid"] = 1 if smtp_result == "valid" else (0 if smtp_result == "invalid" else None)
                    r["verification_method"] = "smtp"
                    r["domain_confidence"] = "high"

                    if smtp_result == "valid":
                        if await detect_catch_all(domain, mx_host):
                            r["verification"] = "risky"
                            r["verification_method"] = "smtp_catch_all"
                            r["mailbox_confidence"] = "low"
                            r["is_catch_all"] = 1
                        else:
                            r["verification"] = "valid"
                            r["mailbox_confidence"] = "high"
                    elif smtp_result == "invalid":
                        r["verification"] = "invalid"
                        r["mailbox_confidence"] = "high"
                    else:
                        r["verification"] = "unknown"

                    async with lock:
                        results.append(r)
                        counter["done"] += 1
                        # Track verification stats
                        vstats["smtp_checked"] += 1
                        if smtp_result == "valid":
                            vstats["smtp_valid"] += 1
                        elif smtp_result == "invalid":
                            vstats["smtp_invalid"] += 1
                        else:
                            vstats["smtp_unknown"] += 1
                        method_key = f"method_{r['verification_method']}"
                        if method_key in vstats:
                            vstats[method_key] += 1
                        if r.get("is_catch_all"):
                            vstats["smtp_catch_all_domains"] += 1
                        v = r["verification"]
                        if v == "valid":
                            counter["valid"] += 1
                            vstats["result_valid"] += 1
                        elif v == "invalid":
                            counter["invalid"] += 1
                            vstats["result_invalid"] += 1
                        elif v == "risky":
                            counter["risky"] += 1
                            vstats["result_risky"] += 1
                        elif v == "spam_trap":
                            counter["spam_trap"] += 1
                            vstats["result_spam_trap"] += 1
                        else:
                            counter["unknown"] += 1
                            vstats["result_unknown"] += 1
                        if on_progress:
                            on_progress(counter["done"], total, counter)

        group_tasks = [
            process_mx_group(mx_host, items)
            for mx_host, items in mx_groups.items()
        ]
        await asyncio.gather(*group_tasks)

    else:
        # DNS-only fallback
        for item in smtp_needed:
            r = _dns_based_verify(item["record"]["email"], item["domain"], item["mx_host"])
            r["id"] = item["record"]["id"]
            method_key = f"method_{r['verification_method']}"
            if method_key in vstats:
                vstats[method_key] += 1
            async with lock:
                results.append(r)
                counter["done"] += 1
                v = r["verification"]
                if v == "valid":
                    counter["valid"] += 1
                    vstats["result_valid"] += 1
                elif v == "invalid":
                    counter["invalid"] += 1
                    vstats["result_invalid"] += 1
                elif v == "risky":
                    counter["risky"] += 1
                    vstats["result_risky"] += 1
                else:
                    counter["unknown"] += 1
                    vstats["result_unknown"] += 1
                if on_progress:
                    on_progress(counter["done"], total, counter)

    vstats["unique_domains"] = len(seen_domains)
    vstats["unique_mx_hosts"] = len(seen_mx_hosts)
    return results, vstats


def clear_mx_cache():
    """Clear all caches between verification runs."""
    global _mx_cache, _smtp_available, _safe_roles_set, _soft_risk_set
    global _mx_semaphores, _ehlo_hostname, _mail_from_address
    with _mx_lock:
        _mx_cache.clear()
    with _catch_all_lock:
        _catch_all_cache.clear()
    _smtp_available = None
    _smtp_test_started.clear()
    _smtp_test_done.clear()
    _safe_roles_set = None
    _soft_risk_set = None
    _mx_semaphores = {}
    _ehlo_hostname = None
    _mail_from_address = None
