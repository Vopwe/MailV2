"""
Email extraction from HTML — regex + mailto link parsing.

Also handles obfuscated email patterns commonly used to dodge scrapers:
- `name [at] domain [dot] com` / `name(at)domain(dot)com`
- HTML entities like `name&#64;domain.com`
- Fullwidth unicode like `name＠domain．com`
"""
import html as html_lib
import re
from bs4 import BeautifulSoup
import tldextract
import config

EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# Obfuscated variants: [at]/[dot] and (at)/(dot) with optional whitespace
OBFUSCATED_PATTERNS = [
    re.compile(
        r"([a-zA-Z0-9._%+\-]+)\s*\[\s*(?:at|@)\s*\]\s*"
        r"([a-zA-Z0-9.\-]+(?:\s*\[\s*(?:dot|\.)\s*\]\s*[a-zA-Z0-9\-]+)+)",
        re.IGNORECASE,
    ),
    re.compile(
        r"([a-zA-Z0-9._%+\-]+)\s*\(\s*(?:at|@)\s*\)\s*"
        r"([a-zA-Z0-9.\-]+(?:\s*\(\s*(?:dot|\.)\s*\)\s*[a-zA-Z0-9\-]+)+)",
        re.IGNORECASE,
    ),
    # "name at domain dot com" — whitespace-separated
    re.compile(
        r"\b([a-zA-Z0-9._%+\-]+)\s+at\s+"
        r"([a-zA-Z0-9\-]+(?:\s+dot\s+[a-zA-Z0-9\-]+)+)\b",
        re.IGNORECASE,
    ),
]

# Fullwidth unicode -> ASCII
_UNICODE_NORMALIZE = {
    "\uFF20": "@",   # ＠
    "\uFF0E": ".",   # ．
    "\u2024": ".",   # ․
    "\uFE52": ".",   # ﹒
}

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico", ".bmp"}


def _deobfuscate_match(local: str, rest: str) -> str:
    """Turn 'name', 'domain [dot] com' -> 'name@domain.com'."""
    domain = re.sub(r"\s*\[\s*(?:dot|\.)\s*\]\s*", ".", rest, flags=re.IGNORECASE)
    domain = re.sub(r"\s*\(\s*(?:dot|\.)\s*\)\s*", ".", domain, flags=re.IGNORECASE)
    domain = re.sub(r"\s+dot\s+", ".", domain, flags=re.IGNORECASE)
    domain = domain.strip().strip(".")
    return f"{local.strip()}@{domain}".lower()


def _normalize_text(text: str) -> str:
    """Decode HTML entities and fold fullwidth unicode to ASCII."""
    decoded = html_lib.unescape(text)
    for src, dst in _UNICODE_NORMALIZE.items():
        decoded = decoded.replace(src, dst)
    return decoded


def extract_emails(html: str, source_url: str) -> list[dict]:
    """
    Extract emails from HTML content.
    Returns list of dicts with: email, domain, source_url, source_domain, is_generic
    """
    emails_found = set()
    soup = BeautifulSoup(html, "lxml")

    # 1. mailto: links (most reliable)
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if href.lower().startswith("mailto:"):
            email = href[7:].split("?")[0].split("&")[0].strip()
            if email:
                emails_found.add(email.lower())

    # 2. Regex on visible text (strip scripts/styles first)
    for tag in soup(["script", "style", "noscript", "meta", "link"]):
        tag.decompose()
    text = soup.get_text(separator=" ")

    # Normalize HTML entities + fullwidth unicode before regex
    normalized = _normalize_text(text)

    # 2a. Standard pattern on normalized text (also catches &#64; / ＠)
    for match in EMAIL_REGEX.findall(normalized):
        emails_found.add(match.lower())

    # 2b. Obfuscated patterns ([at]/[dot], (at)/(dot), spaced)
    for pattern in OBFUSCATED_PATTERNS:
        for local, rest in pattern.findall(normalized):
            candidate = _deobfuscate_match(local, rest)
            # Re-validate through standard regex to drop noise
            if EMAIL_REGEX.fullmatch(candidate):
                emails_found.add(candidate)

    # 3. Filter and build records
    source_ext = tldextract.extract(source_url)
    source_domain = f"{source_ext.domain}.{source_ext.suffix}"
    results = []

    for email in emails_found:
        # Skip image-like false positives
        if any(email.endswith(ext) for ext in IMAGE_EXTENSIONS):
            continue

        # Basic validation
        parts = email.split("@")
        if len(parts) != 2:
            continue
        local_part, email_domain = parts
        if not local_part or not email_domain or "." not in email_domain:
            continue
        if len(local_part) > 64 or len(email_domain) > 253:
            continue

        # Check if generic
        is_generic = 0
        if config.SKIP_GENERIC_EMAILS:
            for prefix in config.GENERIC_PREFIXES:
                if local_part.lower().startswith(prefix):
                    is_generic = 1
                    break

        results.append({
            "email": email,
            "domain": email_domain,
            "source_url": source_url,
            "source_domain": source_domain,
            "is_generic": is_generic,
        })

    return results
