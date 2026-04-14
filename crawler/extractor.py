"""
Email extraction from HTML — regex + mailto link parsing.
"""
import re
from bs4 import BeautifulSoup
import tldextract
import config

EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico", ".bmp"}


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
    for match in EMAIL_REGEX.findall(text):
        emails_found.add(match.lower())

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
