"""
OpenRouter AI URL generator — third source for URL generation.
Uses free models on OpenRouter to generate business website URLs
for a given niche/city/country.
"""
import json
import logging
import re

import httpx

import config

logger = logging.getLogger(__name__)

OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"

# Free models to try in order of preference
FREE_MODELS = [
    "google/gemma-3-1b-it:free",
    "meta-llama/llama-3.1-8b-instruct:free",
    "mistralai/mistral-small-3.1-24b-instruct:free",
    "google/gemma-3-4b-it:free",
]


def _get_api_key() -> str | None:
    """Get OpenRouter API key from settings."""
    return config.get_setting("openrouter_api_key", None)


def _get_model() -> str:
    """Get configured model or first free default."""
    return config.get_setting("openrouter_model", FREE_MODELS[0])


def _build_prompt(niche: str, city: str, country: str, count: int) -> str:
    """Build the URL generation prompt."""
    return f"""You are a business directory expert. Generate a list of {count} real website URLs for "{niche}" businesses located in or serving {city}, {country}.

Rules:
- Return ONLY actual business website URLs (not directories like Yelp, Google, Facebook, etc.)
- Each URL should be a different company/business
- Include the full URL starting with https://
- Focus on small-to-medium local businesses that are likely to have contact emails on their websites
- Return one URL per line, nothing else — no numbering, no descriptions, no markdown

Example output format:
https://www.smithplumbing.com
https://www.acmeroofing.com
https://www.citycleaners.net"""


async def generate_ai_urls(niche: str, city: str, country: str, count: int = 40) -> list[str]:
    """
    Generate business URLs using OpenRouter AI.
    Returns list of URLs. Returns empty list if no API key or on error.
    """
    api_key = _get_api_key()
    if not api_key:
        logger.debug("OpenRouter: no API key configured, skipping AI URL generation")
        return []

    model = _get_model()
    prompt = _build_prompt(niche, city, country, count)

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://graphenmail.app",
        "X-Title": "GraphenMail",
    }

    payload = {
        "model": model,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.7,
        "max_tokens": 2000,
    }

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            logger.info(f"OpenRouter AI: generating URLs with {model}")
            resp = await client.post(OPENROUTER_API_URL, json=payload, headers=headers)

            if resp.status_code != 200:
                logger.warning(f"OpenRouter returned {resp.status_code}: {resp.text[:200]}")
                return []

            data = resp.json()
            content = data["choices"][0]["message"]["content"]

            # Parse URLs from response
            urls = _parse_urls(content)
            logger.info(f"OpenRouter AI: generated {len(urls)} URLs")
            return urls

    except Exception as e:
        logger.error(f"OpenRouter AI error: {e}")
        return []


def _parse_urls(text: str) -> list[str]:
    """Extract valid URLs from AI response text."""
    urls = []
    # Match URLs in the text
    url_pattern = re.compile(r'https?://[^\s,\)\]\"\'>]+')
    for match in url_pattern.findall(text):
        url = match.rstrip(".,;:)")
        if "." in url and len(url) > 10:
            urls.append(url)
    return urls
