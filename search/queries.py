"""
Bing search query builder — generates multiple query variations
per niche/city/country for maximum URL diversity.
"""


def build_queries(niche: str, city: str, country: str, country_tld: str = ".com", count: int = 40) -> list[dict]:
    """
    Build diverse Bing search queries for a niche/city/country combo.
    Returns list of dicts: {"query": str, "results_needed": int}

    We over-generate queries so the scraper can stop early once `count` unique URLs are found.
    """
    tld_clean = country_tld.lstrip(".")

    queries = [
        # High-intent: contact/email pages
        {
            "query": f'"{niche}" "{city}" contact email',
            "results_needed": count,
        },
        # Company listings
        {
            "query": f'"{niche}" companies "{city}" {country}',
            "results_needed": count,
        },
        # Directory-style
        {
            "query": f'"{niche}" directory "{city}"',
            "results_needed": count,
        },
    ]

    # Add TLD-scoped query only if not generic .com
    if tld_clean != "com":
        queries.append({
            "query": f'"{niche}" "{city}" site:.{tld_clean}',
            "results_needed": count,
        })

    # Services-style query
    queries.append({
        "query": f'{niche} services in {city} {country} email',
        "results_needed": count,
    })

    return queries
