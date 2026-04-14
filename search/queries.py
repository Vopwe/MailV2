"""
Bing search query builder — generates diverse query variations
per niche/city/country for maximum URL yield.

Strategy: cast a WIDE net with many different query phrasings.
Bing returns ~10 organic results per page, so we need varied queries
to accumulate enough unique domains.
"""


def build_queries(niche: str, city: str, country: str, country_tld: str = ".com", count: int = 40) -> list[dict]:
    """
    Build diverse Bing search queries for a niche/city/country combo.
    Returns list of dicts: {"query": str, "results_needed": int}

    We generate 12-15+ query variations to maximize unique domain yield.
    Each Bing page gives ~8-10 organic results, and after domain dedup
    we keep ~60-70%, so we need many queries to hit the target count.
    """
    tld_clean = country_tld.lstrip(".")
    per_query = max(count, 20)  # target per query (will dedup later)

    queries = [
        # ── Direct business search ────────────────────────────────
        {
            "query": f'{niche} in {city} {country}',
            "results_needed": per_query,
        },
        {
            "query": f'{niche} {city} {country}',
            "results_needed": per_query,
        },
        # ── Contact / email intent ────────────────────────────────
        {
            "query": f'{niche} {city} contact email',
            "results_needed": per_query,
        },
        {
            "query": f'{niche} {city} contact us',
            "results_needed": per_query,
        },
        # ── Company list intent ───────────────────────────────────
        {
            "query": f'best {niche} in {city}',
            "results_needed": per_query,
        },
        {
            "query": f'top {niche} companies {city}',
            "results_needed": per_query,
        },
        {
            "query": f'{niche} companies in {city} {country}',
            "results_needed": per_query,
        },
        # ── Directory / listing intent ────────────────────────────
        {
            "query": f'{niche} directory {city} {country}',
            "results_needed": per_query,
        },
        {
            "query": f'{niche} near me {city}',
            "results_needed": per_query,
        },
        # ── Service intent ────────────────────────────────────────
        {
            "query": f'{niche} services {city}',
            "results_needed": per_query,
        },
        {
            "query": f'hire {niche} {city} {country}',
            "results_needed": per_query,
        },
        # ── Review / recommendation intent (different result set) ─
        {
            "query": f'{niche} {city} reviews',
            "results_needed": per_query,
        },
        # ── Professional / business intent ────────────────────────
        {
            "query": f'{niche} agency {city}',
            "results_needed": per_query,
        },
        {
            "query": f'{niche} firm {city} {country}',
            "results_needed": per_query,
        },
    ]

    # TLD-scoped queries (non-.com countries)
    if tld_clean != "com":
        queries.extend([
            {
                "query": f'{niche} {city} site:.{tld_clean}',
                "results_needed": per_query,
            },
            {
                "query": f'{niche} services site:.{tld_clean} {city}',
                "results_needed": per_query,
            },
        ])

    # Quoted variations (force exact match — different result set)
    queries.extend([
        {
            "query": f'"{niche}" "{city}" email',
            "results_needed": per_query,
        },
        {
            "query": f'"{niche}" "{city}" website',
            "results_needed": per_query,
        },
    ])

    return queries
