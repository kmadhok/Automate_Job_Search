#!/usr/bin/env python3
"""Discover LinkedIn networking contacts by role/keyword theme."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

DEFAULT_INTERNAL_COMPANIES = ["Walmart", "Walmart Global Tech"]
DEFAULT_TITLE_TERMS = [
    "Data Scientist",
    "AI Engineer",
    "Machine Learning Engineer",
    "Data Analyst",
    "Applied Scientist",
]
DEFAULT_KEYWORDS = [
    "agents",
    "large language models",
    "llm",
    "applied ai",
    "production ai",
    "real-world ai",
]
DEFAULT_FOCUS_TERMS = [
    "customer impact",
    "supply chain",
    "optimization",
    "recommendation systems",
    "automation",
]

COMPANY_SUFFIXES = {
    "inc",
    "inc.",
    "llc",
    "ltd",
    "ltd.",
    "corp",
    "corp.",
    "co",
    "co.",
    "company",
    "technologies",
    "technology",
    "group",
    "holdings",
}


def parse_csv_arg(raw: str) -> List[str]:
    return [part.strip() for part in raw.split(",") if part.strip()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Find networking contacts from LinkedIn profile search for internal and external targets."
        )
    )
    parser.add_argument(
        "--internal-companies",
        default=os.getenv("NETWORK_INTERNAL_COMPANIES", ",".join(DEFAULT_INTERNAL_COMPANIES)),
        help="Comma-separated companies for internal networking search.",
    )
    parser.add_argument(
        "--external-companies",
        default=os.getenv("NETWORK_EXTERNAL_COMPANIES", ""),
        help=(
            "Optional comma-separated companies for external search. "
            "If omitted, runs one broad external query excluding internal company names."
        ),
    )
    parser.add_argument(
        "--title-terms",
        default=os.getenv("NETWORK_TITLE_TERMS", ",".join(DEFAULT_TITLE_TERMS)),
        help="Comma-separated role title terms to prioritize.",
    )
    parser.add_argument(
        "--keywords",
        default=os.getenv("NETWORK_KEYWORDS", ",".join(DEFAULT_KEYWORDS)),
        help="Comma-separated technical/domain keyword terms.",
    )
    parser.add_argument(
        "--focus-terms",
        default=os.getenv("NETWORK_FOCUS_TERMS", ",".join(DEFAULT_FOCUS_TERMS)),
        help="Comma-separated applied-problem focus terms.",
    )
    parser.add_argument(
        "--per-query",
        type=int,
        default=8,
        help="Number of contacts to keep per query.",
    )
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Fetch profile candidates from Serper.dev API; otherwise output query-only report.",
    )
    parser.add_argument(
        "--serper-api-key",
        default=os.getenv("SERPER_DEV_API_KEY", "") or os.getenv("SERPER_API_KEY", ""),
        help="Serper API key (from SERPER_DEV_API_KEY or SERPER_API_KEY).",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=12,
        help="Max raw results fetched from Serper for each query (1-20).",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Output JSON path (default: output/networking_targets_<timestamp>.json).",
    )
    parser.add_argument(
        "--output-csv",
        default="",
        help="Output CSV path (default: output/networking_targets_<timestamp>.csv).",
    )
    return parser.parse_args()


def text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "; ".join(text(v) for v in value)
    return str(value)


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def normalize_company(company: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9 ]", " ", company.lower())
    return normalize_whitespace(cleaned)


def company_aliases(company: str) -> List[str]:
    base = normalize_company(company)
    if not base:
        return []

    tokens = [token for token in base.split() if token]
    aliases = {base}

    trimmed = [token for token in tokens if token not in COMPANY_SUFFIXES]
    if trimmed:
        aliases.add(" ".join(trimmed))

    geography_suffixes = {"north", "south", "east", "west", "america", "americas", "us", "usa"}
    geo_trimmed = list(trimmed or tokens)
    while geo_trimmed and geo_trimmed[-1] in geography_suffixes:
        geo_trimmed.pop()
    if geo_trimmed:
        aliases.add(" ".join(geo_trimmed))

    return [alias for alias in aliases if alias]


def is_company_match(company: str, text_blob: str) -> bool:
    haystack = normalize_company(text_blob)
    if not haystack:
        return False

    for alias in company_aliases(company):
        if re.search(rf"\b{re.escape(alias)}\b", haystack):
            return True
    return False


def quoted_or_clause(terms: List[str]) -> str:
    clean_terms = [normalize_whitespace(term) for term in terms if normalize_whitespace(term)]
    if not clean_terms:
        return '"AI"'
    if len(clean_terms) == 1:
        return f'"{clean_terms[0]}"'
    return "(" + " OR ".join(f'"{term}"' for term in clean_terms) + ")"


def build_search_groups(
    internal_companies: List[str],
    external_companies: List[str],
    title_terms: List[str],
    keywords: List[str],
    focus_terms: List[str],
) -> List[Dict[str, str]]:
    title_clause = quoted_or_clause(title_terms)
    keyword_clause = quoted_or_clause(keywords)
    focus_clause = quoted_or_clause(focus_terms)

    groups: List[Dict[str, str]] = []
    for company in internal_companies:
        groups.append(
            {
                "scope": "internal",
                "label": f"internal:{company}",
                "target_company": company,
                "query": (
                    f'site:linkedin.com/in "{company}" {title_clause} '
                    f'({keyword_clause} OR {focus_clause}) '
                    '("building" OR "applied" OR "production")'
                ),
            }
        )

    if external_companies:
        for company in external_companies:
            groups.append(
                {
                    "scope": "external_company",
                    "label": f"external_company:{company}",
                    "target_company": company,
                    "query": (
                        f'site:linkedin.com/in "{company}" {title_clause} '
                        f'({keyword_clause} OR {focus_clause}) '
                        '("building" OR "applied" OR "production")'
                    ),
                }
            )
    else:
        excluded = " ".join(f'-"{name}"' for name in internal_companies)
        groups.append(
            {
                "scope": "external_broad",
                "label": "external_broad",
                "target_company": "",
                "query": (
                    f"site:linkedin.com/in {title_clause} ({keyword_clause} OR {focus_clause}) "
                    f"{excluded}"
                ).strip(),
            }
        )

    return groups


def serper_search(query: str, api_key: str, num: int = 10) -> List[Dict[str, Any]]:
    body = json.dumps(
        {
            "q": query,
            "num": max(1, min(num, 20)),
            "gl": "us",
            "hl": "en",
        }
    ).encode("utf-8")
    request = Request(
        url="https://google.serper.dev/search",
        data=body,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-API-KEY": api_key,
        },
        method="POST",
    )
    with urlopen(request, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return payload.get("organic", []) or []


def extract_name_from_title(title: str) -> str:
    cleaned = title.replace("| LinkedIn", "").strip()
    if " - " in cleaned:
        cleaned = cleaned.split(" - ", 1)[0].strip()
    elif " | " in cleaned:
        cleaned = cleaned.split(" | ", 1)[0].strip()

    tokens = cleaned.split()
    if len(tokens) < 2 or len(tokens) > 6:
        return ""
    if any(ch.isdigit() for ch in cleaned):
        return ""
    return cleaned


def extract_name_from_link(link: str) -> str:
    path = urlsplit(link).path
    match = re.search(r"/in/([^/]+)/?", path)
    if not match:
        return ""
    slug = match.group(1)
    parts = [p for p in re.split(r"[-_]+", slug) if p and p.isalpha()]
    if len(parts) < 2:
        return ""
    return " ".join(part.capitalize() for part in parts[:4])


def infer_current_company(title: str, snippet: str) -> str:
    combined = normalize_whitespace(f"{title} {snippet}")
    match = re.search(r"\bat\s+([A-Za-z0-9&.,\- ]{2,80})", combined)
    if not match:
        return ""
    company = normalize_whitespace(match.group(1).strip(".,;- "))
    if len(company.split()) > 8:
        return ""
    return company


def matched_terms(terms: List[str], text_blob: str) -> List[str]:
    haystack = text_blob.lower()
    hits = []
    for term in terms:
        low = term.lower().strip()
        if low and low in haystack:
            hits.append(term)
    return hits


def score_candidate(
    rank: int,
    title_hits: List[str],
    keyword_hits: List[str],
    focus_hits: List[str],
    company_match: bool,
) -> int:
    score = max(0, 120 - (rank * 3))
    score += min(4, len(title_hits)) * 8
    score += min(5, len(keyword_hits)) * 7
    score += min(3, len(focus_hits)) * 5
    if company_match:
        score += 20
    return score


def keep_result_for_scope(scope: str, combined: str, target_company: str, internal_companies: List[str]) -> bool:
    if scope in {"internal", "external_company"}:
        return is_company_match(target_company, combined)

    if scope == "external_broad":
        for company in internal_companies:
            if is_company_match(company, combined):
                return False
        return True

    return True


def select_candidates(
    results: List[Dict[str, Any]],
    group: Dict[str, str],
    title_terms: List[str],
    keywords: List[str],
    focus_terms: List[str],
    internal_companies: List[str],
    top_n: int,
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    seen = set()

    for rank, item in enumerate(results, start=1):
        link = text(item.get("url") or item.get("link")).strip()
        if "linkedin.com/in/" not in link:
            continue

        title = text(item.get("title"))
        snippet = text(item.get("description") or item.get("snippet"))
        combined = normalize_whitespace(f"{title} {snippet}")

        if not keep_result_for_scope(group["scope"], combined, group["target_company"], internal_companies):
            continue

        title_hits = matched_terms(title_terms, combined)
        keyword_hits = matched_terms(keywords, combined)
        focus_hits = matched_terms(focus_terms, combined)
        if not title_hits and not keyword_hits and not focus_hits:
            continue

        name = extract_name_from_title(title) or extract_name_from_link(link)
        if not name:
            continue

        dedupe_key = link.split("?", 1)[0].rstrip("/")
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        company_match = False
        if group["target_company"]:
            company_match = is_company_match(group["target_company"], combined)

        candidates.append(
            {
                "name": name,
                "headline": snippet[:280],
                "linkedin_url": link,
                "current_company_guess": infer_current_company(title, snippet),
                "matched_title_terms": title_hits,
                "matched_keywords": keyword_hits,
                "matched_focus_terms": focus_hits,
                "score": score_candidate(
                    rank=rank,
                    title_hits=title_hits,
                    keyword_hits=keyword_hits,
                    focus_hits=focus_hits,
                    company_match=company_match,
                ),
                "source_title": title,
            }
        )

    candidates.sort(key=lambda c: c["score"], reverse=True)
    return candidates[:top_n]


def flatten_for_csv(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for group in report.get("search_groups", []):
        base = {
            "scope": group.get("scope"),
            "label": group.get("label"),
            "target_company": group.get("target_company"),
            "query": group.get("query"),
        }
        for rank, person in enumerate(group.get("candidates", []), start=1):
            rows.append(
                {
                    **base,
                    "rank": rank,
                    "name": person.get("name"),
                    "headline": person.get("headline"),
                    "current_company_guess": person.get("current_company_guess"),
                    "linkedin_url": person.get("linkedin_url"),
                    "score": person.get("score"),
                    "matched_title_terms": "; ".join(person.get("matched_title_terms", [])),
                    "matched_keywords": "; ".join(person.get("matched_keywords", [])),
                    "matched_focus_terms": "; ".join(person.get("matched_focus_terms", [])),
                }
            )
    return rows


def ensure_non_empty(raw_values: List[str], fallback: List[str]) -> List[str]:
    values = [normalize_whitespace(value) for value in raw_values if normalize_whitespace(value)]
    return values or fallback


def looks_like_placeholder_key(value: str) -> bool:
    lowered = value.strip().lower()
    return lowered in {
        "",
        "your-key-here",
        "your_api_key_here",
        "replace-me",
        "changeme",
        "test",
        "demo",
    }


def main() -> int:
    args = parse_args()

    internal_companies = ensure_non_empty(
        parse_csv_arg(args.internal_companies),
        DEFAULT_INTERNAL_COMPANIES,
    )
    external_companies = parse_csv_arg(args.external_companies)
    title_terms = ensure_non_empty(parse_csv_arg(args.title_terms), DEFAULT_TITLE_TERMS)
    keywords = ensure_non_empty(parse_csv_arg(args.keywords), DEFAULT_KEYWORDS)
    focus_terms = ensure_non_empty(parse_csv_arg(args.focus_terms), DEFAULT_FOCUS_TERMS)

    requested_fetch = bool(args.fetch)
    serper_key = (args.serper_api_key or "").strip()
    fetch_enabled = bool(requested_fetch and not looks_like_placeholder_key(serper_key))
    groups = build_search_groups(
        internal_companies=internal_companies,
        external_companies=external_companies,
        title_terms=title_terms,
        keywords=keywords,
        focus_terms=focus_terms,
    )

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    report: Dict[str, Any] = {
        "metadata": {
            "generated_at_utc": generated_at,
            "fetch_enabled": fetch_enabled,
            "fetch_requested": requested_fetch,
            "search_provider": "serper" if fetch_enabled else "none",
            "per_query": args.per_query,
            "max_results": args.max_results,
            "internal_companies": internal_companies,
            "external_companies": external_companies,
            "title_terms": title_terms,
            "keywords": keywords,
            "focus_terms": focus_terms,
        },
        "search_groups": [],
    }
    fetch_attempts = 0
    fetch_failures = 0
    fetch_successes = 0

    for group in groups:
        candidates: List[Dict[str, Any]] = []
        if fetch_enabled:
            fetch_attempts += 1
            try:
                results = serper_search(group["query"], serper_key, num=args.max_results)
                fetch_successes += 1
            except Exception as exc:
                fetch_failures += 1
                print(f"Warning: Serper fetch failed for {group['label']}: {exc}")
                results = []

            candidates = select_candidates(
                results=results,
                group=group,
                title_terms=title_terms,
                keywords=keywords,
                focus_terms=focus_terms,
                internal_companies=internal_companies,
                top_n=args.per_query,
            )

        report["search_groups"].append(
            {
                "scope": group["scope"],
                "label": group["label"],
                "target_company": group["target_company"],
                "query": group["query"],
                "candidates": candidates,
            }
        )

    total_candidates = sum(
        len(group.get("candidates", []))
        for group in report.get("search_groups", [])
        if isinstance(group, dict)
    )
    report["metadata"]["fetch_attempts"] = fetch_attempts
    report["metadata"]["fetch_successes"] = fetch_successes
    report["metadata"]["fetch_failures"] = fetch_failures
    report["metadata"]["candidates_found"] = total_candidates

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)
    default_json = output_dir / f"networking_targets_{timestamp}.json"
    default_csv = output_dir / f"networking_targets_{timestamp}.csv"
    out_json = Path(args.output_json) if args.output_json else default_json
    out_csv = Path(args.output_csv) if args.output_csv else default_csv

    with open(out_json, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=2, ensure_ascii=False)

    csv_rows = flatten_for_csv(report)
    with open(out_csv, "w", encoding="utf-8", newline="") as file:
        fieldnames = [
            "scope",
            "label",
            "target_company",
            "query",
            "rank",
            "name",
            "headline",
            "current_company_guess",
            "linkedin_url",
            "score",
            "matched_title_terms",
            "matched_keywords",
            "matched_focus_terms",
        ]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows)

    print(f"Saved networking JSON: {out_json}")
    print(f"Saved networking CSV: {out_csv}")
    if requested_fetch and looks_like_placeholder_key(serper_key):
        print(
            "Fetch was requested but the Serper key looks like a placeholder "
            "(for example 'your-key-here'); wrote query-only output."
        )
    elif requested_fetch and not fetch_enabled:
        print(
            "Fetch was requested but SERPER_DEV_API_KEY/SERPER_API_KEY is missing; wrote query-only output."
        )
    elif not requested_fetch:
        print(
            "Query-only mode enabled. Re-run with --fetch and SERPER_DEV_API_KEY "
            "(or SERPER_API_KEY) to auto-find people."
        )
    elif fetch_failures == fetch_attempts:
        print(
            f"Fetch mode requested, but all {fetch_attempts} queries failed. "
            "Check SERPER_DEV_API_KEY/SERPER_API_KEY and account status."
        )
    elif fetch_failures > 0:
        print(
            f"Fetch mode completed with partial failures "
            f"({fetch_successes} succeeded, {fetch_failures} failed). "
            f"Candidates found: {total_candidates}."
        )
    else:
        print(
            f"Fetch mode enabled. All {fetch_successes} queries succeeded. "
            f"Candidates found: {total_candidates}."
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
