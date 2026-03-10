#!/usr/bin/env python3
"""Generate outreach targets (manager/recruiter/team member) for scraped jobs."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen

ROLE_TYPES = ("manager", "recruiter", "team_member")

ROLE_KEYWORDS = {
    "manager": [
        "hiring manager",
        "manager",
        "director",
        "head of",
        "lead",
    ],
    "recruiter": [
        "recruiter",
        "talent",
        "sourcer",
        "staffing",
        "talent partner",
    ],
    "team_member": [
        "engineer",
        "scientist",
        "analyst",
        "researcher",
        "staff",
        "senior",
    ],
}

TITLE_TEAM_STOPWORDS = {
    "remote",
    "hybrid",
    "onsite",
    "united states",
    "usa",
}

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find outreach targets for each job using team inference + people search."
    )
    parser.add_argument("--json", required=True, help="Path to jobs_*.json output file")
    parser.add_argument(
        "--per-role",
        type=int,
        default=3,
        help="Candidates to keep per role type (manager/recruiter/team_member)",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=0,
        help="Only process first N jobs (0 = all)",
    )
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Fetch candidates from Serper.dev API; otherwise output only queries",
    )
    parser.add_argument(
        "--serper-api-key",
        default=os.getenv("SERPER_DEV_API_KEY", "") or os.getenv("SERPER_API_KEY", ""),
        help="Serper API key (from SERPER_DEV_API_KEY or SERPER_API_KEY)",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Output JSON path (default: output/outreach_targets_<timestamp>.json)",
    )
    parser.add_argument(
        "--output-csv",
        default="",
        help="Output CSV path (default: output/outreach_targets_<timestamp>.csv)",
    )
    return parser.parse_args()


def load_jobs(path: Path) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError("Top-level JSON payload must be an object")
    jobs = payload.get("jobs", [])
    if not isinstance(jobs, list):
        raise ValueError("'jobs' field must be a list")
    return payload, jobs


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


def sanitize_team_name(value: str) -> str:
    team = normalize_whitespace(value).strip(" -,*.")
    team = re.sub(r"^(our|the)\s+", "", team, flags=re.IGNORECASE)
    return team


def infer_team_from_title(title: str) -> Tuple[str, float, str]:
    parts = [normalize_whitespace(p) for p in title.split(",") if normalize_whitespace(p)]
    if len(parts) < 2:
        return "", 0.0, ""

    for candidate in parts[1:]:
        low = candidate.lower()
        if low in TITLE_TEAM_STOPWORDS:
            continue
        if len(candidate.split()) <= 6 and not any(ch.isdigit() for ch in candidate):
            return candidate, 0.7, "title_comma_segment"

    return "", 0.0, ""


def infer_team_from_description(description: str) -> Tuple[str, float, str]:
    # Keep early content where actual JD usually appears.
    desc = normalize_whitespace(description)[:2500]
    if not desc:
        return "", 0.0, ""

    patterns = [
        (r"part of the ([A-Za-z0-9&/\- ]{2,80}?) org", 0.9, "part_of_org"),
        (r"within the ([A-Za-z0-9&/\- ]{2,80}?)(?: team| org| organization)", 0.85, "within_team"),
        (r"on the ([A-Za-z0-9&/\- ]{2,80}?) team", 0.8, "on_team"),
        (r"([A-Za-z0-9&/\- ]{2,80}?) team is", 0.75, "team_is"),
        (r"([A-Za-z0-9&/\- ]{2,80}?) team works", 0.75, "team_works"),
        (r"([A-Za-z0-9&/\- ]{2,80}?) team builds", 0.75, "team_builds"),
    ]

    best = ("", 0.0, "")
    for pattern, confidence, source in patterns:
        for match in re.finditer(pattern, desc, flags=re.IGNORECASE):
            candidate = sanitize_team_name(match.group(1))
            if not candidate:
                continue
            if candidate.lower() in {"team", "organization", "org", "group"}:
                continue
            if len(candidate) < 3 or len(candidate) > 80:
                continue
            if confidence > best[1]:
                best = (candidate, confidence, source)
    return best


def infer_team(job: Dict[str, Any]) -> Tuple[str, float, str]:
    title = text(job.get("title"))
    description = text(job.get("description"))
    from_desc = infer_team_from_description(description)
    from_title = infer_team_from_title(title)
    return from_desc if from_desc[1] >= from_title[1] else from_title


def infer_role_family(title: str) -> str:
    low = title.lower()
    mapping = [
        ("data scientist", "Data Scientist"),
        ("data engineer", "Data Engineer"),
        ("machine learning engineer", "Machine Learning Engineer"),
        ("machine learning scientist", "Machine Learning Scientist"),
        ("applied scientist", "Applied Scientist"),
        ("ai engineer", "AI Engineer"),
        ("analytics engineer", "Analytics Engineer"),
    ]
    for key, label in mapping:
        if key in low:
            return label
    return normalize_whitespace(title.split(",")[0]) or "Engineer"


def normalize_company(company: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9 ]", " ", company.lower())
    return normalize_whitespace(cleaned)


def company_aliases(company: str) -> List[str]:
    base = normalize_company(company)
    if not base:
        return []

    tokens = [t for t in base.split() if t]
    aliases = {base}

    trimmed = [t for t in tokens if t not in COMPANY_SUFFIXES]
    if trimmed:
        aliases.add(" ".join(trimmed))

    # Also allow dropping trailing geography terms for company names like:
    # "Toyota Connected North America" -> "toyota connected"
    geography_suffixes = {"north", "south", "east", "west", "america", "americas", "us", "usa"}
    geo_trimmed = list(trimmed or tokens)
    while geo_trimmed and geo_trimmed[-1] in geography_suffixes:
        geo_trimmed.pop()
    if geo_trimmed:
        aliases.add(" ".join(geo_trimmed))

    return [a for a in aliases if a]


def is_company_match(company: str, text_blob: str) -> bool:
    haystack = normalize_company(text_blob)
    if not haystack:
        return False

    for alias in company_aliases(company):
        if not alias:
            continue
        if re.search(rf"\b{re.escape(alias)}\b", haystack):
            return True
    return False


def has_role_signal(role_type: str, text_blob: str) -> bool:
    joined = text_blob.lower()
    return any(keyword in joined for keyword in ROLE_KEYWORDS[role_type])


def build_queries(job: Dict[str, Any], team: str) -> Dict[str, str]:
    company = text(job.get("company")) or "Company"
    role_family = infer_role_family(text(job.get("title")))

    scope_clause = f"\"{team}\" OR \"{role_family}\"" if team else f"\"{role_family}\""
    base = f"site:linkedin.com/in \"{company}\" ({scope_clause})"

    return {
        "manager": base + " (\"hiring manager\" OR manager OR director OR \"head of\" OR lead)",
        "recruiter": base + " (recruiter OR \"talent partner\" OR \"technical recruiter\" OR sourcer)",
        "team_member": base + " (\"senior\" OR \"staff\" OR engineer OR scientist OR analyst)",
    }


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


def score_candidate(
    role_type: str,
    rank: int,
    title: str,
    snippet: str,
    company: str,
    team: str,
) -> int:
    joined = (title + " " + snippet).lower()
    score = 100 - rank
    if company and company.lower() in joined:
        score += 35
    if team and team.lower() in joined:
        score += 20
    for keyword in ROLE_KEYWORDS[role_type]:
        if keyword in joined:
            score += 12
    return score


def select_candidates(
    results: List[Dict[str, Any]],
    role_type: str,
    company: str,
    team: str,
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
        combined = f"{title} {snippet}".strip()

        # Tighten quality: keep only results that mention the target company and role signal.
        if not is_company_match(company, combined):
            continue
        if not has_role_signal(role_type, combined):
            continue

        name = extract_name_from_title(title)
        if not name:
            continue

        key = link.split("?", 1)[0].rstrip("/")
        if key in seen:
            continue
        seen.add(key)

        candidates.append(
            {
                "name": name,
                "headline": snippet[:280],
                "linkedin_url": link,
                "score": score_candidate(role_type, rank, title, snippet, company, team),
                "source_title": title,
            }
        )

    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[:top_n]


def flatten_for_csv(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    for job in report.get("jobs", []):
        base = {
            "job_id": job.get("job_id"),
            "title": job.get("title"),
            "company": job.get("company"),
            "team_guess": job.get("team_guess"),
            "team_confidence": job.get("team_confidence"),
        }
        candidates = job.get("candidates", {})
        for role_type in ROLE_TYPES:
            for i, person in enumerate(candidates.get(role_type, []), start=1):
                rows.append(
                    {
                        **base,
                        "role_type": role_type,
                        "rank": i,
                        "name": person.get("name"),
                        "headline": person.get("headline"),
                        "linkedin_url": person.get("linkedin_url"),
                        "score": person.get("score"),
                    }
                )
    return rows


def main() -> int:
    args = parse_args()
    json_path = Path(args.json)
    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    payload, jobs = load_jobs(json_path)
    if args.max_jobs > 0:
        jobs = jobs[: args.max_jobs]

    fetch_enabled = bool(args.fetch and args.serper_api_key)

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    report: Dict[str, Any] = {
        "metadata": {
            "generated_at_utc": generated_at,
            "source_jobs_file": str(json_path),
            "jobs_processed": len(jobs),
            "per_role": args.per_role,
            "fetch_enabled": fetch_enabled,
            "search_provider": "serper" if fetch_enabled else "none",
        },
        "jobs": [],
    }

    for job in jobs:
        team_guess, team_conf, team_source = infer_team(job)
        queries = build_queries(job, team_guess)
        company = text(job.get("company"))

        candidates = {role_type: [] for role_type in ROLE_TYPES}
        if fetch_enabled:
            for role_type in ROLE_TYPES:
                try:
                    results = serper_search(queries[role_type], args.serper_api_key, num=10)
                except Exception as exc:
                    print(
                        f"Warning: Serper fetch failed for role={role_type} "
                        f"job={job.get('job_id') or job.get('title')}: {exc}"
                    )
                    results = []
                candidates[role_type] = select_candidates(
                    results=results,
                    role_type=role_type,
                    company=company,
                    team=team_guess,
                    top_n=args.per_role,
                )

        report["jobs"].append(
            {
                "job_id": text(job.get("job_id")),
                "title": text(job.get("title")),
                "company": company,
                "location": text(job.get("location")),
                "team_guess": team_guess,
                "team_confidence": team_conf,
                "team_source": team_source,
                "queries": queries,
                "candidates": candidates,
            }
        )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_json = json_path.parent / f"outreach_targets_{timestamp}.json"
    default_csv = json_path.parent / f"outreach_targets_{timestamp}.csv"
    out_json = Path(args.output_json) if args.output_json else default_json
    out_csv = Path(args.output_csv) if args.output_csv else default_csv

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    csv_rows = flatten_for_csv(report)
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "job_id",
            "title",
            "company",
            "team_guess",
            "team_confidence",
            "role_type",
            "rank",
            "name",
            "headline",
            "linkedin_url",
            "score",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows)

    print(f"Saved outreach JSON: {out_json}")
    print(f"Saved outreach CSV: {out_csv}")
    if not fetch_enabled and args.fetch:
        print(
            "Fetch was requested but SERPER_DEV_API_KEY/SERPER_API_KEY is missing; "
            "wrote query-only output."
        )
    elif not args.fetch:
        print(
            "Query-only mode enabled. Re-run with --fetch and "
            "SERPER_DEV_API_KEY (or SERPER_API_KEY) to auto-find people."
        )
    else:
        print("Fetch mode enabled. Candidate lists were populated from Serper API results.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
