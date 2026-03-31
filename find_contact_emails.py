#!/usr/bin/env python3
"""Discover email addresses for outreach contacts using Hunter.io + pattern guessing."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

ROLE_TYPES = ("manager", "recruiter", "team_member")

# Common corporate email patterns (ordered by prevalence)
EMAIL_PATTERNS = [
    "{first}.{last}@{domain}",
    "{first}{last}@{domain}",
    "{first}@{domain}",
    "{first_initial}{last}@{domain}",
    "{first_initial}.{last}@{domain}",
    "{last}.{first}@{domain}",
    "{first}_{last}@{domain}",
]

# Well-known company domain overrides (company name -> email domain)
KNOWN_DOMAINS = {
    "google": "google.com",
    "alphabet": "google.com",
    "meta": "meta.com",
    "facebook": "meta.com",
    "amazon": "amazon.com",
    "aws": "amazon.com",
    "microsoft": "microsoft.com",
    "apple": "apple.com",
    "netflix": "netflix.com",
    "nvidia": "nvidia.com",
    "openai": "openai.com",
    "anthropic": "anthropic.com",
    "walmart": "walmart.com",
    "walmart global tech": "walmart.com",
    "jpmorgan": "jpmorgan.com",
    "jpmorgan chase": "jpmchase.com",
    "goldman sachs": "gs.com",
    "deloitte": "deloitte.com",
    "mckinsey": "mckinsey.com",
    "tesla": "tesla.com",
    "uber": "uber.com",
    "lyft": "lyft.com",
    "airbnb": "airbnb.com",
    "stripe": "stripe.com",
    "salesforce": "salesforce.com",
    "adobe": "adobe.com",
    "ibm": "ibm.com",
    "oracle": "oracle.com",
    "palantir": "palantir.com",
    "databricks": "databricks.com",
    "snowflake": "snowflake.com",
    "datadog": "datadoghq.com",
    "spotify": "spotify.com",
    "twitter": "x.com",
    "x": "x.com",
    "linkedin": "linkedin.com",
    "bytedance": "bytedance.com",
    "tiktok": "bytedance.com",
    "samsung": "samsung.com",
    "intel": "intel.com",
    "amd": "amd.com",
    "cisco": "cisco.com",
    "vmware": "vmware.com",
    "servicenow": "servicenow.com",
    "workday": "workday.com",
    "twilio": "twilio.com",
    "shopify": "shopify.com",
    "doordash": "doordash.com",
    "instacart": "instacart.com",
    "coinbase": "coinbase.com",
    "robinhood": "robinhood.com",
    "capital one": "capitalone.com",
    "american express": "aexp.com",
    "visa": "visa.com",
    "mastercard": "mastercard.com",
    "paypal": "paypal.com",
    "block": "block.xyz",
    "square": "squareup.com",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find email addresses for outreach contacts via Hunter.io + pattern guessing."
    )
    parser.add_argument("--json", required=True, help="Path to outreach_targets JSON file")
    parser.add_argument(
        "--hunter-api-key",
        default=os.getenv("HUNTER_IO_API_KEY", ""),
        help="Hunter.io API key (from HUNTER_IO_API_KEY env var)",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=0,
        help="Only process first N jobs (0 = all)",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Output JSON path (default: output/contact_emails_<timestamp>.json)",
    )
    parser.add_argument(
        "--output-csv",
        default="",
        help="Output CSV path (default: output/contact_emails_<timestamp>.csv)",
    )
    return parser.parse_args()


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "; ".join(as_text(v) for v in value)
    return str(value)


def split_name(full_name: str) -> Tuple[str, str]:
    """Split a full name into first and last name."""
    parts = full_name.strip().split()
    if len(parts) == 0:
        return "", ""
    if len(parts) == 1:
        return parts[0].lower(), ""
    return parts[0].lower(), parts[-1].lower()


def guess_company_domain(company: str) -> Optional[str]:
    """
    Guess the email domain for a company name.

    Checks the known domains dict first, then falls back to
    cleaning the company name into a plausible domain.
    """
    if not company:
        return None

    cleaned = re.sub(r"[^a-zA-Z0-9 ]", "", company).strip().lower()

    # Check known domains
    for key, domain in KNOWN_DOMAINS.items():
        if key == cleaned or key in cleaned:
            return domain

    # Fallback: strip common suffixes and build domain
    suffixes_to_strip = {
        "inc", "llc", "ltd", "corp", "co", "company",
        "technologies", "technology", "group", "holdings",
        "solutions", "services", "labs", "systems",
    }
    tokens = cleaned.split()
    stripped = [t for t in tokens if t not in suffixes_to_strip]
    if not stripped:
        stripped = tokens

    # Build domain from remaining tokens
    domain_base = "".join(stripped)
    if domain_base:
        return f"{domain_base}.com"

    return None


def hunter_email_finder(
    first_name: str,
    last_name: str,
    domain: str,
    api_key: str,
) -> Optional[Dict[str, Any]]:
    """
    Use Hunter.io email-finder API to find an email address.

    Returns dict with email, confidence, sources, etc. or None on failure.
    """
    if not api_key:
        return None

    params = (
        f"domain={quote_plus(domain)}"
        f"&first_name={quote_plus(first_name)}"
        f"&last_name={quote_plus(last_name)}"
        f"&api_key={quote_plus(api_key)}"
    )
    url = f"https://api.hunter.io/v2/email-finder?{params}"

    try:
        req = Request(url, headers={"Accept": "application/json"})
        with urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode("utf-8"))

        data = payload.get("data", {})
        email = data.get("email")
        if email:
            return {
                "email": email,
                "confidence": data.get("score", 0),
                "source": "hunter_io",
                "first_name_used": data.get("first_name", first_name),
                "last_name_used": data.get("last_name", last_name),
                "domain": domain,
            }
    except HTTPError as e:
        if e.code == 429:
            print("  Hunter.io rate limit reached. Falling back to pattern guessing.")
        elif e.code == 401:
            print("  Hunter.io API key invalid.")
        else:
            print(f"  Hunter.io HTTP error {e.code}: {e.reason}")
    except (URLError, Exception) as e:
        print(f"  Hunter.io request failed: {e}")

    return None


def hunter_domain_search(domain: str, api_key: str) -> Optional[str]:
    """
    Use Hunter.io domain-search to find the email pattern for a domain.

    Returns the pattern string (e.g., "{first}.{last}") or None.
    """
    if not api_key:
        return None

    url = f"https://api.hunter.io/v2/domain-search?domain={quote_plus(domain)}&api_key={quote_plus(api_key)}&limit=1"

    try:
        req = Request(url, headers={"Accept": "application/json"})
        with urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode("utf-8"))

        data = payload.get("data", {})
        pattern = data.get("pattern")
        return pattern  # e.g., "{first}.{last}" or "{first}{l}"
    except Exception:
        return None


def generate_pattern_emails(
    first_name: str,
    last_name: str,
    domain: str,
) -> List[Dict[str, Any]]:
    """
    Generate candidate email addresses from common patterns.

    Returns list of dicts with email, confidence, and source.
    """
    if not first_name or not last_name or not domain:
        return []

    first = re.sub(r"[^a-z]", "", first_name.lower())
    last = re.sub(r"[^a-z]", "", last_name.lower())

    if not first or not last:
        return []

    results = []
    # Confidence decreases with each less-common pattern
    base_confidence = 45

    for i, pattern in enumerate(EMAIL_PATTERNS):
        email = pattern.format(
            first=first,
            last=last,
            first_initial=first[0],
            domain=domain,
        )
        results.append({
            "email": email,
            "confidence": max(10, base_confidence - (i * 5)),
            "source": "pattern_guess",
            "pattern": pattern,
        })

    return results


def find_email_for_contact(
    name: str,
    company: str,
    hunter_api_key: str,
    _hunter_exhausted: List[bool],
) -> Dict[str, Any]:
    """
    Find the best email address for a single contact.

    Strategy:
    1. Try Hunter.io email-finder (primary)
    2. Fall back to pattern guessing (secondary)

    Returns dict with email, confidence, source, and all candidates.
    """
    first, last = split_name(name)
    domain = guess_company_domain(company)

    result: Dict[str, Any] = {
        "email": None,
        "email_confidence": 0,
        "email_source": "none",
        "email_domain": domain,
        "all_candidates": [],
    }

    if not first or not last:
        result["email_error"] = "Could not parse first/last name"
        return result

    if not domain:
        result["email_error"] = "Could not determine company email domain"
        return result

    # Strategy 1: Hunter.io
    if hunter_api_key and not _hunter_exhausted[0]:
        hunter_result = hunter_email_finder(first, last, domain, hunter_api_key)
        if hunter_result:
            result["email"] = hunter_result["email"]
            result["email_confidence"] = hunter_result["confidence"]
            result["email_source"] = "hunter_io"
            result["all_candidates"] = [hunter_result]
            return result
        # If Hunter returns nothing, we still try patterns
        # But if it was a 429, mark exhausted
        # (the flag is set inside hunter_email_finder via the mutable list)

    # Strategy 2: Pattern guessing
    pattern_candidates = generate_pattern_emails(first, last, domain)
    if pattern_candidates:
        best = pattern_candidates[0]
        result["email"] = best["email"]
        result["email_confidence"] = best["confidence"]
        result["email_source"] = "pattern_guess"
        result["all_candidates"] = pattern_candidates[:3]  # Top 3 guesses

    return result


def process_contacts(
    payload: Dict[str, Any],
    hunter_api_key: str,
    max_jobs: int,
) -> Dict[str, Any]:
    """
    Process all contacts in the outreach targets payload and find emails.

    Returns enriched payload with email fields added to each candidate.
    """
    jobs = payload.get("jobs", [])
    if max_jobs > 0:
        jobs = jobs[:max_jobs]

    hunter_exhausted = [False]  # Mutable flag for rate limiting
    total_found = 0
    total_contacts = 0

    enriched_jobs = []
    for job in jobs:
        company = as_text(job.get("company"))
        candidates = job.get("candidates", {})

        enriched_candidates = {}
        for role_type in ROLE_TYPES:
            role_people = candidates.get(role_type, [])
            enriched_people = []

            for person in role_people:
                if not isinstance(person, dict):
                    continue

                total_contacts += 1
                name = as_text(person.get("name"))

                print(f"  Finding email for {name} at {company}...")
                email_result = find_email_for_contact(
                    name=name,
                    company=company,
                    hunter_api_key=hunter_api_key,
                    _hunter_exhausted=hunter_exhausted,
                )

                enriched_person = {**person, **email_result}
                enriched_people.append(enriched_person)

                if email_result.get("email"):
                    total_found += 1
                    print(f"    -> {email_result['email']} ({email_result['email_source']}, "
                          f"confidence: {email_result['email_confidence']})")
                else:
                    print(f"    -> No email found: {email_result.get('email_error', 'unknown')}")

                # Small delay to avoid hammering APIs
                time.sleep(0.5)

            enriched_candidates[role_type] = enriched_people

        enriched_job = {**job, "candidates": enriched_candidates}
        enriched_jobs.append(enriched_job)

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    report = {
        "metadata": {
            "generated_at_utc": generated_at,
            "source_file": as_text(payload.get("metadata", {}).get("source_jobs_file", "")),
            "contacts_processed": total_contacts,
            "emails_found": total_found,
            "hunter_io_used": bool(hunter_api_key),
        },
        "jobs": enriched_jobs,
    }

    print(f"\nEmail discovery complete: {total_found}/{total_contacts} emails found.")
    return report


def flatten_for_csv(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flatten the enriched report into CSV rows."""
    rows = []
    for job in report.get("jobs", []):
        base = {
            "job_id": job.get("job_id"),
            "title": job.get("title"),
            "company": job.get("company"),
            "team_guess": job.get("team_guess"),
        }
        candidates = job.get("candidates", {})
        for role_type in ROLE_TYPES:
            for rank, person in enumerate(candidates.get(role_type, []), start=1):
                rows.append({
                    **base,
                    "role_type": role_type,
                    "rank": rank,
                    "name": person.get("name"),
                    "headline": person.get("headline"),
                    "linkedin_url": person.get("linkedin_url"),
                    "score": person.get("score"),
                    "email": person.get("email"),
                    "email_confidence": person.get("email_confidence"),
                    "email_source": person.get("email_source"),
                    "email_domain": person.get("email_domain"),
                })
    return rows


def main() -> int:
    args = parse_args()
    input_path = Path(args.json)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    with open(input_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    print(f"Finding emails for contacts in {input_path.name}...")
    if args.hunter_api_key:
        print("  Hunter.io API key detected - will use as primary source.")
    else:
        print("  No Hunter.io API key - using pattern guessing only.")

    report = process_contacts(
        payload=payload,
        hunter_api_key=args.hunter_api_key,
        max_jobs=args.max_jobs,
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_json = input_path.parent / f"contact_emails_{timestamp}.json"
    default_csv = input_path.parent / f"contact_emails_{timestamp}.csv"
    out_json = Path(args.output_json) if args.output_json else default_json
    out_csv = Path(args.output_csv) if args.output_csv else default_csv

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    csv_rows = flatten_for_csv(report)
    if csv_rows:
        with open(out_csv, "w", encoding="utf-8", newline="") as f:
            fieldnames = [
                "job_id", "title", "company", "team_guess",
                "role_type", "rank", "name", "headline", "linkedin_url",
                "score", "email", "email_confidence", "email_source", "email_domain",
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_rows)

    print(f"Saved email discovery JSON: {out_json}")
    print(f"Saved email discovery CSV: {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
