#!/usr/bin/env python3
"""Extract job entries from daily search markdown reports.

Parses the markdown table format used in daily_searches/*_new_jobs.md files.
Outputs a list of job dicts compatible with job_scraper.py output format.
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional


def extract_url_from_markdown_link(text: str) -> str:
    """Extract URL from markdown link syntax [text](url)."""
    match = re.search(r'\[.*?\]\((https?://[^\s)]+)\)', text)
    return match.group(1) if match else ""


def extract_linkedin_job_id(url: str) -> str:
    """Extract numeric job ID from LinkedIn URL."""
    match = re.search(r'/jobs/view/(\d+)', url)
    return match.group(1) if match else ""


def clean_bold(text: str) -> str:
    """Remove markdown bold markers."""
    return re.sub(r'\*\*', '', text).strip()


def parse_table_rows(content: str) -> List[Dict[str, str]]:
    """Parse markdown table rows into job entries.

    Handles the format:
    | Company | Title | Location | URL | Fit Notes |
    |---------|-------|----------|-----|-----------|
    | **Anthropic** | Forward Deployed... | SF | [Apply](url) | Strong fit... |
    """
    entries = []
    lines = content.split('\n')

    in_table = False
    header_found = False

    for line in lines:
        stripped = line.strip()

        # Detect table by looking for pipe-delimited rows
        if not stripped.startswith('|'):
            in_table = False
            header_found = False
            continue

        # Split by pipe, ignore first and last empty elements
        cells = [c.strip() for c in stripped.split('|')]
        cells = [c for c in cells if c != '']

        # Skip separator rows (like |---|---|)
        if all(re.match(r'^[-:]+$', c) for c in cells):
            header_found = True
            continue

        # Skip header row (contains "Company", "Title", etc.)
        if not header_found and any(c.lower() in ('company', 'title', 'location', 'url', 'fit notes') for c in cells):
            in_table = True
            continue

        # Must have at least 4 columns to be a data row
        if len(cells) < 4:
            continue

        # Parse the row
        company = clean_bold(cells[0])
        title = cells[1].strip()
        location = cells[2].strip()
        url_cell = cells[3].strip()
        fit_notes = cells[4].strip() if len(cells) > 4 else ""

        url = extract_url_from_markdown_link(url_cell)
        if not url:
            url = url_cell  # Maybe it's a raw URL

        # Skip rows that look like headers or non-job entries
        if not company or company.lower() == 'company':
            continue

        job_id = extract_linkedin_job_id(url)

        entries.append({
            "company": company,
            "title": title,
            "location": location,
            "url": url,
            "job_id": job_id,
            "fit_notes": clean_bold(fit_notes),
            "source": "daily_report",
        })

    return entries


def parse_daily_report(filepath: str) -> List[Dict[str, str]]:
    """Extract job entries from a daily search markdown report.

    Args:
        filepath: Path to a *_new_jobs.md file

    Returns:
        List of dicts with company, title, location, url, job_id, fit_notes, source
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Report not found: {filepath}")

    content = path.read_text(encoding="utf-8")
    return parse_table_rows(content)


def main():
    parser = argparse.ArgumentParser(description="Parse daily search report(s) into job entries.")
    parser.add_argument("reports", nargs="+", help="Path(s) to daily report markdown files")
    parser.add_argument("--output-json", default="", help="Output JSON path")
    parser.add_argument("--deduplicate", action="store_true", help="Remove duplicate URLs across reports")
    args = parser.parse_args()

    all_entries = []
    for report_path in args.reports:
        try:
            entries = parse_daily_report(report_path)
            print(f"Parsed {len(entries)} jobs from {Path(report_path).name}")
            all_entries.extend(entries)
        except Exception as e:
            print(f"Error parsing {report_path}: {e}")

    if args.deduplicate:
        seen_urls = set()
        deduped = []
        for entry in all_entries:
            url = entry.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                deduped.append(entry)
            elif not url:
                deduped.append(entry)
        all_entries = deduped

    if args.output_json:
        out_path = Path(args.output_json)
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = Path("output") / f"daily_report_jobs_{ts}.json"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"source": "daily_reports", "jobs": all_entries}, f, indent=2)

    print(f"Total jobs extracted: {len(all_entries)}")
    print(f"Saved to: {out_path}")


if __name__ == "__main__":
    main()
