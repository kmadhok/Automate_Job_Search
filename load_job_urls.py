#!/usr/bin/env python3
"""Load, validate, and deduplicate LinkedIn job URLs from a text file."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Set


def _extract_job_id(url: str) -> Optional[str]:
    """Extract the numeric LinkedIn job ID from a URL."""
    match = re.search(r"/jobs/view/(\d+)", url)
    return match.group(1) if match else None


def _normalize_url(raw: str) -> Optional[Dict[str, str]]:
    """
    Validate and normalize a LinkedIn job URL.

    Returns a dict with raw_url, canonical_url, and job_id,
    or None if the URL is not a valid LinkedIn job link.
    """
    cleaned = raw.strip().rstrip(".,;:)")
    if not cleaned:
        return None

    # Must look like a LinkedIn job URL
    if not re.match(r"https?://.*linkedin\.com/.*jobs/view/\d+", cleaned, re.IGNORECASE):
        return None

    job_id = _extract_job_id(cleaned)
    if not job_id:
        return None

    canonical = f"https://www.linkedin.com/jobs/view/{job_id}/"
    return {
        "raw_url": cleaned,
        "canonical_url": canonical,
        "job_id": job_id,
    }


def load_processed_job_ids(path: Path) -> Set[str]:
    """Load previously processed job IDs from the tracking file."""
    if not path.exists():
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {str(item) for item in data}
    except Exception:
        pass
    return set()


def save_processed_job_ids(path: Path, job_ids: Set[str]) -> None:
    """Persist processed job IDs for future deduplication."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sorted(job_ids), f, indent=2)


def load_urls_from_file(
    file_path: Path,
    processed_ids_path: Optional[Path] = None,
    skip_processed: bool = True,
) -> List[Dict[str, str]]:
    """
    Read LinkedIn job URLs from a text file (one URL per line).

    Args:
        file_path: Path to the text file containing URLs.
        processed_ids_path: Path to processed_job_ids.json for deduplication.
        skip_processed: If True, skip URLs whose job IDs are already processed.

    Returns:
        List of dicts with raw_url, canonical_url, and job_id.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"URL file not found: {file_path}")

    processed_ids: Set[str] = set()
    if skip_processed and processed_ids_path:
        processed_ids = load_processed_job_ids(processed_ids_path)

    seen_ids: Set[str] = set()
    results: List[Dict[str, str]] = []
    skipped_processed = 0
    skipped_invalid = 0
    skipped_duplicate = 0

    with open(file_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            raw = line.strip()

            # Skip blanks and comments
            if not raw or raw.startswith("#"):
                continue

            normalized = _normalize_url(raw)
            if not normalized:
                skipped_invalid += 1
                print(f"  Line {line_num}: skipped invalid URL: {raw[:80]}")
                continue

            job_id = normalized["job_id"]

            if job_id in seen_ids:
                skipped_duplicate += 1
                continue
            seen_ids.add(job_id)

            if skip_processed and job_id in processed_ids:
                skipped_processed += 1
                continue

            results.append(normalized)

    print(f"Loaded {len(results)} new URLs from {file_path.name}")
    if skipped_invalid:
        print(f"  Skipped {skipped_invalid} invalid URLs")
    if skipped_duplicate:
        print(f"  Skipped {skipped_duplicate} duplicate URLs")
    if skipped_processed:
        print(f"  Skipped {skipped_processed} already-processed URLs")

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Validate and preview job URLs from a file.")
    parser.add_argument("file", help="Path to text file with LinkedIn job URLs (one per line)")
    parser.add_argument(
        "--processed-ids",
        default="",
        help="Path to processed_job_ids.json (optional, for dedup)",
    )
    args = parser.parse_args()

    processed_path = Path(args.processed_ids) if args.processed_ids else None
    urls = load_urls_from_file(Path(args.file), processed_path)

    for entry in urls:
        print(f"  {entry['job_id']}  {entry['canonical_url']}")

    print(f"\nTotal valid URLs: {len(urls)}")
