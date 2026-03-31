#!/usr/bin/env python3
"""One-time migration: merge processed_job_ids.json entries into seen_jobs.json."""

import json
import sys
from pathlib import Path

# Add repo to path
sys.path.insert(0, str(Path(__file__).parent))

from config import OUTPUT_DIR, PROCESSED_JOB_IDS_FILE, WORKSPACE_DAILY_DIR
from job_manager import JobManager


def find_latest_jobs_json(output_dir: Path) -> Path | None:
    files = list(output_dir.glob("jobs_*.json"))
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)


def main():
    # Load processed job IDs (flat list of strings)
    if not PROCESSED_JOB_IDS_FILE.exists():
        print("No processed_job_ids.json found. Nothing to migrate.")
        return

    with open(PROCESSED_JOB_IDS_FILE, "r") as f:
        processed_ids = set(json.load(f))

    print(f"Found {len(processed_ids)} processed job IDs.")

    # Load the most recent jobs JSON to get metadata for these IDs
    jobs_json_path = find_latest_jobs_json(OUTPUT_DIR)
    if not jobs_json_path:
        print("No jobs_*.json files found in output/. Cannot get metadata for migration.")
        print("Migration will skip metadata-based registration.")
        return

    with open(jobs_json_path, "r") as f:
        payload = json.load(f)

    jobs = payload.get("jobs", [])
    jobs_by_id = {}
    for job in jobs:
        jid = job.get("job_id")
        if jid:
            jobs_by_id[str(jid)] = job

    print(f"Loaded {len(jobs_by_id)} jobs from {jobs_json_path.name}")

    # Initialize JobManager pointing at WORKSPACE_DAILY_DIR
    mgr = JobManager(str(WORKSPACE_DAILY_DIR))
    registered = 0
    skipped = 0

    for job_id in processed_ids:
        job_data = jobs_by_id.get(job_id)
        if not job_data:
            continue  # No metadata available for this ID

        company = job_data.get("company", "")
        title = job_data.get("title", "")
        if not company or not title:
            continue

        # Check if already in seen_jobs.json
        if mgr.is_seen(company, title):
            skipped += 1
            continue

        # Also check by URL
        url = job_data.get("url", "")
        if url and mgr.is_seen_by_url(url):
            skipped += 1
            continue

        # Register it
        try:
            result = mgr.register_from_scraped(job_data)
            print(f"  Registered: {company} — {title}")
            registered += 1
        except Exception as e:
            print(f"  Failed to register {company} — {title}: {e}")

    print(f"\nMigration complete: {registered} registered, {skipped} skipped (already seen)")


if __name__ == "__main__":
    main()
