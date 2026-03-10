#!/usr/bin/env python3
"""
Main script to process LinkedIn job emails and scrape job descriptions.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd

from config import (
    GMAIL_LABEL,
    MAX_JOB_URLS,
    JOB_TAGS,
    OPENAI_API_KEY,
    OUTPUT_DIR,
    PROCESSED_JOB_IDS_FILE,
    PROCESSED_MESSAGE_IDS_FILE,
    SAVE_CSV,
    SAVE_JSON,
)
from email_processor import (
    build_gmail_query,
    extract_email_content,
    extract_linkedin_job_links,
    get_emails_by_label,
    match_job_tags,
)
from gmail_auth import get_gmail_service, get_label_id
from job_scraper import scrape_jobs_sync


def load_seen_ids(path: Path) -> Set[str]:
    """Load processed IDs from JSON file."""
    if not path.exists():
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {str(item) for item in data}
    except Exception as e:
        print(f"⚠️  Could not read {path.name}: {e}")
    return set()


def save_seen_ids(path: Path, values: Set[str]) -> None:
    """Persist processed IDs as a sorted JSON array."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sorted(values), f, indent=2)


def flatten_job_for_csv(job: Dict) -> Dict:
    """Flatten list-like fields so they can be written to CSV."""
    flat_job = job.copy()
    for field in ("requirements", "responsibilities", "benefits", "source_tags"):
        if isinstance(flat_job.get(field), list):
            flat_job[field] = "; ".join(flat_job[field])
    return flat_job


def main() -> int:
    """Main execution function."""
    print("=" * 60)
    print("LinkedIn Job Email Automation")
    print("=" * 60)

    if not OPENAI_API_KEY:
        print("\n❌ ERROR: OPENAI_API_KEY not found!")
        print("Please create a .env file with your OpenAI API key:")
        print("OPENAI_API_KEY=your-api-key-here")
        return 1

    print("\n[1/6] Loading incremental state...")
    processed_message_ids = load_seen_ids(PROCESSED_MESSAGE_IDS_FILE)
    processed_job_keys = load_seen_ids(PROCESSED_JOB_IDS_FILE)
    print(f"✓ Previously processed messages: {len(processed_message_ids)}")
    print(f"✓ Previously processed jobs: {len(processed_job_keys)}")

    print("\n[2/6] Authenticating with Gmail...")
    try:
        service = get_gmail_service()
        print("✓ Successfully authenticated")
    except FileNotFoundError as e:
        print(f"❌ {e}")
        return 1
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return 1

    print(f"\n[3/6] Finding label '{GMAIL_LABEL}'...")
    label_id = get_label_id(service, GMAIL_LABEL)
    if not label_id:
        print(f"❌ Label '{GMAIL_LABEL}' not found!")
        print("\nAvailable labels:")
        try:
            results = service.users().labels().list(userId="me").execute()
            for label in results.get("labels", []):
                print(f"  - {label['name']}")
        except Exception:
            pass
        return 1
    print(f"✓ Found label: {label_id}")

    print("\n[4/6] Fetching LinkedIn emails...")
    gmail_query = build_gmail_query(JOB_TAGS)
    if gmail_query:
        print(f"Using Gmail query: {gmail_query}")
    emails = get_emails_by_label(service, label_id, query=gmail_query)
    if not emails:
        print("❌ No emails found for this label/query")
        return 0
    print(f"✓ Found {len(emails)} candidate emails")

    print("\n[5/6] Extracting tagged LinkedIn job URLs from emails...")
    email_metadata = []
    link_sources = []
    newly_processed_messages = set()

    for i, email in enumerate(emails):
        email_content = extract_email_content(email)
        message_id = email_content["message_id"]

        if message_id in processed_message_ids:
            continue

        matched_tags = match_job_tags(email_content, JOB_TAGS)
        if JOB_TAGS and not matched_tags:
            newly_processed_messages.add(message_id)
            continue

        linkedin_links = extract_linkedin_job_links(email_content)
        print(
            f"  Email {i + 1}/{len(emails)}: "
            f"{email_content['subject'][:70]} ({len(linkedin_links)} links)"
        )

        email_metadata.append(
            {
                "message_id": message_id,
                "subject": email_content["subject"],
                "from": email_content["from"],
                "date": email_content["date"],
                "matched_tags": matched_tags,
                "links_count": len(linkedin_links),
                "body_text_preview": (email_content["body_text"][:500] or "").strip(),
                "body_html_preview": (email_content["body_html"][:500] or "").strip(),
            }
        )

        for link in linkedin_links:
            dedupe_key = link["job_id"] or link["canonical_url"]
            if dedupe_key in processed_job_keys:
                continue
            link_sources.append(
                {
                    "message_id": message_id,
                    "subject": email_content["subject"],
                    "matched_tags": matched_tags,
                    "raw_url": link["raw_url"],
                    "canonical_url": link["canonical_url"],
                    "job_id": link["job_id"],
                    "dedupe_key": dedupe_key,
                }
            )

        newly_processed_messages.add(message_id)

    if not link_sources:
        print("❌ No new LinkedIn job links found after filtering/deduping")
        processed_message_ids.update(newly_processed_messages)
        save_seen_ids(PROCESSED_MESSAGE_IDS_FILE, processed_message_ids)
        save_seen_ids(PROCESSED_JOB_IDS_FILE, processed_job_keys)
        return 0

    deduped_sources = {}
    for source in link_sources:
        key = source["dedupe_key"]
        if key not in deduped_sources:
            deduped_sources[key] = {
                "canonical_url": source["canonical_url"],
                "job_id": source["job_id"],
                "source_records": [],
            }
        deduped_sources[key]["source_records"].append(source)

    dedupe_items = list(deduped_sources.items())
    if MAX_JOB_URLS > 0 and len(dedupe_items) > MAX_JOB_URLS:
        print(
            f"⚠️  Capping scrape list from {len(dedupe_items)} to {MAX_JOB_URLS} "
            "URLs (MAX_JOB_URLS)."
        )
        dedupe_items = dedupe_items[:MAX_JOB_URLS]

    selected_dedupe_sources = dict(dedupe_items)
    urls_to_scrape = [item["canonical_url"] for item in selected_dedupe_sources.values()]
    print(f"\n✓ Unique new LinkedIn job URLs: {len(urls_to_scrape)}")
    for sample_url in urls_to_scrape[:5]:
        print(f"  - {sample_url}")

    print(f"\n[6/6] Scraping {len(urls_to_scrape)} job postings...")
    jobs = scrape_jobs_sync(urls_to_scrape)

    jobs_by_url = {job.get("url"): job for job in jobs}
    enriched_jobs = []

    for dedupe_key, info in selected_dedupe_sources.items():
        canonical_url = info["canonical_url"]
        job_data = jobs_by_url.get(canonical_url, {"url": canonical_url, "error": "No result returned"})
        merged = job_data.copy()
        merged["job_id"] = merged.get("job_id") or info.get("job_id")
        merged["source_email_ids"] = sorted({r["message_id"] for r in info["source_records"]})
        merged["source_tags"] = sorted(
            {tag for r in info["source_records"] for tag in r.get("matched_tags", [])}
        )
        merged["source_records"] = info["source_records"]
        enriched_jobs.append(merged)

    successful_jobs = [j for j in enriched_jobs if j.get("title")]
    failed_jobs = [j for j in enriched_jobs if not j.get("title")]

    print(f"\n✓ Successfully scraped: {len(successful_jobs)}")
    print(f"✗ Failed to scrape: {len(failed_jobs)}")

    processed_message_ids.update(newly_processed_messages)
    processed_job_keys.update(selected_dedupe_sources.keys())
    save_seen_ids(PROCESSED_MESSAGE_IDS_FILE, processed_message_ids)
    save_seen_ids(PROCESSED_JOB_IDS_FILE, processed_job_keys)

    print("\n" + "=" * 60)
    print("Saving Results")
    print("=" * 60)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if SAVE_JSON:
        json_file = OUTPUT_DIR / f"jobs_{timestamp}.json"
        payload = {
            "metadata": {
                "scraped_at": datetime.now().isoformat(),
                "gmail_label": GMAIL_LABEL,
                "gmail_query": gmail_query,
                "configured_tags": JOB_TAGS,
                "emails_fetched": len(emails),
                "emails_processed_this_run": len(newly_processed_messages),
                "emails_with_job_links": len(email_metadata),
                "unique_job_urls": len(urls_to_scrape),
                "max_job_urls_limit": MAX_JOB_URLS,
                "successful_scrapes": len(successful_jobs),
                "failed_scrapes": len(failed_jobs),
            },
            "emails": email_metadata,
            "jobs": enriched_jobs,
        }
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        print(f"✓ Saved JSON: {json_file}")

    if SAVE_CSV and enriched_jobs:
        csv_file = OUTPUT_DIR / f"jobs_{timestamp}.csv"
        flattened_jobs = [flatten_job_for_csv(job) for job in enriched_jobs]
        pd.DataFrame(flattened_jobs).to_csv(csv_file, index=False, encoding="utf-8")
        print(f"✓ Saved CSV: {csv_file}")

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    for job in successful_jobs[:10]:
        title = job.get("title", "Unknown")
        company = job.get("company", "Unknown")
        location = job.get("location", "Unknown")
        tags = ", ".join(job.get("source_tags") or [])
        tag_suffix = f" | tags: {tags}" if tags else ""
        print(f"  • {title} at {company} ({location}){tag_suffix}")
    if len(successful_jobs) > 10:
        print(f"  ... and {len(successful_jobs) - 10} more")

    print("\n✓ Done! Check the 'output' folder for results.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
