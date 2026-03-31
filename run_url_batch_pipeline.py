#!/usr/bin/env python3
"""
Run the URL-batch job search pipeline end-to-end.

Workflow:
  1. Load LinkedIn job URLs from a text file
  2. Scrape job postings
  3. Find outreach contacts (managers/recruiters/team members)
  4. Discover email addresses (Hunter.io + pattern guessing)
  5. Generate personalized outreach messages
  6. Create Gmail drafts (one per contact)
  7. Upload everything to Google Sheets for tracking

Usage:
  python run_url_batch_pipeline.py \\
    --urls job_urls.txt \\
    --sheet "https://docs.google.com/spreadsheets/d/YOUR_ID/edit" \\
    --resume-pdf /path/to/resume.pdf

  # Dry run (no Gmail drafts created):
  python run_url_batch_pipeline.py \\
    --urls job_urls.txt \\
    --sheet "https://docs.google.com/spreadsheets/d/YOUR_ID/edit" \\
    --dry-run
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the URL-batch job search pipeline: scrape -> contacts -> emails -> messages -> drafts."
    )

    # ── Core inputs ──────────────────────────────────────────────────
    parser.add_argument(
        "--urls",
        required=True,
        help="Path to text file with LinkedIn job URLs (one per line)",
    )
    parser.add_argument(
        "--sheet",
        default="",
        help="Google Sheet URL or spreadsheet ID for tracking (optional)",
    )

    # ── Sheet tab names ──────────────────────────────────────────────
    parser.add_argument("--jobs-tab", default="jobs", help="Sheet tab for scraped jobs")
    parser.add_argument("--contacts-tab", default="job_contacts", help="Tab for outreach contacts")
    parser.add_argument("--messages-tab", default="job_messages", help="Tab for outreach messages")
    parser.add_argument("--drafts-tab", default="gmail_drafts", help="Tab for Gmail draft tracking")

    # ── Sheet modes ──────────────────────────────────────────────────
    parser.add_argument("--jobs-mode", default="append", choices=["append", "replace"])
    parser.add_argument("--contacts-mode", default="append", choices=["append", "replace"])
    parser.add_argument("--messages-mode", default="append", choices=["append", "replace"])
    parser.add_argument("--drafts-mode", default="append", choices=["append", "replace"])

    # ── Contact discovery ────────────────────────────────────────────
    parser.add_argument("--per-role", type=int, default=3, help="Contacts per role type")
    parser.add_argument(
        "--serper-api-key",
        default=os.getenv("SERPER_DEV_API_KEY", "") or os.getenv("SERPER_API_KEY", ""),
        help="Serper API key for contact discovery",
    )

    # ── Email discovery ──────────────────────────────────────────────
    parser.add_argument(
        "--hunter-api-key",
        default=os.getenv("HUNTER_IO_API_KEY", ""),
        help="Hunter.io API key for email discovery",
    )

    # ── Message generation ───────────────────────────────────────────
    parser.add_argument(
        "--resume-pdf",
        default=os.getenv("OUTREACH_RESUME_PDF", ""),
        help="Path to resume PDF for profile summary extraction",
    )
    parser.add_argument(
        "--profile-summary",
        default=os.getenv("OUTREACH_PROFILE_SUMMARY", ""),
        help="Short profile summary for outreach messages",
    )

    # ── Gmail drafts ─────────────────────────────────────────────────
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the full pipeline but skip creating actual Gmail drafts",
    )
    parser.add_argument(
        "--max-drafts",
        type=int,
        default=0,
        help="Max number of Gmail drafts to create (0 = unlimited)",
    )

    # ── Skip options ─────────────────────────────────────────────────
    parser.add_argument("--skip-contacts", action="store_true", help="Skip contact discovery")
    parser.add_argument("--skip-emails", action="store_true", help="Skip email discovery")
    parser.add_argument("--skip-messages", action="store_true", help="Skip message generation")
    parser.add_argument("--skip-drafts", action="store_true", help="Skip Gmail draft creation")
    parser.add_argument("--skip-sheets", action="store_true", help="Skip all Google Sheets uploads")

    # ── Sheets auth ──────────────────────────────────────────────────
    parser.add_argument(
        "--non-interactive-sheets",
        action="store_true",
        help="Pass --non-interactive to all sheet upload scripts",
    )

    # ── Existing files (resume from mid-pipeline) ────────────────────
    parser.add_argument("--jobs-json", default="", help="Use existing jobs JSON instead of scraping")
    parser.add_argument("--contacts-json", default="", help="Use existing contacts JSON")
    parser.add_argument("--emails-json", default="", help="Use existing emails JSON")
    parser.add_argument("--messages-json", default="", help="Use existing messages JSON")

    return parser.parse_args()


def run_step(cmd: List[str], env: dict, label: str, redact: bool = False) -> None:
    """Run a subprocess step with logging."""
    printable = cmd[:]
    if redact:
        for i, token in enumerate(printable[:-1]):
            if token in {"--serper-api-key", "--hunter-api-key", "--api-key"}:
                printable[i + 1] = "***REDACTED***"

    divider = "=" * 70
    print(f"\n{divider}")
    print(f"STEP: {label}")
    print(f"{divider}")
    print("+ " + " ".join(shlex.quote(part) for part in printable))

    subprocess.run(cmd, check=True, env=env)


def maybe_add_non_interactive(args: argparse.Namespace, cmd: List[str]) -> List[str]:
    if args.non_interactive_sheets:
        return cmd + ["--non-interactive"]
    return cmd


def main() -> int:
    args = parse_args()

    base_dir = Path(__file__).resolve().parent
    output_dir = (base_dir / "output").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    env = os.environ.copy()

    print("=" * 70)
    print("URL-BATCH JOB SEARCH PIPELINE")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # ── Step 1: Scrape job postings from URLs ────────────────────────
    if args.jobs_json:
        jobs_json = Path(args.jobs_json).resolve()
        if not jobs_json.exists():
            raise FileNotFoundError(f"Jobs JSON not found: {jobs_json}")
        print(f"\nUsing existing jobs JSON: {jobs_json}")
    else:
        urls_file = Path(args.urls)
        if not urls_file.is_absolute():
            urls_file = (base_dir / urls_file).resolve()
        if not urls_file.exists():
            raise FileNotFoundError(f"URL file not found: {urls_file}")

        jobs_json = (output_dir / f"jobs_{run_stamp}.json").resolve()
        jobs_csv = (output_dir / f"jobs_{run_stamp}.csv").resolve()

        # Use scrape_jobs.py (a small wrapper we invoke) - but since main.py
        # handles Gmail-based flow, we need a direct scrape approach.
        # We'll call the scraping logic directly via a small inline script.
        scrape_script = f"""
import json, sys
sys.path.insert(0, {str(base_dir)!r})
from load_job_urls import load_urls_from_file
from job_scraper import scrape_jobs_sync
from pathlib import Path
from datetime import datetime

urls_file = Path({str(urls_file)!r})
processed_ids_path = Path({str(output_dir)!r}) / "processed_job_ids.json"

url_records = load_urls_from_file(urls_file, processed_ids_path, skip_processed=True)
if not url_records:
    print("No new URLs to process.")
    # Write empty output
    payload = {{"metadata": {{"scraped_at": datetime.now().isoformat(), "total_urls": 0, "successful_scrapes": 0, "failed_scrapes": 0}}, "jobs": []}}
    with open({str(jobs_json)!r}, "w") as f:
        json.dump(payload, f, indent=2)
    sys.exit(0)

urls = [r["canonical_url"] for r in url_records]
print(f"Scraping {{len(urls)}} job URLs...")
jobs = scrape_jobs_sync(urls)

successful = [j for j in jobs if not j.get("error")]
failed = [j for j in jobs if j.get("error")]

payload = {{
    "metadata": {{
        "scraped_at": datetime.now().isoformat(),
        "source_file": urls_file.name,
        "total_urls": len(urls),
        "successful_scrapes": len(successful),
        "failed_scrapes": len(failed),
    }},
    "jobs": successful,
}}

with open({str(jobs_json)!r}, "w") as f:
    json.dump(payload, f, indent=2, ensure_ascii=False)

# Also save CSV
if successful:
    import csv
    csv_path = {str(jobs_csv)!r}
    keys = set()
    for j in successful:
        keys.update(j.keys())
    keys = sorted(keys)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for j in successful:
            row = {{}}
            for k in keys:
                v = j.get(k)
                if isinstance(v, list):
                    v = "; ".join(str(x) for x in v)
                row[k] = v
            writer.writerows([row])

# Update processed IDs
from load_job_urls import load_processed_job_ids, save_processed_job_ids
existing_ids = load_processed_job_ids(processed_ids_path)
for j in successful:
    jid = j.get("job_id")
    if jid:
        existing_ids.add(str(jid))
save_processed_job_ids(processed_ids_path, existing_ids)

print(f"Scraped {{len(successful)}} jobs successfully, {{len(failed)}} failed.")
print(f"Saved: {str(jobs_json)!r}")
"""
        run_step(
            [sys.executable, "-c", scrape_script],
            env=env,
            label="Scrape job postings from URLs",
        )

    # ── Step 2: Upload jobs to Sheets ────────────────────────────────
    if args.sheet and not args.skip_sheets:
        upload_jobs_cmd = [
            sys.executable,
            str((base_dir / "upload_jobs_to_sheets.py").resolve()),
            "--json", str(jobs_json),
            "--sheet", args.sheet,
            "--tab", args.jobs_tab,
            "--mode", args.jobs_mode,
        ]
        run_step(
            maybe_add_non_interactive(args, upload_jobs_cmd),
            env=env,
            label="Upload jobs to Google Sheets",
        )

    # ── Step 3: Find outreach contacts ───────────────────────────────
    if args.contacts_json:
        outreach_json = Path(args.contacts_json).resolve()
        if not outreach_json.exists():
            raise FileNotFoundError(f"Contacts JSON not found: {outreach_json}")
        print(f"\nUsing existing contacts JSON: {outreach_json}")
    elif args.skip_contacts:
        print("\nSkipping contact discovery (--skip-contacts)")
        outreach_json = None
    else:
        if not args.serper_api_key:
            raise SystemExit(
                "Missing SERPER_DEV_API_KEY (or SERPER_API_KEY). "
                "Set it in .env or pass --serper-api-key to discover contacts."
            )

        outreach_json = (output_dir / f"outreach_targets_{run_stamp}.json").resolve()
        outreach_csv = (output_dir / f"outreach_targets_{run_stamp}.csv").resolve()

        find_contacts_cmd = [
            sys.executable,
            str((base_dir / "find_outreach_contacts.py").resolve()),
            "--json", str(jobs_json),
            "--fetch",
            "--per-role", str(max(1, args.per_role)),
            "--serper-api-key", args.serper_api_key,
            "--output-json", str(outreach_json),
            "--output-csv", str(outreach_csv),
        ]
        run_step(find_contacts_cmd, env=env, label="Find outreach contacts", redact=True)

        # Upload contacts to Sheets
        if args.sheet and not args.skip_sheets:
            upload_contacts_cmd = [
                sys.executable,
                str((base_dir / "upload_contacts_to_sheets.py").resolve()),
                "--json", str(outreach_json),
                "--sheet", args.sheet,
                "--tab", args.contacts_tab,
                "--mode", args.contacts_mode,
            ]
            run_step(
                maybe_add_non_interactive(args, upload_contacts_cmd),
                env=env,
                label="Upload contacts to Google Sheets",
            )

    # ── Step 4: Find email addresses ─────────────────────────────────
    if args.emails_json:
        emails_json = Path(args.emails_json).resolve()
        if not emails_json.exists():
            raise FileNotFoundError(f"Emails JSON not found: {emails_json}")
        print(f"\nUsing existing emails JSON: {emails_json}")
    elif args.skip_emails or not outreach_json:
        print("\nSkipping email discovery")
        emails_json = None
    else:
        emails_json = (output_dir / f"contact_emails_{run_stamp}.json").resolve()
        emails_csv = (output_dir / f"contact_emails_{run_stamp}.csv").resolve()

        find_emails_cmd = [
            sys.executable,
            str((base_dir / "find_contact_emails.py").resolve()),
            "--json", str(outreach_json),
            "--output-json", str(emails_json),
            "--output-csv", str(emails_csv),
        ]
        if args.hunter_api_key:
            find_emails_cmd.extend(["--hunter-api-key", args.hunter_api_key])
        run_step(find_emails_cmd, env=env, label="Discover email addresses", redact=True)

    # ── Step 5: Generate outreach messages ───────────────────────────
    if args.messages_json:
        messages_json = Path(args.messages_json).resolve()
        if not messages_json.exists():
            raise FileNotFoundError(f"Messages JSON not found: {messages_json}")
        print(f"\nUsing existing messages JSON: {messages_json}")
    elif args.skip_messages or not outreach_json:
        print("\nSkipping message generation")
        messages_json = None
    else:
        messages_json = (output_dir / f"outreach_messages_{run_stamp}.json").resolve()
        messages_csv = (output_dir / f"outreach_messages_{run_stamp}.csv").resolve()

        gen_messages_cmd = [
            sys.executable,
            str((base_dir / "generate_outreach_messages.py").resolve()),
            "--json", str(outreach_json),
            "--max-per-role", str(max(1, args.per_role)),
            "--output-json", str(messages_json),
            "--output-csv", str(messages_csv),
        ]
        if args.profile_summary:
            gen_messages_cmd.extend(["--profile-summary", args.profile_summary])
        if args.resume_pdf:
            gen_messages_cmd.extend(["--resume-pdf", args.resume_pdf])
        run_step(gen_messages_cmd, env=env, label="Generate outreach messages")

        # Upload messages to Sheets
        if args.sheet and not args.skip_sheets:
            upload_messages_cmd = [
                sys.executable,
                str((base_dir / "upload_messages_to_sheets.py").resolve()),
                "--json", str(messages_json),
                "--sheet", args.sheet,
                "--tab", args.messages_tab,
                "--mode", args.messages_mode,
            ]
            run_step(
                maybe_add_non_interactive(args, upload_messages_cmd),
                env=env,
                label="Upload messages to Google Sheets",
            )

    # ── Step 6: Create Gmail drafts ──────────────────────────────────
    drafts_json = None
    if args.skip_drafts or not messages_json or not emails_json:
        if not args.skip_drafts and (not messages_json or not emails_json):
            print("\nSkipping Gmail drafts (missing messages or emails data)")
        else:
            print("\nSkipping Gmail drafts (--skip-drafts)")
    else:
        drafts_json = (output_dir / f"drafts_{run_stamp}.json").resolve()

        create_drafts_cmd = [
            sys.executable,
            str((base_dir / "create_gmail_drafts.py").resolve()),
            "--messages-json", str(messages_json),
            "--emails-json", str(emails_json),
            "--output-json", str(drafts_json),
        ]
        if args.max_drafts > 0:
            create_drafts_cmd.extend(["--max-drafts", str(args.max_drafts)])
        if args.dry_run:
            create_drafts_cmd.append("--dry-run")

        run_step(create_drafts_cmd, env=env, label="Create Gmail drafts")

        # Upload draft tracking to Sheets
        if args.sheet and not args.skip_sheets:
            upload_drafts_cmd = [
                sys.executable,
                str((base_dir / "upload_drafts_to_sheets.py").resolve()),
                "--json", str(drafts_json),
                "--sheet", args.sheet,
                "--tab", args.drafts_tab,
                "--mode", args.drafts_mode,
            ]
            run_step(
                maybe_add_non_interactive(args, upload_drafts_cmd),
                env=env,
                label="Upload draft tracking to Google Sheets",
            )

    # ── Summary ──────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nOutput files:")
    print(f"  Jobs:     {jobs_json}")
    if outreach_json:
        print(f"  Contacts: {outreach_json}")
    if emails_json:
        print(f"  Emails:   {emails_json}")
    if messages_json:
        print(f"  Messages: {messages_json}")
    if drafts_json:
        print(f"  Drafts:   {drafts_json}")
    if args.sheet:
        print(f"\nGoogle Sheet: {args.sheet}")
    if args.dry_run:
        print("\n** DRY RUN MODE ** - No actual Gmail drafts were created.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
