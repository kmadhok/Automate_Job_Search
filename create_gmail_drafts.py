#!/usr/bin/env python3
"""Create Gmail drafts from outreach messages with contact emails."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from gmail_auth import create_gmail_draft, get_gmail_service


ROLE_TYPES = ("manager", "recruiter", "team_member")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create Gmail drafts from outreach messages + contact emails."
    )
    parser.add_argument(
        "--messages-json",
        required=True,
        help="Path to outreach_messages JSON file",
    )
    parser.add_argument(
        "--emails-json",
        required=True,
        help="Path to contact_emails JSON file",
    )
    parser.add_argument(
        "--max-drafts",
        type=int,
        default=0,
        help="Max total drafts to create (0 = unlimited)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be created without actually creating drafts",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Output JSON path (default: output/drafts_<timestamp>.json)",
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


def build_email_lookup(emails_payload: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """
    Build a lookup from (job_id, name, role_type) -> email info.

    Uses the contact_emails JSON to create a fast lookup table.
    """
    lookup: Dict[str, Dict[str, str]] = {}

    for job in emails_payload.get("jobs", []):
        job_id = as_text(job.get("job_id"))
        candidates = job.get("candidates", {})

        for role_type in ROLE_TYPES:
            for person in candidates.get(role_type, []):
                name = as_text(person.get("name"))
                email = as_text(person.get("email"))
                if not email:
                    continue

                key = f"{job_id}|{name}|{role_type}"
                lookup[key] = {
                    "email": email,
                    "email_confidence": person.get("email_confidence", 0),
                    "email_source": as_text(person.get("email_source")),
                }

    return lookup


def create_drafts(
    messages_payload: Dict[str, Any],
    email_lookup: Dict[str, Dict[str, str]],
    service,
    max_drafts: int,
    dry_run: bool,
) -> List[Dict[str, Any]]:
    """
    Create Gmail drafts for each message that has an associated email.

    Returns list of draft records for tracking.
    """
    messages = messages_payload.get("messages", [])
    draft_records: List[Dict[str, Any]] = []
    created_count = 0
    skipped_no_email = 0
    errors = 0

    for msg in messages:
        if max_drafts > 0 and created_count >= max_drafts:
            print(f"\nReached max drafts limit ({max_drafts}). Stopping.")
            break

        job_id = as_text(msg.get("job_id"))
        name = as_text(msg.get("name"))
        role_type = as_text(msg.get("role_type"))
        subject = as_text(msg.get("subject"))
        body = as_text(msg.get("message_body"))
        company = as_text(msg.get("company"))
        job_title = as_text(msg.get("job_title"))

        # Look up email
        key = f"{job_id}|{name}|{role_type}"
        email_info = email_lookup.get(key)

        if not email_info or not email_info.get("email"):
            skipped_no_email += 1
            continue

        to_email = email_info["email"]
        email_confidence = email_info.get("email_confidence", 0)
        email_source = email_info.get("email_source", "unknown")

        record = {
            "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "job_id": job_id,
            "job_title": job_title,
            "company": company,
            "contact_name": name,
            "contact_email": to_email,
            "role_type": role_type,
            "email_source": email_source,
            "email_confidence": email_confidence,
            "subject": subject,
            "draft_id": None,
            "status": "pending",
            "error_message": "",
        }

        if dry_run:
            print(f"  [DRY RUN] Would create draft: To={to_email}, Subject={subject}")
            record["status"] = "dry_run"
            record["draft_id"] = "dry_run"
            draft_records.append(record)
            created_count += 1
            continue

        print(f"  Creating draft: To={to_email} ({role_type}), Subject={subject[:60]}...")

        draft = create_gmail_draft(service, to_email, subject, body)

        if draft:
            draft_id = draft.get("id", "")
            record["draft_id"] = draft_id
            record["status"] = "created"
            created_count += 1
            print(f"    -> Draft created (ID: {draft_id})")
        else:
            record["status"] = "error"
            record["error_message"] = "Gmail API returned None"
            errors += 1
            print(f"    -> Failed to create draft")

        draft_records.append(record)

        # Small delay between draft creations
        time.sleep(0.3)

    print(f"\nDraft creation summary:")
    print(f"  Created: {created_count}")
    print(f"  Skipped (no email): {skipped_no_email}")
    print(f"  Errors: {errors}")

    return draft_records


def main() -> int:
    args = parse_args()

    messages_path = Path(args.messages_json)
    emails_path = Path(args.emails_json)

    if not messages_path.exists():
        raise FileNotFoundError(f"Messages file not found: {messages_path}")
    if not emails_path.exists():
        raise FileNotFoundError(f"Emails file not found: {emails_path}")

    with open(messages_path, "r", encoding="utf-8") as f:
        messages_payload = json.load(f)
    with open(emails_path, "r", encoding="utf-8") as f:
        emails_payload = json.load(f)

    email_lookup = build_email_lookup(emails_payload)
    print(f"Loaded {len(email_lookup)} contact emails from {emails_path.name}")

    total_messages = len(messages_payload.get("messages", []))
    print(f"Processing {total_messages} outreach messages...")

    # Authenticate Gmail (with compose scope)
    service = None
    if not args.dry_run:
        print("Authenticating with Gmail...")
        service = get_gmail_service()
        print("Gmail authenticated successfully.")

    draft_records = create_drafts(
        messages_payload=messages_payload,
        email_lookup=email_lookup,
        service=service,
        max_drafts=args.max_drafts,
        dry_run=args.dry_run,
    )

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_json = messages_path.parent / f"drafts_{timestamp}.json"
    out_json = Path(args.output_json) if args.output_json else default_json

    report = {
        "metadata": {
            "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "source_messages_file": str(messages_path),
            "source_emails_file": str(emails_path),
            "drafts_created": sum(1 for r in draft_records if r["status"] == "created"),
            "drafts_dry_run": sum(1 for r in draft_records if r["status"] == "dry_run"),
            "drafts_errored": sum(1 for r in draft_records if r["status"] == "error"),
            "dry_run": args.dry_run,
        },
        "drafts": draft_records,
    }

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"\nSaved draft tracking JSON: {out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
