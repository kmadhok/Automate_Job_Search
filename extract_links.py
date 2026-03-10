#!/usr/bin/env python3
"""
Extract links from Gmail emails and save to CSV.

This script:
1. Authenticates with Gmail API
2. Fetches emails from a specified label
3. Extracts links from email content
4. Saves unique links to a CSV file with extraction timestamp
"""

import csv
from datetime import datetime
from pathlib import Path

from gmail_auth import get_gmail_service, get_label_id
from email_processor import get_emails_by_label, extract_email_content, extract_links
from config import GMAIL_LABEL, OUTPUT_DIR


def main():
    """Main execution function for link extraction."""
    print("=" * 60)
    print("Email Link Extraction")
    print("=" * 60)

    # Step 1: Authenticate with Gmail
    print("\n[1/4] Authenticating with Gmail...")
    try:
        service = get_gmail_service()
        print("✓ Successfully authenticated")
    except FileNotFoundError as e:
        print(f"❌ {e}")
        return
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return

    # Step 2: Get label ID
    print(f"\n[2/4] Finding label '{GMAIL_LABEL}'...")
    label_id = get_label_id(service, GMAIL_LABEL)

    if not label_id:
        print(f"❌ Label '{GMAIL_LABEL}' not found!")
        print("\nAvailable labels:")
        try:
            results = service.users().labels().list(userId='me').execute()
            labels = results.get('labels', [])
            for label in labels:
                print(f"  - {label['name']}")
        except:
            pass
        print("\nPlease update GMAIL_LABEL in config.py")
        return

    print(f"✓ Found label: {label_id}")

    # Step 3: Fetch emails
    print(f"\n[3/4] Fetching emails from '{GMAIL_LABEL}'...")
    emails = get_emails_by_label(service, label_id)

    if not emails:
        print("❌ No emails found in this label")
        return

    print(f"✓ Found {len(emails)} emails")

    # Step 4: Extract links from emails
    print("\n[4/4] Extracting links from emails...")
    all_links = []

    for i, email in enumerate(emails):
        email_content = extract_email_content(email)
        links = extract_links(email_content)

        print(f"  Email {i+1}/{len(emails)}: {email_content['subject'][:50]}...")
        print(f"    Found {len(links)} links")

        all_links.extend(links)

    # Remove duplicates
    unique_links = list(set(all_links))
    print(f"\n✓ Total unique links: {len(unique_links)}")

    if not unique_links:
        print("❌ No links found in emails")
        return

    # Show sample of links
    print("\nSample of extracted links:")
    for link in unique_links[:5]:
        print(f"  - {link[:80]}...")
    if len(unique_links) > 5:
        print(f"  ... and {len(unique_links) - 5} more")

    # Save to CSV
    print("\n" + "=" * 60)
    print("Saving Results")
    print("=" * 60)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = OUTPUT_DIR / f"links_{timestamp}.csv"
    extraction_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['url', 'extracted_at'])

        for url in unique_links:
            writer.writerow([url, extraction_date])

    print(f"✓ Saved {len(unique_links)} unique links to: {csv_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Emails processed: {len(emails)}")
    print(f"  Total links found: {len(all_links)}")
    print(f"  Unique links: {len(unique_links)}")
    print(f"  Output file: {csv_file.name}")
    print(f"\n✓ Done! Use this file with scrape_jobs.py to scrape job details.")


if __name__ == "__main__":
    main()
