#!/usr/bin/env python3
"""
Scrape job descriptions from a CSV file of URLs.

This script:
1. Reads URLs from a CSV file (created by extract_links.py)
2. Scrapes job descriptions using LLM-powered extraction
3. Saves results to JSON and CSV files

Usage:
    python scrape_jobs.py <path_to_links_csv> [--limit N]

Examples:
    python scrape_jobs.py output/links_20250125_123456.csv
    python scrape_jobs.py output/links_20250125_123456.csv --limit 10
"""

import sys
import csv
import json
import argparse
import pandas as pd
from datetime import datetime
from pathlib import Path

from job_scraper import scrape_jobs_sync
from config import OUTPUT_DIR, SAVE_JSON, SAVE_CSV, OPENAI_API_KEY


def load_urls_from_csv(csv_file_path):
    """
    Load URLs from a CSV file.

    Args:
        csv_file_path: Path to CSV file containing URLs

    Returns:
        list: List of URLs
    """
    urls = []

    with open(csv_file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        # Check if required column exists
        if 'url' not in reader.fieldnames:
            raise ValueError("CSV file must have a 'url' column")

        for row in reader:
            url = row['url'].strip()
            if url:
                urls.append(url)

    return urls


def main():
    """Main execution function for job scraping."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Scrape job descriptions from a CSV file of URLs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scrape_jobs.py output/links_20250125_123456.csv
  python scrape_jobs.py output/links_20250125_123456.csv --limit 10
        """
    )
    parser.add_argument('csv_file', help='Path to CSV file containing URLs')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of URLs to scrape (useful for testing)')

    args = parser.parse_args()

    print("=" * 60)
    print("Job Description Scraper")
    print("=" * 60)

    # Check for OpenAI API key
    if not OPENAI_API_KEY:
        print("\n❌ ERROR: OPENAI_API_KEY not found!")
        print("Please create a .env file with your OpenAI API key:")
        print("OPENAI_API_KEY=your-api-key-here")
        return

    csv_file_path = Path(args.csv_file)

    # Validate file exists
    if not csv_file_path.exists():
        print(f"\n❌ ERROR: File not found: {csv_file_path}")
        return

    if not csv_file_path.suffix == '.csv':
        print(f"\n❌ ERROR: File must be a CSV file: {csv_file_path}")
        return

    # Load URLs
    print(f"\n[1/2] Loading URLs from {csv_file_path.name}...")
    try:
        urls = load_urls_from_csv(csv_file_path)
    except ValueError as e:
        print(f"❌ {e}")
        return
    except Exception as e:
        print(f"❌ Error reading CSV file: {e}")
        return

    if not urls:
        print("❌ No URLs found in CSV file")
        return

    print(f"✓ Loaded {len(urls)} URLs")

    # Apply limit if specified
    total_urls = len(urls)
    if args.limit and args.limit < len(urls):
        urls = urls[:args.limit]
        print(f"⚠️  Limiting to first {args.limit} URLs for testing")

    # Show sample of URLs
    print("\nSample URLs to scrape:")
    for url in urls[:5]:
        print(f"  - {url[:80]}...")
    if len(urls) > 5:
        print(f"  ... and {len(urls) - 5} more")

    # Scrape job postings
    print(f"\n[2/2] Scraping {len(urls)} job postings with LLM extraction...")
    print("(This may take a while...)")

    jobs = scrape_jobs_sync(urls)

    successful_jobs = [j for j in jobs if 'error' not in j or j.get('title')]
    failed_jobs = [j for j in jobs if 'error' in j and not j.get('title')]

    print(f"\n✓ Successfully scraped: {len(successful_jobs)}")
    print(f"✗ Failed to scrape: {len(failed_jobs)}")

    # Save results
    print("\n" + "=" * 60)
    print("Saving Results")
    print("=" * 60)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save JSON
    if SAVE_JSON:
        json_file = OUTPUT_DIR / f"jobs_{timestamp}.json"
        metadata = {
            'scraped_at': datetime.now().isoformat(),
            'source_file': str(csv_file_path),
            'total_urls_in_file': total_urls,
            'urls_scraped': len(urls),
            'successful_scrapes': len(successful_jobs),
            'failed_scrapes': len(failed_jobs)
        }
        if args.limit:
            metadata['limit_applied'] = args.limit

        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump({
                'metadata': metadata,
                'jobs': jobs
            }, f, indent=2, ensure_ascii=False)
        print(f"✓ Saved JSON: {json_file}")

    # Save CSV
    if SAVE_CSV and jobs:
        csv_file = OUTPUT_DIR / f"jobs_{timestamp}.csv"

        # Flatten job data for CSV
        flattened_jobs = []
        for job in jobs:
            flat_job = job.copy()

            # Convert lists to comma-separated strings
            if isinstance(flat_job.get('requirements'), list):
                flat_job['requirements'] = '; '.join(flat_job['requirements'])
            if isinstance(flat_job.get('responsibilities'), list):
                flat_job['responsibilities'] = '; '.join(flat_job['responsibilities'])
            if isinstance(flat_job.get('benefits'), list):
                flat_job['benefits'] = '; '.join(flat_job['benefits'])

            flattened_jobs.append(flat_job)

        df = pd.DataFrame(flattened_jobs)
        df.to_csv(csv_file, index=False, encoding='utf-8')
        print(f"✓ Saved CSV: {csv_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)

    if successful_jobs:
        print("\nSuccessfully extracted jobs:")
        for job in successful_jobs[:10]:  # Show first 10
            title = job.get('title', 'Unknown')
            company = job.get('company', 'Unknown')
            location = job.get('location', 'Unknown')
            print(f"  • {title} at {company} ({location})")

        if len(successful_jobs) > 10:
            print(f"  ... and {len(successful_jobs) - 10} more")

    if failed_jobs:
        print(f"\n⚠️  {len(failed_jobs)} URLs failed to scrape")
        print("Check the JSON output for error details")

    print(f"\n✓ Done! Check the 'output' folder for results.")


if __name__ == "__main__":
    main()
