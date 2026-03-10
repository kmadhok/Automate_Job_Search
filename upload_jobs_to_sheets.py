#!/usr/bin/env python3
"""Upload scraped jobs JSON output to Google Sheets."""

from __future__ import annotations

import argparse
import json
import pickle
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List
from urllib.parse import urlparse

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

DEFAULT_COLUMNS = [
    "imported_at_utc",
    "source_file",
    "scraped_at",
    "posted_date",
    "job_id",
    "title",
    "company",
    "location",
    "employment_type",
    "salary_range",
    "url",
    "description",
    "requirements",
    "responsibilities",
    "benefits",
    "source_tags",
    "source_email_ids",
    "error",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload job JSON output to a Google Sheet tab."
    )
    parser.add_argument("--json", required=True, help="Path to jobs JSON file")
    parser.add_argument(
        "--sheet",
        required=True,
        help="Google Sheet URL or spreadsheet ID",
    )
    parser.add_argument("--tab", default="jobs", help="Target sheet tab name")
    parser.add_argument(
        "--mode",
        default="append",
        choices=["append", "replace"],
        help="Append rows or replace all tab contents",
    )
    parser.add_argument(
        "--credentials-file",
        default="credentials.json",
        help="OAuth client credentials file",
    )
    parser.add_argument(
        "--token-file",
        default="token_sheets.pickle",
        help="Path to saved Sheets OAuth token",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Do not start OAuth flow if token is missing/invalid",
    )
    return parser.parse_args()


def extract_sheet_id(sheet: str) -> str:
    if "docs.google.com/spreadsheets/d/" in sheet:
        path = urlparse(sheet).path
        marker = "/spreadsheets/d/"
        start = path.find(marker)
        if start >= 0:
            remainder = path[start + len(marker) :]
            return remainder.split("/", 1)[0]
    return sheet


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return "; ".join(as_text(item) for item in value)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def normalize_date_string(value: Any) -> str:
    text = as_text(value).strip()
    if not text:
        return ""
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.date().isoformat()
    except Exception:
        return text


def infer_posted_date(job: Dict[str, Any]) -> str:
    for key in ("posted_date", "date_posted", "datePosted", "posted_at"):
        candidate = normalize_date_string(job.get(key))
        if candidate:
            return candidate

    date_re = re.compile(r"\bposted on (\d{1,2}/\d{1,2}/\d{2,4})\b", re.IGNORECASE)
    for source in job.get("source_records", []) or []:
        if not isinstance(source, dict):
            continue
        subject = as_text(source.get("subject"))
        match = date_re.search(subject)
        if not match:
            continue
        raw = match.group(1)
        for fmt in ("%m/%d/%y", "%m/%d/%Y"):
            try:
                return datetime.strptime(raw, fmt).date().isoformat()
            except ValueError:
                continue
    return ""


def load_payload(json_path: Path) -> Dict[str, Any]:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Expected top-level JSON object")
    return data


def build_rows(payload: Dict[str, Any], source_file: str) -> List[Dict[str, str]]:
    jobs = payload.get("jobs", [])
    if not isinstance(jobs, list):
        raise ValueError("Expected 'jobs' to be a list")

    metadata = payload.get("metadata", {})
    scraped_at = ""
    if isinstance(metadata, dict):
        scraped_at = as_text(metadata.get("scraped_at"))

    imported_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows: List[Dict[str, str]] = []
    for job in jobs:
        if not isinstance(job, dict):
            continue
        record = {
            "imported_at_utc": imported_at,
            "source_file": source_file,
            "scraped_at": scraped_at,
            "posted_date": infer_posted_date(job),
            "job_id": job.get("job_id"),
            "title": job.get("title"),
            "company": job.get("company"),
            "location": job.get("location"),
            "employment_type": job.get("employment_type"),
            "salary_range": job.get("salary_range"),
            "url": job.get("url"),
            "description": job.get("description"),
            "requirements": job.get("requirements"),
            "responsibilities": job.get("responsibilities"),
            "benefits": job.get("benefits"),
            "source_tags": job.get("source_tags"),
            "source_email_ids": job.get("source_email_ids"),
            "error": job.get("error"),
        }
        rows.append({key: as_text(value) for key, value in record.items()})
    return rows


def get_credentials(
    credentials_file: Path, token_file: Path, non_interactive: bool
):
    creds = None

    if token_file.exists():
        try:
            with open(token_file, "rb") as f:
                creds = pickle.load(f)
        except Exception:
            creds = None

    if creds and hasattr(creds, "has_scopes") and not creds.has_scopes(SCOPES):
        creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception:
                creds = None

        if not creds:
            if non_interactive:
                raise RuntimeError(
                    "No valid Sheets token and --non-interactive was set."
                )
            if not credentials_file.exists():
                raise FileNotFoundError(
                    f"Credentials file not found: {credentials_file}"
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                str(credentials_file), SCOPES
            )
            creds = flow.run_local_server(port=0, open_browser=False)

        with open(token_file, "wb") as f:
            pickle.dump(creds, f)

    return creds


def ensure_tab_exists(service, spreadsheet_id: str, tab_name: str) -> None:
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing_tabs = {
        sheet["properties"]["title"] for sheet in spreadsheet.get("sheets", [])
    }
    if tab_name in existing_tabs:
        return
    request = {"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=request
    ).execute()


def chunked(rows: List[List[str]], chunk_size: int) -> Iterable[List[List[str]]]:
    for i in range(0, len(rows), chunk_size):
        yield rows[i : i + chunk_size]


def is_tab_empty(service, spreadsheet_id: str, tab_name: str) -> bool:
    resp = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"{tab_name}!1:1")
        .execute()
    )
    return not resp.get("values")


def get_header(service, spreadsheet_id: str, tab_name: str) -> List[str]:
    resp = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"{tab_name}!1:1")
        .execute()
    )
    values = resp.get("values", [])
    if not values:
        return []
    return [as_text(v).strip() for v in values[0] if as_text(v).strip()]


def ensure_header(
    service, spreadsheet_id: str, tab_name: str, required_columns: List[str]
) -> List[str]:
    existing = get_header(service, spreadsheet_id, tab_name)
    if not existing:
        append_rows(service, spreadsheet_id, tab_name, [required_columns])
        return required_columns

    missing = [column for column in required_columns if column not in existing]
    if not missing:
        return existing

    merged = existing + missing
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!1:1",
        valueInputOption="USER_ENTERED",
        body={"values": [merged]},
    ).execute()
    return merged


def clear_tab(service, spreadsheet_id: str, tab_name: str) -> None:
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id, range=f"{tab_name}!A:ZZ", body={}
    ).execute()


def append_rows(service, spreadsheet_id: str, tab_name: str, rows: List[List[str]]) -> int:
    total = 0
    for block in chunked(rows, 500):
        if not block:
            continue
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": block},
        ).execute()
        total += len(block)
    return total


def write_rows(service, spreadsheet_id: str, tab_name: str, rows: List[List[str]]) -> int:
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A1",
        valueInputOption="USER_ENTERED",
        body={"values": rows},
    ).execute()
    return len(rows)


def materialize_rows(records: List[Dict[str, str]], columns: List[str]) -> List[List[str]]:
    return [[record.get(column, "") for column in columns] for record in records]


def main() -> int:
    args = parse_args()
    json_path = Path(args.json)
    credentials_file = Path(args.credentials_file)
    token_file = Path(args.token_file)
    spreadsheet_id = extract_sheet_id(args.sheet)

    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    payload = load_payload(json_path)
    records = build_rows(payload, source_file=json_path.name)
    if not records:
        print("No jobs found in JSON. Nothing to upload.")
        return 0

    creds = get_credentials(credentials_file, token_file, args.non_interactive)
    service = build("sheets", "v4", credentials=creds)
    ensure_tab_exists(service, spreadsheet_id, args.tab)

    uploaded = 0
    if args.mode == "replace":
        clear_tab(service, spreadsheet_id, args.tab)
        rows = materialize_rows(records, DEFAULT_COLUMNS)
        all_rows = [DEFAULT_COLUMNS] + rows
        uploaded = write_rows(service, spreadsheet_id, args.tab, all_rows) - 1
    else:
        columns = ensure_header(service, spreadsheet_id, args.tab, DEFAULT_COLUMNS)
        rows = materialize_rows(records, columns)
        uploaded = append_rows(service, spreadsheet_id, args.tab, rows)

    print(f"Uploaded {uploaded} job rows to '{args.tab}' in spreadsheet {spreadsheet_id}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
