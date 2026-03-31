#!/usr/bin/env python3
"""Upload a pipeline run summary row to Google Sheets."""

from __future__ import annotations

import argparse
import json
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

DEFAULT_COLUMNS = [
    "run_timestamp",
    "source",
    "jobs_discovered",
    "jobs_green",
    "jobs_yellow",
    "jobs_red",
    "contacts_found",
    "drafts_created",
    "errors",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload a pipeline run summary row to Google Sheets."
    )
    parser.add_argument("--json", default="", help="Path to run summary JSON (optional)")
    parser.add_argument("--sheet", required=True, help="Google Sheet URL or spreadsheet ID")
    parser.add_argument("--tab", default="pipeline_runs", help="Target sheet tab name")
    # Run metadata (used if --json is not provided)
    parser.add_argument("--source", default="", help="Run source (gmail/daily_report/manual)")
    parser.add_argument("--jobs-discovered", type=int, default=0)
    parser.add_argument("--jobs-green", type=int, default=0)
    parser.add_argument("--jobs-yellow", type=int, default=0)
    parser.add_argument("--jobs-red", type=int, default=0)
    parser.add_argument("--contacts-found", type=int, default=0)
    parser.add_argument("--drafts-created", type=int, default=0)
    parser.add_argument("--errors", default="")
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
            remainder = path[start + len(marker):]
            return remainder.split("/", 1)[0]
    return sheet


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return "; ".join(str(item) for item in value)
    return str(value)


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
                raise RuntimeError("No valid Sheets token and --non-interactive was set.")
            if not credentials_file.exists():
                raise FileNotFoundError(f"Credentials file not found: {credentials_file}")
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
    body = {"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


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
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [required_columns]},
        ).execute()
        return required_columns

    missing = [col for col in required_columns if col not in existing]
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


def append_row(service, spreadsheet_id: str, tab_name: str, row: List[str]) -> None:
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()


def main() -> int:
    args = parse_args()
    sheet_id = extract_sheet_id(args.sheet)

    # Build run record from JSON or CLI args
    if args.json and Path(args.json).exists():
        with open(args.json, "r", encoding="utf-8") as f:
            data = json.load(f)
        record = {
            "run_timestamp": data.get("run_timestamp", datetime.now(timezone.utc).replace(microsecond=0).isoformat()),
            "source": data.get("source", ""),
            "jobs_discovered": data.get("jobs_discovered", 0),
            "jobs_green": data.get("jobs_green", 0),
            "jobs_yellow": data.get("jobs_yellow", 0),
            "jobs_red": data.get("jobs_red", 0),
            "contacts_found": data.get("contacts_found", 0),
            "drafts_created": data.get("drafts_created", 0),
            "errors": data.get("errors", ""),
        }
    else:
        record = {
            "run_timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "source": args.source,
            "jobs_discovered": args.jobs_discovered,
            "jobs_green": args.jobs_green,
            "jobs_yellow": args.jobs_yellow,
            "jobs_red": args.jobs_red,
            "contacts_found": args.contacts_found,
            "drafts_created": args.drafts_created,
            "errors": args.errors,
        }

    creds = get_credentials(
        credentials_file=Path(args.credentials_file),
        token_file=Path(args.token_file),
        non_interactive=args.non_interactive,
    )
    service = build("sheets", "v4", credentials=creds)
    ensure_tab_exists(service, sheet_id, args.tab)

    columns = ensure_header(service, sheet_id, args.tab, DEFAULT_COLUMNS)
    row = [as_text(record.get(col, "")) for col in columns]
    append_row(service, sheet_id, args.tab, row)

    print(f"Logged pipeline run to '{args.tab}' in spreadsheet {sheet_id}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
