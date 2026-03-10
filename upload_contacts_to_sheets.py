#!/usr/bin/env python3
"""Upload outreach contact candidates to Google Sheets."""

from __future__ import annotations

import argparse
import json
import pickle
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
    "job_id",
    "job_title",
    "company",
    "location",
    "team_guess",
    "team_confidence",
    "team_source",
    "role_type",
    "rank",
    "name",
    "headline",
    "linkedin_url",
    "score",
    "query",
]

ROLE_TYPES = ("manager", "recruiter", "team_member")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload outreach candidate JSON to a Google Sheet tab."
    )
    parser.add_argument("--json", required=True, help="Path to outreach_targets JSON file")
    parser.add_argument("--sheet", required=True, help="Google Sheet URL or spreadsheet ID")
    parser.add_argument("--tab", default="job_contacts", help="Target sheet tab name")
    parser.add_argument(
        "--mode",
        default="replace",
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


def load_payload(json_path: Path) -> Dict[str, Any]:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Expected top-level JSON object")
    return data


def build_records(payload: Dict[str, Any], source_file: str) -> List[Dict[str, str]]:
    jobs = payload.get("jobs", [])
    if not isinstance(jobs, list):
        raise ValueError("Expected 'jobs' to be a list")

    imported_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows: List[Dict[str, str]] = []

    for job in jobs:
        if not isinstance(job, dict):
            continue
        candidates = job.get("candidates", {})
        queries = job.get("queries", {})
        if not isinstance(candidates, dict):
            continue

        job_base = {
            "imported_at_utc": imported_at,
            "source_file": source_file,
            "job_id": job.get("job_id"),
            "job_title": job.get("title"),
            "company": job.get("company"),
            "location": job.get("location"),
            "team_guess": job.get("team_guess"),
            "team_confidence": job.get("team_confidence"),
            "team_source": job.get("team_source"),
        }

        for role_type in ROLE_TYPES:
            role_candidates = candidates.get(role_type, [])
            if not isinstance(role_candidates, list):
                continue
            for idx, person in enumerate(role_candidates, start=1):
                if not isinstance(person, dict):
                    continue
                row = {
                    **job_base,
                    "role_type": role_type,
                    "rank": idx,
                    "name": person.get("name"),
                    "headline": person.get("headline"),
                    "linkedin_url": person.get("linkedin_url"),
                    "score": person.get("score"),
                    "query": queries.get(role_type),
                }
                rows.append({k: as_text(v) for k, v in row.items()})

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


def chunked(rows: List[List[str]], chunk_size: int) -> Iterable[List[List[str]]]:
    for i in range(0, len(rows), chunk_size):
        yield rows[i : i + chunk_size]


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


def clear_tab(service, spreadsheet_id: str, tab_name: str) -> None:
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id, range=f"{tab_name}!A:ZZ", body={}
    ).execute()


def materialize_rows(records: List[Dict[str, str]], columns: List[str]) -> List[List[str]]:
    return [[record.get(column, "") for column in columns] for record in records]


def main() -> int:
    args = parse_args()
    json_path = Path(args.json)
    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    payload = load_payload(json_path)
    records = build_records(payload, source_file=json_path.name)
    if not records:
        print("No contact candidates in JSON. Nothing to upload.")
        return 0

    spreadsheet_id = extract_sheet_id(args.sheet)
    creds = get_credentials(
        credentials_file=Path(args.credentials_file),
        token_file=Path(args.token_file),
        non_interactive=args.non_interactive,
    )
    service = build("sheets", "v4", credentials=creds)

    ensure_tab_exists(service, spreadsheet_id, args.tab)
    rows = materialize_rows(records, DEFAULT_COLUMNS)

    if args.mode == "replace":
        clear_tab(service, spreadsheet_id, args.tab)
        uploaded = write_rows(service, spreadsheet_id, args.tab, [DEFAULT_COLUMNS] + rows) - 1
    else:
        uploaded = append_rows(service, spreadsheet_id, args.tab, rows)

    print(f"Uploaded {uploaded} contact rows to '{args.tab}' in spreadsheet {spreadsheet_id}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
