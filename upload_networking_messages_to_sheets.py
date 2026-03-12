#!/usr/bin/env python3
"""Upload networking message drafts to Google Sheets."""

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
MAX_CELL_CHARS = 49000
TRUNCATION_SUFFIX = "... [truncated]"

DEFAULT_COLUMNS = [
    "imported_at_utc",
    "source_file",
    "scope",
    "label",
    "target_company",
    "query",
    "role_type",
    "rank",
    "name",
    "headline",
    "current_company_guess",
    "linkedin_url",
    "score",
    "matched_title_terms",
    "matched_keywords",
    "matched_focus_terms",
    "subject",
    "message_body",
    "connection_note",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload networking messages JSON to a Google Sheet tab."
    )
    parser.add_argument("--json", required=True, help="Path to networking_messages JSON file")
    parser.add_argument("--sheet", required=True, help="Google Sheet URL or spreadsheet ID")
    parser.add_argument("--tab", default="networking_messages", help="Target sheet tab name")
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
        text = json.dumps(value, ensure_ascii=False)
    else:
        text = str(value)
    if len(text) > MAX_CELL_CHARS:
        return text[: MAX_CELL_CHARS - len(TRUNCATION_SUFFIX)] + TRUNCATION_SUFFIX
    return text


def load_payload(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError("Expected top-level JSON object")
    return payload


def build_records(payload: Dict[str, Any], source_file: str) -> List[Dict[str, str]]:
    messages = payload.get("messages", [])
    if not isinstance(messages, list):
        raise ValueError("Expected 'messages' to be a list")

    imported = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows: List[Dict[str, str]] = []
    for item in messages:
        if not isinstance(item, dict):
            continue
        row = {"imported_at_utc": imported, "source_file": source_file}
        for key in DEFAULT_COLUMNS:
            if key in {"imported_at_utc", "source_file"}:
                continue
            row[key] = as_text(item.get(key))
        rows.append(row)
    return rows


def get_credentials(credentials_file: Path, token_file: Path, non_interactive: bool):
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
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_file), SCOPES)
            creds = flow.run_local_server(port=0, open_browser=False)

        with open(token_file, "wb") as f:
            pickle.dump(creds, f)

    return creds


def ensure_tab_exists(service, spreadsheet_id: str, tab_name: str) -> None:
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing_tabs = {sheet["properties"]["title"] for sheet in spreadsheet.get("sheets", [])}
    if tab_name in existing_tabs:
        return
    body = {"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def chunked(rows: List[List[str]], size: int) -> Iterable[List[List[str]]]:
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


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
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A:ZZ",
        body={},
    ).execute()


def materialize(records: List[Dict[str, str]], columns: List[str]) -> List[List[str]]:
    return [[record.get(col, "") for col in columns] for record in records]


def main() -> int:
    args = parse_args()
    json_path = Path(args.json)
    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    payload = load_payload(json_path)
    records = build_records(payload, source_file=json_path.name)
    if not records:
        print("No networking messages in JSON. Nothing to upload.")
        return 0

    sheet_id = extract_sheet_id(args.sheet)
    creds = get_credentials(
        credentials_file=Path(args.credentials_file),
        token_file=Path(args.token_file),
        non_interactive=args.non_interactive,
    )
    service = build("sheets", "v4", credentials=creds)
    ensure_tab_exists(service, sheet_id, args.tab)

    rows = materialize(records, DEFAULT_COLUMNS)
    if args.mode == "replace":
        clear_tab(service, sheet_id, args.tab)
        uploaded = write_rows(service, sheet_id, args.tab, [DEFAULT_COLUMNS] + rows) - 1
    else:
        uploaded = append_rows(service, sheet_id, args.tab, rows)

    print(f"Uploaded {uploaded} networking message rows to '{args.tab}' in spreadsheet {sheet_id}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
