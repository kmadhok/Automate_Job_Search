#!/usr/bin/env python3
"""Run outreach discovery and upload contacts to Google Sheets in one command."""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Trigger statement-driven pipeline for job contact discovery."
    )
    parser.add_argument(
        "--trigger",
        required=True,
        help="Statement to trigger pipeline. Must include 'job contacts'.",
    )
    parser.add_argument(
        "--jobs-json",
        required=True,
        help="Path to jobs JSON input file.",
    )
    parser.add_argument(
        "--sheet",
        required=True,
        help="Google Sheet URL or spreadsheet ID.",
    )
    parser.add_argument(
        "--tab",
        default="job_contacts",
        help="Destination sheet tab name.",
    )
    parser.add_argument(
        "--per-role",
        type=int,
        default=3,
        help="Candidates per role type.",
    )
    parser.add_argument(
        "--mode",
        default="replace",
        choices=["replace", "append"],
        help="Upload mode for contact rows.",
    )
    parser.add_argument(
        "--messages-tab",
        default="job_messages",
        help="Destination tab for generated outreach messages.",
    )
    parser.add_argument(
        "--messages-mode",
        default="replace",
        choices=["replace", "append"],
        help="Upload mode for message rows.",
    )
    parser.add_argument(
        "--profile-summary",
        default=os.getenv("OUTREACH_PROFILE_SUMMARY", ""),
        help="Optional blurb injected into each generated message.",
    )
    parser.add_argument(
        "--resume-pdf",
        default=os.getenv("OUTREACH_RESUME_PDF", ""),
        help="Optional resume PDF path used to auto-generate profile summary.",
    )
    parser.add_argument(
        "--skip-messages",
        action="store_true",
        help="Skip message generation and message sheet upload.",
    )
    return parser.parse_args()


def is_trigger_valid(trigger: str) -> bool:
    normalized = trigger.strip().lower()
    return "job contacts" in normalized


def run_step(cmd: list[str], env: dict[str, str]) -> None:
    print("+ " + " ".join(shlex.quote(part) for part in cmd))
    subprocess.run(cmd, check=True, env=env)


def redact_api_key_args(cmd: list[str]) -> list[str]:
    redacted = cmd[:]
    for i, part in enumerate(redacted[:-1]):
        if part == "--serper-api-key":
            redacted[i + 1] = "***REDACTED***"
    return redacted


def main() -> int:
    args = parse_args()
    if not is_trigger_valid(args.trigger):
        raise SystemExit(
            "Trigger rejected. Include the phrase 'job contacts' in --trigger."
        )

    base_dir = Path(__file__).resolve().parent
    jobs_json = Path(args.jobs_json)
    if not jobs_json.is_absolute():
        jobs_json = (base_dir / jobs_json).resolve()
    if not jobs_json.exists():
        raise FileNotFoundError(f"Jobs JSON not found: {jobs_json}")

    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    outreach_json = (base_dir / "output" / f"outreach_targets_{run_stamp}.json").resolve()
    outreach_csv = (base_dir / "output" / f"outreach_targets_{run_stamp}.csv").resolve()

    serper_key = (
        os.getenv("SERPER_DEV_API_KEY", "").strip()
        or os.getenv("SERPER_API_KEY", "").strip()
    )
    if not serper_key:
        raise SystemExit(
            "Missing SERPER_DEV_API_KEY (or SERPER_API_KEY). Set it before running."
        )

    env = os.environ.copy()

    discover_cmd = [
        sys.executable,
        str((base_dir / "find_outreach_contacts.py").resolve()),
        "--json",
        str(jobs_json),
        "--fetch",
        "--per-role",
        str(args.per_role),
        "--serper-api-key",
        serper_key,
        "--output-json",
        str(outreach_json),
        "--output-csv",
        str(outreach_csv),
    ]
    print("+ " + " ".join(shlex.quote(part) for part in redact_api_key_args(discover_cmd)))
    subprocess.run(discover_cmd, check=True, env=env)

    run_step(
        [
            sys.executable,
            str((base_dir / "upload_contacts_to_sheets.py").resolve()),
            "--json",
            str(outreach_json),
            "--sheet",
            args.sheet,
            "--tab",
            args.tab,
            "--mode",
            args.mode,
        ],
        env=env,
    )

    if not args.skip_messages:
        messages_json = (base_dir / "output" / f"outreach_messages_{run_stamp}.json").resolve()
        messages_csv = (base_dir / "output" / f"outreach_messages_{run_stamp}.csv").resolve()

        message_cmd = [
            sys.executable,
            str((base_dir / "generate_outreach_messages.py").resolve()),
            "--json",
            str(outreach_json),
            "--max-per-role",
            str(args.per_role),
            "--output-json",
            str(messages_json),
            "--output-csv",
            str(messages_csv),
        ]
        if args.profile_summary:
            message_cmd.extend(["--profile-summary", args.profile_summary])
        if args.resume_pdf:
            message_cmd.extend(["--resume-pdf", args.resume_pdf])
        run_step(message_cmd, env=env)

        run_step(
            [
                sys.executable,
                str((base_dir / "upload_messages_to_sheets.py").resolve()),
                "--json",
                str(messages_json),
                "--sheet",
                args.sheet,
                "--tab",
                args.messages_tab,
                "--mode",
                args.messages_mode,
            ],
            env=env,
        )

    print(f"Pipeline complete. Uploaded contacts from: {outreach_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
