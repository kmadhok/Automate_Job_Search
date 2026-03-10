#!/usr/bin/env python3
"""Run end-to-end job + outreach + resume-tailoring + networking automation."""

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
        description="Run complete career automation pipeline and upload outputs to Google Sheets."
    )
    parser.add_argument(
        "--trigger",
        required=True,
        help="Statement to trigger full pipeline. Must include 'complete package'.",
    )
    parser.add_argument("--sheet", required=True, help="Google Sheet URL or spreadsheet ID")
    parser.add_argument(
        "--jobs-json",
        default="",
        help="Optional existing jobs JSON. If omitted, script runs main.py first.",
    )
    parser.add_argument(
        "--skip-job-discovery",
        action="store_true",
        help="Skip running main.py. Requires --jobs-json or an existing output/jobs_*.json file.",
    )

    parser.add_argument("--jobs-tab", default="jobs", help="Sheet tab for scraped jobs")
    parser.add_argument("--job-contacts-tab", default="job_contacts", help="Tab for job outreach contacts")
    parser.add_argument("--job-messages-tab", default="job_messages", help="Tab for job outreach messages")
    parser.add_argument("--resume-tab", default="resume_tailoring", help="Tab for resume tailoring rows")
    parser.add_argument(
        "--networking-contacts-tab",
        default="networking_contacts",
        help="Tab for networking contact rows",
    )
    parser.add_argument(
        "--networking-messages-tab",
        default="networking_messages",
        help="Tab for networking message rows",
    )

    parser.add_argument("--jobs-mode", default="append", choices=["append", "replace"])
    parser.add_argument("--contacts-mode", default="replace", choices=["append", "replace"])
    parser.add_argument("--messages-mode", default="replace", choices=["append", "replace"])
    parser.add_argument("--resume-mode", default="replace", choices=["append", "replace"])
    parser.add_argument("--networking-mode", default="replace", choices=["append", "replace"])
    parser.add_argument("--networking-messages-mode", default="replace", choices=["append", "replace"])

    parser.add_argument("--per-role", type=int, default=3, help="Contacts/messages per role for job outreach")
    parser.add_argument("--skip-job-messages", action="store_true", help="Skip generating/uploading job messages")

    parser.add_argument("--resume-pdf", default=os.getenv("OUTREACH_RESUME_PDF", ""))
    parser.add_argument("--profile-summary", default=os.getenv("OUTREACH_PROFILE_SUMMARY", ""))
    parser.add_argument("--resume-max-jobs", type=int, default=20)
    parser.add_argument("--skip-resume-tailoring", action="store_true")

    parser.add_argument(
        "--network-internal-companies",
        default=os.getenv("NETWORK_INTERNAL_COMPANIES", "Walmart,Walmart Global Tech"),
    )
    parser.add_argument(
        "--network-external-companies",
        default=os.getenv("NETWORK_EXTERNAL_COMPANIES", "Microsoft,Amazon,Meta,OpenAI,NVIDIA"),
    )
    parser.add_argument(
        "--network-title-terms",
        default=os.getenv(
            "NETWORK_TITLE_TERMS",
            "Data Scientist,AI Engineer,Machine Learning Engineer,Data Analyst,Applied Scientist",
        ),
    )
    parser.add_argument(
        "--network-keywords",
        default=os.getenv(
            "NETWORK_KEYWORDS",
            "agents,large language models,llm,applied ai,production ai,real-world ai",
        ),
    )
    parser.add_argument(
        "--network-focus-terms",
        default=os.getenv(
            "NETWORK_FOCUS_TERMS",
            "customer impact,supply chain,optimization,recommendation systems,automation",
        ),
    )
    parser.add_argument("--network-per-query", type=int, default=8)
    parser.add_argument("--network-max-results", type=int, default=12)
    parser.add_argument(
        "--network-goal",
        default=(
            "learn how teams are applying AI to real business problems and build long-term professional "
            "relationships"
        ),
    )
    parser.add_argument("--skip-networking", action="store_true")
    parser.add_argument("--skip-networking-messages", action="store_true")

    parser.add_argument(
        "--non-interactive-sheets",
        action="store_true",
        help="Pass --non-interactive to all sheet upload scripts.",
    )
    return parser.parse_args()


def is_trigger_valid(trigger: str) -> bool:
    return "complete package" in trigger.strip().lower()


def run_step(cmd: List[str], env: dict[str, str], redact: bool = False) -> None:
    printable = cmd[:]
    if redact:
        for i, token in enumerate(printable[:-1]):
            if token in {"--serper-api-key", "--api-key"}:
                printable[i + 1] = "***REDACTED***"
    print("+ " + " ".join(shlex.quote(part) for part in printable))
    subprocess.run(cmd, check=True, env=env)


def latest_jobs_json(output_dir: Path) -> Path:
    files = list(output_dir.glob("jobs_*.json"))
    if not files:
        raise FileNotFoundError("No jobs_*.json files found in output/.")
    return max(files, key=lambda path: path.stat().st_mtime)


def maybe_add_non_interactive(args: argparse.Namespace, cmd: List[str]) -> List[str]:
    if args.non_interactive_sheets:
        return cmd + ["--non-interactive"]
    return cmd


def main() -> int:
    args = parse_args()
    if not is_trigger_valid(args.trigger):
        raise SystemExit("Trigger rejected. Include the phrase 'complete package' in --trigger.")

    base_dir = Path(__file__).resolve().parent
    output_dir = (base_dir / "output").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    env = os.environ.copy()

    # Step 1: get jobs JSON
    if args.jobs_json:
        jobs_json = Path(args.jobs_json)
        if not jobs_json.is_absolute():
            jobs_json = (base_dir / jobs_json).resolve()
        if not jobs_json.exists():
            raise FileNotFoundError(f"Jobs JSON not found: {jobs_json}")
    else:
        if not args.skip_job_discovery:
            run_step([sys.executable, str((base_dir / "main.py").resolve())], env=env)
        jobs_json = latest_jobs_json(output_dir)

    # Step 2: upload jobs
    upload_jobs_cmd = [
        sys.executable,
        str((base_dir / "upload_jobs_to_sheets.py").resolve()),
        "--json",
        str(jobs_json),
        "--sheet",
        args.sheet,
        "--tab",
        args.jobs_tab,
        "--mode",
        args.jobs_mode,
    ]
    run_step(maybe_add_non_interactive(args, upload_jobs_cmd), env=env)

    # Shared Serper key for contacts/networking discovery.
    serper_key = (
        os.getenv("SERPER_DEV_API_KEY", "").strip()
        or os.getenv("SERPER_API_KEY", "").strip()
    )

    # Step 3: job outreach contact discovery + upload
    if not serper_key:
        raise SystemExit(
            "Missing SERPER_DEV_API_KEY (or SERPER_API_KEY). Set it before running complete pipeline."
        )

    outreach_json = (output_dir / f"outreach_targets_{run_stamp}.json").resolve()
    outreach_csv = (output_dir / f"outreach_targets_{run_stamp}.csv").resolve()
    find_contacts_cmd = [
        sys.executable,
        str((base_dir / "find_outreach_contacts.py").resolve()),
        "--json",
        str(jobs_json),
        "--fetch",
        "--per-role",
        str(max(1, args.per_role)),
        "--serper-api-key",
        serper_key,
        "--output-json",
        str(outreach_json),
        "--output-csv",
        str(outreach_csv),
    ]
    run_step(find_contacts_cmd, env=env, redact=True)

    upload_contacts_cmd = [
        sys.executable,
        str((base_dir / "upload_contacts_to_sheets.py").resolve()),
        "--json",
        str(outreach_json),
        "--sheet",
        args.sheet,
        "--tab",
        args.job_contacts_tab,
        "--mode",
        args.contacts_mode,
    ]
    run_step(maybe_add_non_interactive(args, upload_contacts_cmd), env=env)

    # Step 4: job outreach message generation + upload
    messages_json = None
    if not args.skip_job_messages:
        messages_json = (output_dir / f"outreach_messages_{run_stamp}.json").resolve()
        messages_csv = (output_dir / f"outreach_messages_{run_stamp}.csv").resolve()
        gen_messages_cmd = [
            sys.executable,
            str((base_dir / "generate_outreach_messages.py").resolve()),
            "--json",
            str(outreach_json),
            "--max-per-role",
            str(max(1, args.per_role)),
            "--output-json",
            str(messages_json),
            "--output-csv",
            str(messages_csv),
        ]
        if args.profile_summary:
            gen_messages_cmd.extend(["--profile-summary", args.profile_summary])
        if args.resume_pdf:
            gen_messages_cmd.extend(["--resume-pdf", args.resume_pdf])
        run_step(gen_messages_cmd, env=env)

        upload_messages_cmd = [
            sys.executable,
            str((base_dir / "upload_messages_to_sheets.py").resolve()),
            "--json",
            str(messages_json),
            "--sheet",
            args.sheet,
            "--tab",
            args.job_messages_tab,
            "--mode",
            args.messages_mode,
        ]
        run_step(maybe_add_non_interactive(args, upload_messages_cmd), env=env)

    # Step 5: resume tailoring + upload
    resume_json = None
    if not args.skip_resume_tailoring:
        resume_json = (output_dir / f"resume_tailoring_{run_stamp}.json").resolve()
        resume_csv = (output_dir / f"resume_tailoring_{run_stamp}.csv").resolve()

        resume_cmd = [
            sys.executable,
            str((base_dir / "generate_resume_tailoring.py").resolve()),
            "--jobs-json",
            str(jobs_json),
            "--max-jobs",
            str(max(0, args.resume_max_jobs)),
            "--output-json",
            str(resume_json),
            "--output-csv",
            str(resume_csv),
        ]
        if args.resume_pdf:
            resume_cmd.extend(["--resume-pdf", args.resume_pdf])
        if args.profile_summary:
            resume_cmd.extend(["--profile-summary", args.profile_summary])

        run_step(resume_cmd, env=env)

        upload_resume_cmd = [
            sys.executable,
            str((base_dir / "upload_resume_tailoring_to_sheets.py").resolve()),
            "--json",
            str(resume_json),
            "--sheet",
            args.sheet,
            "--tab",
            args.resume_tab,
            "--mode",
            args.resume_mode,
        ]
        run_step(maybe_add_non_interactive(args, upload_resume_cmd), env=env)

    # Step 6: networking contacts + messages + uploads
    networking_json = None
    networking_messages_json = None
    if not args.skip_networking:
        networking_json = (output_dir / f"networking_targets_{run_stamp}.json").resolve()
        networking_csv = (output_dir / f"networking_targets_{run_stamp}.csv").resolve()

        find_networking_cmd = [
            sys.executable,
            str((base_dir / "find_networking_contacts.py").resolve()),
            "--fetch",
            "--serper-api-key",
            serper_key,
            "--internal-companies",
            args.network_internal_companies,
            "--external-companies",
            args.network_external_companies,
            "--title-terms",
            args.network_title_terms,
            "--keywords",
            args.network_keywords,
            "--focus-terms",
            args.network_focus_terms,
            "--per-query",
            str(max(1, args.network_per_query)),
            "--max-results",
            str(max(1, args.network_max_results)),
            "--output-json",
            str(networking_json),
            "--output-csv",
            str(networking_csv),
        ]
        run_step(find_networking_cmd, env=env, redact=True)

        upload_networking_contacts_cmd = [
            sys.executable,
            str((base_dir / "upload_networking_contacts_to_sheets.py").resolve()),
            "--json",
            str(networking_json),
            "--sheet",
            args.sheet,
            "--tab",
            args.networking_contacts_tab,
            "--mode",
            args.networking_mode,
        ]
        run_step(maybe_add_non_interactive(args, upload_networking_contacts_cmd), env=env)

        if not args.skip_networking_messages:
            networking_messages_json = (
                output_dir / f"networking_messages_{run_stamp}.json"
            ).resolve()
            networking_messages_csv = (
                output_dir / f"networking_messages_{run_stamp}.csv"
            ).resolve()

            gen_networking_msg_cmd = [
                sys.executable,
                str((base_dir / "generate_networking_messages.py").resolve()),
                "--json",
                str(networking_json),
                "--goal",
                args.network_goal,
                "--max-per-group",
                str(max(1, args.network_per_query)),
                "--output-json",
                str(networking_messages_json),
                "--output-csv",
                str(networking_messages_csv),
            ]
            if args.profile_summary:
                gen_networking_msg_cmd.extend(["--profile-summary", args.profile_summary])
            run_step(gen_networking_msg_cmd, env=env)

            upload_networking_messages_cmd = [
                sys.executable,
                str((base_dir / "upload_networking_messages_to_sheets.py").resolve()),
                "--json",
                str(networking_messages_json),
                "--sheet",
                args.sheet,
                "--tab",
                args.networking_messages_tab,
                "--mode",
                args.networking_messages_mode,
            ]
            run_step(maybe_add_non_interactive(args, upload_networking_messages_cmd), env=env)

    print("Pipeline complete.")
    print(f"Jobs source: {jobs_json}")
    print(f"Outreach contacts: {outreach_json}")
    if messages_json:
        print(f"Outreach messages: {messages_json}")
    if resume_json:
        print(f"Resume tailoring: {resume_json}")
    if networking_json:
        print(f"Networking contacts: {networking_json}")
    if networking_messages_json:
        print(f"Networking messages: {networking_messages_json}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
