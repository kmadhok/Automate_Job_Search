#!/usr/bin/env python3
"""Generate networking outreach messages from networking contact candidates."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate networking messages from networking_targets JSON output."
    )
    parser.add_argument("--json", required=True, help="Path to networking_targets JSON file")
    parser.add_argument(
        "--profile-summary",
        default=os.getenv("NETWORK_PROFILE_SUMMARY", "") or os.getenv("OUTREACH_PROFILE_SUMMARY", ""),
        help="Short profile blurb to include in each message",
    )
    parser.add_argument(
        "--goal",
        default=(
            "learn how strong teams are applying AI to real business problems and build meaningful "
            "peer connections"
        ),
        help="Your networking goal statement used in message personalization.",
    )
    parser.add_argument(
        "--max-per-group",
        type=int,
        default=5,
        help="Max messages to generate for each networking search group.",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Output JSON path (default: output/networking_messages_<timestamp>.json)",
    )
    parser.add_argument(
        "--output-csv",
        default="",
        help="Output CSV path (default: output/networking_messages_<timestamp>.csv)",
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


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def first_name(full_name: str) -> str:
    cleaned = normalize_whitespace(full_name)
    if not cleaned:
        return "there"
    return cleaned.split()[0]


def headline_hook(headline: str) -> str:
    text = normalize_whitespace(headline)
    if not text:
        return ""
    for sep in ("·", "|", ";", ",", "."):
        if sep in text:
            text = text.split(sep, 1)[0].strip()
            break
    words = text.split()
    if len(words) > 16:
        text = " ".join(words[:16]).strip() + "..."
    return text


def build_subject(scope: str, target_company: str) -> str:
    if scope == "internal":
        return f"Quick intro from a fellow {target_company} teammate"
    if scope == "external_company" and target_company:
        return f"Learning from applied AI work at {target_company}"
    return "Quick connection request on applied AI work"


def build_message(
    *,
    scope: str,
    target_company: str,
    name: str,
    headline: str,
    profile_summary: str,
    goal: str,
) -> Dict[str, str]:
    person_first = first_name(name)
    hook = headline_hook(headline)

    intro = f"Hi {person_first},"
    lines = [intro]

    if scope == "internal":
        lines.append(
            "I work at Walmart as well and wanted to introduce myself while growing my internal network."
        )
    else:
        lines.append(
            "I came across your profile while looking to learn from people applying AI in real-world settings."
        )

    if profile_summary:
        lines.append(normalize_whitespace(profile_summary))

    if hook:
        lines.append(f"Your background in {hook} stood out to me.")

    lines.append(f"My current goal is to {normalize_whitespace(goal)}.")
    lines.append("If you're open to it, I'd really value a short 10-15 minute chat to learn from your experience.")
    lines.append("Thanks for considering, and happy to connect either way.")

    message_body = "\n".join(lines)

    note = (
        f"Hi {person_first} - your work stood out to me. I'm focused on applied AI and would value a brief "
        "chat to learn from your experience."
    )
    note = normalize_whitespace(note)
    if len(note) > 280:
        note = note[:277].rstrip() + "..."

    return {
        "subject": build_subject(scope, target_company),
        "message_body": message_body,
        "connection_note": note,
    }


def load_payload(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Expected top-level JSON object")
    return data


def generate_messages(
    payload: Dict[str, Any],
    profile_summary: str,
    goal: str,
    max_per_group: int,
) -> List[Dict[str, Any]]:
    groups = payload.get("search_groups", [])
    if not isinstance(groups, list):
        raise ValueError("Expected 'search_groups' list in networking payload")

    rows: List[Dict[str, Any]] = []
    for group in groups:
        if not isinstance(group, dict):
            continue

        scope = as_text(group.get("scope"))
        label = as_text(group.get("label"))
        target_company = as_text(group.get("target_company"))
        query = as_text(group.get("query"))
        candidates = group.get("candidates", [])
        if not isinstance(candidates, list):
            continue

        for rank, candidate in enumerate(candidates[:max(1, max_per_group)], start=1):
            if not isinstance(candidate, dict):
                continue

            name = as_text(candidate.get("name"))
            headline = as_text(candidate.get("headline"))
            linkedin_url = as_text(candidate.get("linkedin_url"))
            current_company_guess = as_text(candidate.get("current_company_guess"))
            score = candidate.get("score")
            matched_title_terms = candidate.get("matched_title_terms", [])
            matched_keywords = candidate.get("matched_keywords", [])
            matched_focus_terms = candidate.get("matched_focus_terms", [])

            message_parts = build_message(
                scope=scope,
                target_company=target_company,
                name=name,
                headline=headline,
                profile_summary=profile_summary,
                goal=goal,
            )

            rows.append(
                {
                    "scope": scope,
                    "label": label,
                    "target_company": target_company,
                    "query": query,
                    "role_type": "networking",
                    "rank": rank,
                    "name": name,
                    "headline": headline,
                    "current_company_guess": current_company_guess,
                    "linkedin_url": linkedin_url,
                    "score": score,
                    "matched_title_terms": as_text(matched_title_terms),
                    "matched_keywords": as_text(matched_keywords),
                    "matched_focus_terms": as_text(matched_focus_terms),
                    "subject": message_parts["subject"],
                    "message_body": message_parts["message_body"],
                    "connection_note": message_parts["connection_note"],
                }
            )
    return rows


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
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
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    input_path = Path(args.json)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    payload = load_payload(input_path)
    rows = generate_messages(
        payload=payload,
        profile_summary=normalize_whitespace(args.profile_summary),
        goal=normalize_whitespace(args.goal),
        max_per_group=max(1, args.max_per_group),
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_json = input_path.parent / f"networking_messages_{timestamp}.json"
    default_csv = input_path.parent / f"networking_messages_{timestamp}.csv"
    out_json = Path(args.output_json) if args.output_json else default_json
    out_csv = Path(args.output_csv) if args.output_csv else default_csv

    report = {
        "metadata": {
            "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "source_networking_file": str(input_path),
            "messages_generated": len(rows),
            "max_per_group": max(1, args.max_per_group),
            "profile_summary_used": bool(normalize_whitespace(args.profile_summary)),
            "goal": normalize_whitespace(args.goal),
        },
        "messages": rows,
    }

    write_json(out_json, report)
    write_csv(out_csv, rows)

    print(f"Saved networking messages JSON: {out_json}")
    print(f"Saved networking messages CSV: {out_csv}")
    print(f"Generated {len(rows)} networking messages.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
