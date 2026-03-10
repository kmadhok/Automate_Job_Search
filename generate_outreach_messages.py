#!/usr/bin/env python3
"""Generate role-specific outreach messages from contact candidates."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
from pypdf import PdfReader

ROLE_TYPES = ("manager", "recruiter", "team_member")

OPENERS = {
    "manager": [
        "I just applied for the role and wanted to share a quick note.",
        "I recently submitted my application and wanted to introduce myself briefly.",
        "I applied for this opening and wanted to reach out directly with context.",
    ],
    "recruiter": [
        "I applied for the role and wanted to share a concise fit summary.",
        "I recently submitted an application and wanted to introduce myself.",
        "I applied for this opening and wanted to provide a short context note.",
    ],
    "team_member": [
        "I applied for the role and hoped to ask for quick guidance on team fit.",
        "I recently applied and wanted to introduce myself to someone on the team.",
        "I applied for this role and wanted to ask one quick question about the team.",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate manager/recruiter/team-member outreach messages."
    )
    parser.add_argument("--json", required=True, help="Path to outreach_targets JSON file")
    parser.add_argument(
        "--profile-summary",
        default=os.getenv("OUTREACH_PROFILE_SUMMARY", ""),
        help="Short profile blurb to include in every message",
    )
    parser.add_argument(
        "--resume-pdf",
        default=os.getenv("OUTREACH_RESUME_PDF", ""),
        help="Optional path to resume PDF; used to auto-generate profile summary.",
    )
    parser.add_argument(
        "--max-per-role",
        type=int,
        default=3,
        help="Max messages to generate per role type for each job",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Output JSON path (default: output/outreach_messages_<timestamp>.json)",
    )
    parser.add_argument(
        "--output-csv",
        default="",
        help="Output CSV path (default: output/outreach_messages_<timestamp>.csv)",
    )
    return parser.parse_args()


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "; ".join(as_text(item) for item in value)
    return str(value)


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def extract_resume_text(pdf_path: Path) -> str:
    reader = PdfReader(str(pdf_path))
    chunks: List[str] = []
    for page in reader.pages:
        try:
            chunks.append(page.extract_text() or "")
        except Exception:
            continue
    return normalize_whitespace("\n".join(chunks))


def derive_profile_summary_from_resume(resume_text: str) -> str:
    text = normalize_whitespace(resume_text)
    if not text:
        return ""

    # Remove contact lines and links noise.
    text = re.sub(r"\b\S+@\S+\b", " ", text)
    text = re.sub(r"https?://\S+", " ", text)
    text = normalize_whitespace(text)

    lowered = text.lower()
    years_match = re.search(r"(\d+)\+?\s+years?", lowered)
    years_phrase = f"{years_match.group(1)}+ years" if years_match else "several years"

    role_patterns = [
        r"\b(data scientist|data engineer|machine learning engineer|ai engineer|applied scientist|analytics engineer)\b",
        r"\b(software engineer|research scientist|ml engineer)\b",
    ]
    role = ""
    for pattern in role_patterns:
        match = re.search(pattern, lowered)
        if match:
            role = match.group(1).title()
            break
    if not role:
        role = "Data/AI professional"

    skill_pool = [
        "Python",
        "SQL",
        "Machine Learning",
        "Experimentation",
        "A/B Testing",
        "Forecasting",
        "NLP",
        "LLMs",
        "Data Pipelines",
        "Spark",
        "Airflow",
        "Tableau",
    ]
    found = [skill for skill in skill_pool if skill.lower() in lowered]
    top_skills = found[:3] if found else ["Python", "SQL", "Machine Learning"]

    impact_phrase = "shipping production systems with measurable business impact"
    if "fraud" in lowered or "risk" in lowered:
        impact_phrase = "building risk and fraud-focused ML systems with measurable outcomes"
    elif "forecast" in lowered:
        impact_phrase = "developing forecasting and analytics systems tied to business decisions"
    elif "llm" in lowered or "generative ai" in lowered:
        impact_phrase = "building LLM-enabled products and ML systems with measurable impact"

    return (
        f"I'm a {role} with {years_phrase} of experience, with strengths in "
        f"{', '.join(top_skills)}. My recent work focuses on {impact_phrase}."
    )


def first_name(full_name: str) -> str:
    cleaned = normalize_whitespace(full_name)
    if not cleaned:
        return "there"
    first = cleaned.split()[0]
    # Keep fallback simple for initials and unusual names.
    if len(first) <= 1:
        return cleaned
    return first


def headline_hook(headline: str) -> str:
    text = normalize_whitespace(headline)
    if not text:
        return ""
    # Use first clause for concise personalization.
    for sep in ("·", "|", ".", ";", ","):
        if sep in text:
            text = text.split(sep, 1)[0].strip()
            break
    words = text.split()
    if len(words) > 14:
        text = " ".join(words[:14]).strip() + "..."
    return text


def select_variant(role_type: str, job_id: str, name: str) -> int:
    token = f"{role_type}|{job_id}|{name}".encode("utf-8")
    digest = hashlib.md5(token).hexdigest()
    return int(digest[:8], 16)


def build_subject(role_type: str, job_title: str, company: str, team_guess: str) -> str:
    if role_type == "recruiter":
        return f"Application: {job_title} at {company}"
    if role_type == "manager":
        if team_guess:
            return f"Applied to {job_title} ({team_guess})"
        return f"Applied to {job_title} at {company}"
    if team_guess:
        return f"Quick question about {team_guess} at {company}"
    return f"Quick question about {job_title} at {company}"


def build_message(
    role_type: str,
    person_name: str,
    person_headline: str,
    company: str,
    job_title: str,
    team_guess: str,
    profile_summary: str,
    job_id: str,
) -> Tuple[str, str]:
    opener_idx = select_variant(role_type, job_id, person_name) % len(OPENERS[role_type])
    opener = OPENERS[role_type][opener_idx]
    hook = headline_hook(person_headline)
    name = first_name(person_name)
    team_or_role = team_guess if team_guess else job_title

    lines = [f"Hi {name},", opener]

    if profile_summary:
        lines.append(normalize_whitespace(profile_summary))

    if hook:
        lines.append(f"I saw your background in {hook} and thought you might be the right person to ask.")

    lines.append(f"I applied for {job_title} at {company}, and I'm especially interested in {team_or_role}.")

    if role_type == "recruiter":
        lines.append(
            "If helpful, could you share whether my background aligns and what the best next step would be in the process?"
        )
    elif role_type == "manager":
        lines.append(
            "If useful, I can send a short summary of relevant projects. Would you be open to a brief conversation?"
        )
    else:
        lines.append(
            "Would you be open to a quick 10-minute chat so I can better understand what the team values most?"
        )

    lines.append("Thanks for your time.")
    body = "\n".join(lines)

    # Connection note for initial LinkedIn request.
    note = (
        f"Hi {name} - I applied for {job_title} at {company}. "
        f"Would love to connect and ask one quick question about {team_or_role}."
    )
    note = normalize_whitespace(note)
    if len(note) > 280:
        note = note[:277].rstrip() + "..."

    return body, note


def load_payload(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Expected top-level JSON object")
    return data


def generate_messages(payload: Dict[str, Any], profile_summary: str, max_per_role: int) -> List[Dict[str, Any]]:
    jobs = payload.get("jobs", [])
    if not isinstance(jobs, list):
        raise ValueError("Expected 'jobs' list in outreach payload")

    rows: List[Dict[str, Any]] = []
    for job in jobs:
        if not isinstance(job, dict):
            continue

        job_id = as_text(job.get("job_id"))
        job_title = as_text(job.get("title"))
        company = as_text(job.get("company"))
        location = as_text(job.get("location"))
        team_guess = as_text(job.get("team_guess"))
        team_confidence = job.get("team_confidence")
        team_source = as_text(job.get("team_source"))
        queries = job.get("queries", {}) if isinstance(job.get("queries"), dict) else {}
        candidates = job.get("candidates", {}) if isinstance(job.get("candidates"), dict) else {}

        for role_type in ROLE_TYPES:
            role_people = candidates.get(role_type, [])
            if not isinstance(role_people, list):
                continue

            for rank, person in enumerate(role_people[:max_per_role], start=1):
                if not isinstance(person, dict):
                    continue
                name = as_text(person.get("name"))
                headline = as_text(person.get("headline"))
                linkedin_url = as_text(person.get("linkedin_url"))
                score = person.get("score")

                body, note = build_message(
                    role_type=role_type,
                    person_name=name,
                    person_headline=headline,
                    company=company,
                    job_title=job_title,
                    team_guess=team_guess,
                    profile_summary=profile_summary,
                    job_id=job_id,
                )

                rows.append(
                    {
                        "job_id": job_id,
                        "job_title": job_title,
                        "company": company,
                        "location": location,
                        "team_guess": team_guess,
                        "team_confidence": team_confidence,
                        "team_source": team_source,
                        "role_type": role_type,
                        "rank": rank,
                        "name": name,
                        "headline": headline,
                        "linkedin_url": linkedin_url,
                        "score": score,
                        "query": as_text(queries.get(role_type)),
                        "subject": build_subject(role_type, job_title, company, team_guess),
                        "message_body": body,
                        "connection_note": note,
                    }
                )
    return rows


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        fieldnames = [
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
            "subject",
            "message_body",
            "connection_note",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    input_path = Path(args.json)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    payload = load_payload(input_path)
    profile_summary = normalize_whitespace(args.profile_summary)
    resume_used = False
    if not profile_summary and args.resume_pdf:
        resume_path = Path(args.resume_pdf).expanduser().resolve()
        if not resume_path.exists():
            raise FileNotFoundError(f"Resume PDF not found: {resume_path}")
        resume_text = extract_resume_text(resume_path)
        profile_summary = derive_profile_summary_from_resume(resume_text)
        resume_used = bool(profile_summary)

    rows = generate_messages(
        payload=payload,
        profile_summary=profile_summary,
        max_per_role=max(1, args.max_per_role),
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_json = input_path.parent / f"outreach_messages_{timestamp}.json"
    default_csv = input_path.parent / f"outreach_messages_{timestamp}.csv"
    out_json = Path(args.output_json) if args.output_json else default_json
    out_csv = Path(args.output_csv) if args.output_csv else default_csv

    report = {
        "metadata": {
            "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "source_contacts_file": str(input_path),
            "messages_generated": len(rows),
            "max_per_role": max(1, args.max_per_role),
            "profile_summary_used": bool(profile_summary),
            "profile_summary_from_resume": resume_used,
            "resume_pdf": str(args.resume_pdf) if args.resume_pdf else "",
        },
        "messages": rows,
    }

    write_json(out_json, report)
    write_csv(out_csv, rows)

    print(f"Saved outreach messages JSON: {out_json}")
    print(f"Saved outreach messages CSV: {out_csv}")
    print(f"Generated {len(rows)} messages.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
