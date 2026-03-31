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
from typing import Any, Dict, List, Optional, Tuple
from pypdf import PdfReader

ROLE_TYPES = ("manager", "recruiter", "team_member")


# ── Context loading ──────────────────────────────────────────────────────────

def load_context_docs() -> dict:
    """Load career preferences and master experience for personalization."""
    context = {"preferences": "", "experience": ""}
    try:
        from config import CAREER_PREFERENCES_PATH, MASTER_EXPERIENCE_PATH
        if CAREER_PREFERENCES_PATH.exists():
            context["preferences"] = CAREER_PREFERENCES_PATH.read_text(encoding="utf-8")[:5000]
        if MASTER_EXPERIENCE_PATH.exists():
            context["experience"] = MASTER_EXPERIENCE_PATH.read_text(encoding="utf-8")[:8000]
    except Exception:
        pass
    return context


# ── Company hooks ────────────────────────────────────────────────────────────

COMPANY_HOOKS = {
    "anthropic": "The applied AI team's work on Claude's tool use is directly adjacent to what I've been building with MCP servers and agentic systems at Walmart.",
    "google": "Cloud AI's RAG infrastructure work aligns with the retrieval systems I've been designing over large-scale enterprise data.",
    "openai": "The applied AI engineering team's focus on turning research into real-world products maps directly to my experience building RAG and agentic systems that go from prototype to production.",
    "meta": "The GenAI team's work on applied LLM products aligns with the copilot and agent systems I've built at Walmart.",
    "microsoft": "The Copilot team's work on making AI useful for everyday tasks resonates with my text-to-SQL copilot work enabling non-technical users to self-serve analysis.",
    "scale ai": "Scale's data-centric AI approach resonates with my evaluation pipeline work and focus on measuring actual system quality.",
    "palantir": "The Forward Deployed approach \u2014 embedding with customers to build AI solutions \u2014 maps to how I've operated at Walmart: identifying problems, building the AI solution, and making it usable for non-technical stakeholders.",
    "databricks": "The AI Forward Deployed Engineering team's focus on customer-facing LLM solutions aligns with my experience building RAG and agent systems on top of large data platforms.",
    "salesforce": "The Agentforce platform's approach to AI agents aligns with the multi-agent system I've built at Walmart with specialized roles, handoffs, and quality control loops.",
    "cohere": "Cohere's focus on enterprise AI agents and RAG is a direct match for the agentic workflows and retrieval systems I've been building at Walmart.",
    "bloomberg": "The Generative AI and Search team's work on RAG systems and LLM-powered search maps to my text-to-SQL RAG copilot experience over large-scale enterprise data.",
}


# ── Banned phrases & validation ──────────────────────────────────────────────

BANNED_PHRASES = [
    "i hope this message finds you well",
    "i hope this finds you",
    "i'm reaching out",
    "i would love the opportunity",
    "i'm excited to",
    "i am writing to",
]


def validate_message(body: str) -> list[str]:
    """Check message quality. Returns list of issues."""
    issues = []
    words = body.split()
    if len(words) > 150:
        issues.append(f"Too long ({len(words)} words, max 150)")
    body_lower = body.lower()
    for phrase in BANNED_PHRASES:
        if phrase in body_lower:
            issues.append(f"Contains banned phrase: '{phrase}'")
    return issues


def get_company_hook(company: str) -> str:
    """Look up a company-specific hook, case-insensitive."""
    company_lower = company.lower().strip()
    for key, hook in COMPANY_HOOKS.items():
        if key in company_lower or company_lower in key:
            return hook
    return ""


# ── LLM message generation ──────────────────────────────────────────────────

def generate_llm_message(
    *,
    client,
    model: str,
    role_type: str,
    person_name: str,
    person_headline: str,
    company: str,
    job_title: str,
    job_description: str,
    fit_signals: dict,
    context_docs: dict,
    company_hook: str,
) -> dict:
    """Generate a personalized outreach message using GPT-4o.

    Returns dict with subject, body, proof_point_used, company_hook, green_flags_referenced.
    """
    if role_type == "manager":
        role_instruction = (
            "Write a cold outreach message to a hiring manager. "
            "Name ONE concrete proof point from Kanu's experience that directly maps to a requirement in the JD. "
            "Ask for a brief conversation about the team/role."
        )
    elif role_type == "recruiter":
        role_instruction = (
            "Write a cold outreach message to a recruiter. "
            "Emphasize breadth (RAG + agentic + evaluation). "
            "Ask about process/next steps."
        )
    else:
        role_instruction = (
            "Write a peer-level, casual message to a team member. "
            "Reference shared technical interests. "
            "Ask about team culture/tech stack."
        )

    green_flags = fit_signals.get("green_flags", [])

    prompt = f"""You are writing a cold outreach message from Kanu Madhok to a {role_type.replace('_', ' ')}.
Kanu just applied for {job_title} at {company}.

Key context about Kanu:
- Currently builds AI products at Walmart: a text-to-SQL RAG copilot (95% SQL accuracy over 12K queries),
  a multi-agent autonomous analyst system (explorer->analyst->auditor->fixer), and an MCP server for tool-calling.
- Built a RAG proposal generator at FTI Consulting that won Best in Show (UChicago capstone).
- MS in Applied Data Science from UChicago (4.0 GPA).

This role matched because: {', '.join(green_flags[:5]) if green_flags else 'strong alignment with applied AI work'}
The recipient is: {person_name}, {person_headline}
{f'Company hook: {company_hook}' if company_hook else ''}

{role_instruction}

Rules:
1. Open with a specific hook about why {company}'s AI work interests Kanu
2. Keep it under 150 words
3. Sound human, not templated
4. NO banned phrases: "I hope this message finds you well", "I'm reaching out", "I would love the opportunity", "I hope this finds you"
5. Include exactly ONE specific proof point with a metric

Output JSON: {{"subject": "...", "body": "...", "proof_point_used": "which experience was highlighted"}}"""

    response = client.chat.completions.create(
        model=model,
        temperature=0.7,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You write concise, human-sounding cold outreach messages. Return valid JSON only."},
            {"role": "user", "content": prompt},
        ],
    )

    content = (response.choices[0].message.content or "{}").strip()
    parsed = json.loads(content)

    return {
        "subject": parsed.get("subject", f"Applied to {job_title} at {company}"),
        "body": parsed.get("body", ""),
        "proof_point_used": parsed.get("proof_point_used", ""),
        "company_hook": company_hook,
        "green_flags_referenced": green_flags[:3],
    }

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
    parser.add_argument(
        "--use-llm",
        action="store_true",
        help="Use LLM (GPT-4o) for personalized message generation; falls back to deterministic.",
    )
    parser.add_argument(
        "--jobs-json",
        default="",
        help="Path to jobs JSON with fit data (for LLM context). Optional.",
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


def generate_messages(
    payload: Dict[str, Any],
    profile_summary: str,
    max_per_role: int,
    use_llm: bool = False,
    jobs_fit_data: Optional[Dict[str, dict]] = None,
    context_docs: Optional[dict] = None,
) -> List[Dict[str, Any]]:
    jobs = payload.get("jobs", [])
    if not isinstance(jobs, list):
        raise ValueError("Expected 'jobs' list in outreach payload")

    # Set up LLM client if requested
    openai_client = None
    openai_model = "gpt-4o"
    if use_llm:
        try:
            from openai import OpenAI
            from config import OPENAI_MODEL
            openai_client = OpenAI()
            openai_model = OPENAI_MODEL
        except Exception as e:
            print(f"Warning: LLM mode requested but OpenAI client failed to init: {e}")
            print("Falling back to deterministic messages.")

    if context_docs is None:
        context_docs = {}
    if jobs_fit_data is None:
        jobs_fit_data = {}

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

        # Look up fit data for this job (from evaluated jobs JSON)
        fit_data = jobs_fit_data.get(job_id, {})
        fit_signals = fit_data.get("fit_signals", {})
        job_description = fit_data.get("description", "")
        company_hook = get_company_hook(company)

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

                personalization_context: Dict[str, Any] = {
                    "proof_point_used": "",
                    "green_flags_referenced": fit_signals.get("green_flags", [])[:3],
                    "company_hook": company_hook,
                }

                # Try LLM generation first, fall back to deterministic
                llm_used = False
                if openai_client:
                    try:
                        llm_result = generate_llm_message(
                            client=openai_client,
                            model=openai_model,
                            role_type=role_type,
                            person_name=name,
                            person_headline=headline,
                            company=company,
                            job_title=job_title,
                            job_description=job_description,
                            fit_signals=fit_signals,
                            context_docs=context_docs,
                            company_hook=company_hook,
                        )
                        subject = llm_result["subject"]
                        body = llm_result["body"]
                        personalization_context["proof_point_used"] = llm_result.get("proof_point_used", "")
                        personalization_context["green_flags_referenced"] = llm_result.get("green_flags_referenced", [])
                        personalization_context["company_hook"] = llm_result.get("company_hook", "")

                        # Validate the LLM output
                        issues = validate_message(body)
                        if issues:
                            print(f"  LLM message for {name} ({role_type}) has issues: {issues}")
                            # Fall through to deterministic
                        else:
                            llm_used = True
                    except Exception as e:
                        print(f"  LLM generation failed for {name} ({role_type}): {e}")

                if not llm_used:
                    body, _ = build_message(
                        role_type=role_type,
                        person_name=name,
                        person_headline=headline,
                        company=company,
                        job_title=job_title,
                        team_guess=team_guess,
                        profile_summary=profile_summary,
                        job_id=job_id,
                    )
                    subject = build_subject(role_type, job_title, company, team_guess)

                # Connection note is always deterministic (280 char limit)
                fname = first_name(name)
                team_or_role = team_guess if team_guess else job_title
                note = (
                    f"Hi {fname} - I applied for {job_title} at {company}. "
                    f"Would love to connect and ask one quick question about {team_or_role}."
                )
                note = normalize_whitespace(note)
                if len(note) > 280:
                    note = note[:277].rstrip() + "..."

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
                        "subject": subject,
                        "message_body": body,
                        "connection_note": note,
                        "proof_point_used": personalization_context.get("proof_point_used", ""),
                        "company_hook": personalization_context.get("company_hook", ""),
                        "personalization_context": personalization_context,
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
            "proof_point_used",
            "company_hook",
        ]
        # Write rows without personalization_context (complex dict) for CSV
        csv_rows = []
        for row in rows:
            csv_row = {k: v for k, v in row.items() if k in fieldnames}
            csv_rows.append(csv_row)
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows)


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

    # Load fit data from jobs JSON if available (for LLM context)
    jobs_fit_data: Dict[str, dict] = {}
    if args.jobs_json:
        jobs_json_path = Path(args.jobs_json)
        if jobs_json_path.exists():
            try:
                with open(jobs_json_path, "r", encoding="utf-8") as f:
                    jobs_payload = json.load(f)
                for job in jobs_payload.get("jobs", []):
                    jid = as_text(job.get("job_id"))
                    if jid:
                        jobs_fit_data[jid] = job
            except Exception as e:
                print(f"Warning: Could not load jobs fit data: {e}")

    # Load context docs for LLM personalization
    context_docs = load_context_docs() if args.use_llm else {}

    rows = generate_messages(
        payload=payload,
        profile_summary=profile_summary,
        max_per_role=max(1, args.max_per_role),
        use_llm=args.use_llm,
        jobs_fit_data=jobs_fit_data,
        context_docs=context_docs,
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
            "llm_mode": args.use_llm,
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
