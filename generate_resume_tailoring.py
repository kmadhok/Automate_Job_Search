#!/usr/bin/env python3
"""Generate resume-tailoring suggestions for each scraped job description."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from pypdf import PdfReader

KEYWORD_LIBRARY = [
    "python",
    "sql",
    "pyspark",
    "spark",
    "airflow",
    "dbt",
    "snowflake",
    "aws",
    "gcp",
    "azure",
    "machine learning",
    "deep learning",
    "large language models",
    "llm",
    "retrieval augmented generation",
    "rag",
    "prompt engineering",
    "agents",
    "experimentation",
    "a/b testing",
    "forecasting",
    "time series",
    "causal inference",
    "optimization",
    "recommendation systems",
    "supply chain",
    "stakeholder management",
    "communication",
    "data visualization",
    "tableau",
    "power bi",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate tailored resume suggestions for each job in jobs JSON output."
    )
    parser.add_argument("--jobs-json", required=True, help="Path to jobs_*.json output file")
    parser.add_argument(
        "--resume-pdf",
        default=os.getenv("OUTREACH_RESUME_PDF", ""),
        help="Path to resume PDF used for tailoring context.",
    )
    parser.add_argument(
        "--resume-text-file",
        default="",
        help="Optional plain-text resume path (used with or instead of --resume-pdf).",
    )
    parser.add_argument(
        "--profile-summary",
        default=os.getenv("OUTREACH_PROFILE_SUMMARY", ""),
        help="Optional profile summary fallback context.",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=20,
        help="Maximum jobs to process (0 = all jobs).",
    )
    parser.add_argument(
        "--model",
        default=os.getenv("RESUME_TAILOR_MODEL", os.getenv("OPENAI_MODEL", "gpt-4o-mini")),
        help="OpenAI model for tailoring generation.",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.2,
        help="Sampling temperature for LLM-based generation.",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Disable OpenAI calls and use deterministic fallback only.",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Output JSON path (default: output/resume_tailoring_<timestamp>.json)",
    )
    parser.add_argument(
        "--output-csv",
        default="",
        help="Output CSV path (default: output/resume_tailoring_<timestamp>.csv)",
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


def load_payload(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError("Expected top-level JSON object")
    return payload


def extract_resume_text(pdf_path: Path) -> str:
    reader = PdfReader(str(pdf_path))
    chunks: List[str] = []
    for page in reader.pages:
        try:
            chunks.append(page.extract_text() or "")
        except Exception:
            continue
    return normalize_whitespace("\n".join(chunks))


def load_resume_context(resume_pdf: str, resume_text_file: str, profile_summary: str) -> str:
    chunks: List[str] = []

    if resume_pdf:
        pdf_path = Path(resume_pdf).expanduser().resolve()
        if not pdf_path.exists():
            raise FileNotFoundError(f"Resume PDF not found: {pdf_path}")
        chunks.append(extract_resume_text(pdf_path))

    if resume_text_file:
        txt_path = Path(resume_text_file).expanduser().resolve()
        if not txt_path.exists():
            raise FileNotFoundError(f"Resume text file not found: {txt_path}")
        chunks.append(normalize_whitespace(txt_path.read_text(encoding="utf-8")))

    if profile_summary:
        chunks.append(normalize_whitespace(profile_summary))

    merged = normalize_whitespace("\n".join(chunk for chunk in chunks if chunk))
    return merged


def top_keyword_hits(job_text: str, limit: int = 10) -> List[str]:
    lowered = job_text.lower()
    hits = []
    for term in KEYWORD_LIBRARY:
        if term in lowered:
            hits.append(term)
    return hits[:limit]


def top_phrases(job_text: str, limit: int = 8) -> List[str]:
    cleaned = re.sub(r"[^a-zA-Z0-9+/#\- ]", " ", job_text.lower())
    cleaned = normalize_whitespace(cleaned)

    phrase_candidates = re.findall(r"\b[a-z][a-z0-9+/#\-]{2,}(?:\s+[a-z0-9+/#\-]{2,}){1,3}\b", cleaned)
    counts: Dict[str, int] = {}
    stop = {
        "you will",
        "we are",
        "the team",
        "this role",
        "job description",
        "equal opportunity",
        "and or",
    }
    for phrase in phrase_candidates:
        phrase = normalize_whitespace(phrase)
        if phrase in stop:
            continue
        if len(phrase) < 8:
            continue
        counts[phrase] = counts.get(phrase, 0) + 1

    sorted_phrases = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    return [phrase for phrase, _ in sorted_phrases[:limit]]


def deterministic_tailor(job: Dict[str, Any], resume_context: str) -> Dict[str, Any]:
    title = as_text(job.get("title"))
    company = as_text(job.get("company"))
    description = normalize_whitespace(as_text(job.get("description")))
    role_focus = top_keyword_hits(description, limit=8)
    mirror_phrases = top_phrases(description, limit=6)

    if not role_focus:
        role_focus = ["machine learning", "python", "sql"]

    bullets = [
        f"Aligned project summary to emphasize {role_focus[0]} applied to measurable business outcomes.",
        f"Added language reflecting {role_focus[1] if len(role_focus) > 1 else 'cross-functional collaboration'} used in the job description.",
        f"Highlighted impact metrics and ownership that map to {title} expectations at {company}.",
    ]

    summary = (
        f"Tailor your headline/summary for {title} at {company} by mirroring the role language around "
        f"{', '.join(role_focus[:4])}. Focus on one to two shipped projects with quantified impact."
    )

    gaps = []
    if resume_context:
        lowered_resume = resume_context.lower()
        for term in role_focus[:6]:
            if term.lower() not in lowered_resume:
                gaps.append(f"Add explicit evidence for '{term}'.")

    return {
        "tailored_summary": summary,
        "resume_bullets": bullets,
        "ats_keywords": role_focus,
        "language_to_mirror": mirror_phrases,
        "gaps_to_address": gaps[:6],
        "cover_note": (
            f"I am excited about this {title} role because it aligns with my experience building "
            "data/AI solutions tied to business outcomes."
        ),
        "generation_mode": "deterministic",
        "error": "",
    }


def call_llm_tailor(
    *,
    client: Any,
    model: str,
    temperature: float,
    job: Dict[str, Any],
    resume_context: str,
) -> Dict[str, Any]:
    job_title = as_text(job.get("title"))
    company = as_text(job.get("company"))
    location = as_text(job.get("location"))
    job_url = as_text(job.get("url"))
    description = normalize_whitespace(as_text(job.get("description")))

    prompt = {
        "task": "Resume tailoring for one target job",
        "output_schema": {
            "tailored_summary": "string, 2-3 sentences",
            "resume_bullets": [
                "3 concise bullet rewrites that mirror the job language and show impact metrics"
            ],
            "ats_keywords": ["8-15 keywords/phrases"],
            "language_to_mirror": ["5-10 exact or near-exact phrases from JD to mirror"],
            "gaps_to_address": ["up to 8 honest gaps or weak evidence areas"],
            "cover_note": "4-6 line custom cover note",
        },
        "guidelines": [
            "Do not fabricate experience.",
            "Prefer measurable impact wording.",
            "Mirror JD terms without keyword stuffing.",
            "Keep bullets direct and resume-ready.",
        ],
        "job": {
            "title": job_title,
            "company": company,
            "location": location,
            "url": job_url,
            "description": description[:9000],
        },
        "candidate_context": resume_context[:10000],
    }

    response = client.chat.completions.create(
        model=model,
        temperature=temperature,
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an expert resume coach for data/AI roles. Return valid JSON only and follow the "
                    "exact schema keys requested by the user."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(prompt, ensure_ascii=False),
            },
        ],
    )

    content = (response.choices[0].message.content or "{}").strip()
    parsed = json.loads(content)
    if not isinstance(parsed, dict):
        raise ValueError("LLM response was not a JSON object")

    def ensure_list(key: str) -> List[str]:
        raw = parsed.get(key, [])
        if isinstance(raw, list):
            return [normalize_whitespace(as_text(item)) for item in raw if normalize_whitespace(as_text(item))]
        if isinstance(raw, str):
            one = normalize_whitespace(raw)
            return [one] if one else []
        return []

    result = {
        "tailored_summary": normalize_whitespace(as_text(parsed.get("tailored_summary"))),
        "resume_bullets": ensure_list("resume_bullets")[:5],
        "ats_keywords": ensure_list("ats_keywords")[:20],
        "language_to_mirror": ensure_list("language_to_mirror")[:12],
        "gaps_to_address": ensure_list("gaps_to_address")[:10],
        "cover_note": normalize_whitespace(as_text(parsed.get("cover_note"))),
        "generation_mode": "llm",
        "error": "",
    }

    if not result["tailored_summary"]:
        raise ValueError("Missing tailored_summary in LLM output")
    if not result["resume_bullets"]:
        raise ValueError("Missing resume_bullets in LLM output")

    return result


def build_rows(
    jobs: List[Dict[str, Any]],
    resume_context: str,
    client: Any,
    model: str,
    temperature: float,
    disable_llm: bool,
) -> Tuple[List[Dict[str, Any]], int, int]:
    rows: List[Dict[str, Any]] = []
    llm_success = 0
    llm_failures = 0

    for job in jobs:
        if not isinstance(job, dict):
            continue

        base = {
            "job_id": as_text(job.get("job_id")),
            "job_title": as_text(job.get("title")),
            "company": as_text(job.get("company")),
            "location": as_text(job.get("location")),
            "job_url": as_text(job.get("url")),
        }

        tailored: Dict[str, Any]
        if not disable_llm and client is not None:
            try:
                tailored = call_llm_tailor(
                    client=client,
                    model=model,
                    temperature=temperature,
                    job=job,
                    resume_context=resume_context,
                )
                llm_success += 1
            except Exception as exc:
                llm_failures += 1
                tailored = deterministic_tailor(job, resume_context)
                tailored["error"] = f"LLM failed: {exc}"
                tailored["generation_mode"] = "deterministic_fallback"
        else:
            tailored = deterministic_tailor(job, resume_context)

        rows.append(
            {
                **base,
                "tailored_summary": tailored.get("tailored_summary", ""),
                "resume_bullets": as_text(tailored.get("resume_bullets", [])),
                "ats_keywords": as_text(tailored.get("ats_keywords", [])),
                "language_to_mirror": as_text(tailored.get("language_to_mirror", [])),
                "gaps_to_address": as_text(tailored.get("gaps_to_address", [])),
                "cover_note": tailored.get("cover_note", ""),
                "generation_mode": tailored.get("generation_mode", "deterministic"),
                "error": tailored.get("error", ""),
            }
        )

    return rows, llm_success, llm_failures


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "job_id",
        "job_title",
        "company",
        "location",
        "job_url",
        "tailored_summary",
        "resume_bullets",
        "ats_keywords",
        "language_to_mirror",
        "gaps_to_address",
        "cover_note",
        "generation_mode",
        "error",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    jobs_path = Path(args.jobs_json)
    if not jobs_path.exists():
        raise FileNotFoundError(f"Jobs JSON not found: {jobs_path}")

    payload = load_payload(jobs_path)
    jobs_raw = payload.get("jobs", [])
    if not isinstance(jobs_raw, list):
        raise ValueError("Expected 'jobs' to be a list")

    jobs: List[Dict[str, Any]] = [job for job in jobs_raw if isinstance(job, dict)]
    if args.max_jobs > 0:
        jobs = jobs[: args.max_jobs]

    resume_context = load_resume_context(
        resume_pdf=args.resume_pdf,
        resume_text_file=args.resume_text_file,
        profile_summary=args.profile_summary,
    )

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    client = None
    if not args.no_llm and api_key:
        try:
            from openai import OpenAI  # type: ignore

            client = OpenAI(api_key=api_key)
        except Exception as exc:
            print(f"OpenAI client unavailable; falling back to deterministic mode: {exc}")

    rows, llm_success, llm_failures = build_rows(
        jobs=jobs,
        resume_context=resume_context,
        client=client,
        model=args.model,
        temperature=args.temperature,
        disable_llm=args.no_llm,
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)
    out_json = Path(args.output_json) if args.output_json else output_dir / f"resume_tailoring_{timestamp}.json"
    out_csv = Path(args.output_csv) if args.output_csv else output_dir / f"resume_tailoring_{timestamp}.csv"

    report = {
        "metadata": {
            "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "source_jobs_file": str(jobs_path),
            "jobs_processed": len(jobs),
            "llm_enabled": bool(client) and not args.no_llm,
            "llm_model": args.model,
            "llm_success": llm_success,
            "llm_failures": llm_failures,
            "resume_context_provided": bool(resume_context),
            "resume_pdf": args.resume_pdf,
            "resume_text_file": args.resume_text_file,
        },
        "tailoring": rows,
    }

    write_json(out_json, report)
    write_csv(out_csv, rows)

    print(f"Saved resume tailoring JSON: {out_json}")
    print(f"Saved resume tailoring CSV: {out_csv}")
    print(f"Generated tailoring rows: {len(rows)}")
    if args.no_llm:
        print("LLM disabled; deterministic mode used.")
    elif client is None:
        print("OPENAI_API_KEY missing; deterministic mode used.")
    elif llm_failures:
        print(f"LLM completed with {llm_failures} fallback rows.")
    else:
        print("LLM tailoring completed successfully.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
