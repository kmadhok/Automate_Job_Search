#!/usr/bin/env python3
"""Evaluate job fit against career preferences.

Scores each scraped job using deterministic keyword/title/company matching,
with an optional LLM refinement pass for yellow-tier jobs.

Usage:
    python evaluate_job_fit.py --jobs-json output/jobs_20260328_100000.json
    python evaluate_job_fit.py --jobs-json output/jobs_*.json --no-llm --min-score 40
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Title lists
# ---------------------------------------------------------------------------

TITLES_THAT_FIT = [
    "applied ai engineer", "ai engineer", "llm engineer",
    "ai/ml applications engineer", "applied scientist",
    "machine learning engineer",
]

TITLES_THAT_DONT_FIT = [
    "data analyst", "ml platform", "ml infra", "mlops",
    "research scientist", "data engineer",
    "fine-tuning", "pre-training",
]

# ---------------------------------------------------------------------------
# Keyword lists
# ---------------------------------------------------------------------------

GREEN_KEYWORDS = [
    "RAG", "retrieval-augmented", "retrieval augmented generation",
    "agentic", "multi-agent", "copilot", "tool use", "tool-calling",
    "LLM evaluation", "context engineering", "prompt engineering",
    "LLM application", "LLM-powered", "AI agent",
]

RED_KEYWORDS = [
    "dashboarding", "reporting", "BI ", "business intelligence",
    "ETL only", "data pipeline only", "pre-training",
    "fine-tuning only", "model training",
]

YELLOW_KEYWORDS = [
    "fine-tuning",
    "classical ML", "traditional ML",
    "startup",
]

# ---------------------------------------------------------------------------
# Company tiers
# ---------------------------------------------------------------------------

TIER_1_COMPANIES = [
    "Google", "DeepMind", "Meta", "Anthropic", "OpenAI",
    "Microsoft", "Apple",
]

TIER_2_COMPANIES = [
    "Databricks", "Snowflake", "Salesforce", "Stripe", "Uber",
    "Airbnb", "Netflix", "Palantir", "Scale AI", "Cohere", "Bloomberg",
]

TIER_3_COMPANIES = [
    "JPMorgan", "Capital One", "Walmart",
]

# ---------------------------------------------------------------------------
# Location preferences
# ---------------------------------------------------------------------------

PREFERRED_LOCATIONS = [
    "chicago", "san francisco", "sf", "bay area",
    "new york", "nyc", "seattle", "remote",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ci_contains(haystack: str, needle: str) -> bool:
    """Case-insensitive substring check with word-boundary awareness for
    short keywords (e.g. 'BI ' keeps trailing space to avoid 'ability')."""
    return needle.lower() in haystack.lower()


def _build_text_block(job: dict) -> str:
    """Concatenate description + requirements + responsibilities into one
    searchable text block."""
    parts = []
    if job.get("description"):
        parts.append(job["description"])
    for field in ("requirements", "responsibilities"):
        val = job.get(field)
        if isinstance(val, list):
            parts.append(" ".join(val))
        elif isinstance(val, str):
            parts.append(val)
    return " ".join(parts)


def _match_company_tier(company: str) -> int:
    """Return company tier (1-3) or 0 if untiered."""
    company_lower = company.lower()
    for name in TIER_1_COMPANIES:
        if name.lower() in company_lower:
            return 1
    for name in TIER_2_COMPANIES:
        if name.lower() in company_lower:
            return 2
    for name in TIER_3_COMPANIES:
        if name.lower() in company_lower:
            return 3
    return 0


# ---------------------------------------------------------------------------
# Core evaluation
# ---------------------------------------------------------------------------


def evaluate_job(job: dict, use_llm: bool = False, openai_client=None,
                 openai_model: str = "gpt-4o") -> dict:
    """Run all evaluation steps on a single job dict.

    Returns the job dict enriched with fit_score, fit_tier, fit_signals,
    fit_summary, recommended_action, keywords_for_resume, company_tier.
    """
    title = job.get("title") or ""
    company = job.get("company") or ""
    location = job.get("location") or ""
    text_block = _build_text_block(job)

    green_flags: list[str] = []
    yellow_flags: list[str] = []
    red_flags: list[str] = []

    # --- Step 1: Title check ---
    title_lower = title.lower()
    for t in TITLES_THAT_DONT_FIT:
        if t.lower() in title_lower:
            red_flags.append(f"title matches exclusion list: {t}")
            break

    for t in TITLES_THAT_FIT:
        if t.lower() in title_lower:
            green_flags.append(f"title matches target list: {t}")
            break

    if title_lower.strip() == "data scientist":
        yellow_flags.append("Data Scientist title — check if role is AI product-focused")

    # --- Step 2: Description keyword scan ---
    matched_green_keywords = []
    for kw in GREEN_KEYWORDS:
        if _ci_contains(text_block, kw):
            green_flags.append(f"mentions {kw}")
            matched_green_keywords.append(kw)

    for kw in RED_KEYWORDS:
        if _ci_contains(text_block, kw):
            red_flags.append(f"mentions {kw}")

    for kw in YELLOW_KEYWORDS:
        if _ci_contains(text_block, kw):
            yellow_flags.append(f"mentions {kw}")

    # Special case: Senior Data Scientist is fit ONLY if green keywords present
    if "senior data scientist" in title_lower and not matched_green_keywords:
        if not any("title matches target list" in f for f in green_flags):
            yellow_flags.append("Senior Data Scientist without green keywords")

    # --- Step 3: Company tier ---
    company_tier = _match_company_tier(company)
    if company_tier == 1:
        green_flags.append("company is Tier 1")
    elif company_tier == 2:
        green_flags.append("company is Tier 2")

    # --- Step 4: Location check ---
    location_lower = location.lower()
    location_matched = False
    for loc in PREFERRED_LOCATIONS:
        if loc in location_lower:
            green_flags.append(f"location match: {location}")
            location_matched = True
            break
    if not location_matched and location:
        yellow_flags.append(f"location may not match preferences: {location}")

    # --- Step 5: Scoring (before LLM) ---
    score = 50

    green_bonus = min(len(green_flags) * 10, 40)
    score += green_bonus

    score -= len(red_flags) * 20
    score += len(yellow_flags) * 5

    if company_tier == 1:
        score += 15
    elif company_tier == 2:
        score += 10
    elif company_tier == 3:
        score += 5

    if any("location match" in f for f in green_flags):
        score += 5

    score = max(0, min(100, score))

    # Determine initial tier
    if score >= 70:
        fit_tier = "green"
    elif score >= 40:
        fit_tier = "yellow"
    else:
        fit_tier = "red"

    # --- Step 5b: LLM refinement for yellow tier ---
    llm_refined = False
    if use_llm and fit_tier == "yellow" and openai_client is not None:
        try:
            llm_result = _llm_refine(
                openai_client, openai_model, title, company,
                text_block[:3000],
            )
            if llm_result:
                new_tier = llm_result.get("tier", "").lower()
                reason = llm_result.get("reason", "")
                additional_kw = llm_result.get("additional_keywords", [])

                if new_tier in ("green", "red") and new_tier != fit_tier:
                    fit_tier = new_tier
                    green_flags.append(f"LLM refinement: {reason}")
                    llm_refined = True
                    # Adjust score based on LLM
                    if new_tier == "green":
                        score = max(score, 70)
                    elif new_tier == "red":
                        score = min(score, 39)

                if additional_kw:
                    matched_green_keywords.extend(additional_kw)
        except Exception as e:
            # LLM failure is non-fatal
            yellow_flags.append(f"LLM refinement failed: {e}")

    # Re-determine tier after possible LLM adjustment
    if score >= 70:
        fit_tier = "green"
        recommended_action = "apply"
    elif score >= 40:
        fit_tier = "yellow"
        recommended_action = "review"
    else:
        fit_tier = "red"
        recommended_action = "skip"

    # --- Step 6: Keywords for resume ---
    keywords_for_resume = list(dict.fromkeys(matched_green_keywords))[:10]

    # --- Fit summary ---
    top_reason = green_flags[0] if green_flags else (
        yellow_flags[0] if yellow_flags else "no strong signals"
    )
    tier_label = f"Tier {company_tier}" if company_tier else "untiered"
    fit_summary = (
        f"{fit_tier.capitalize()} fit — {title} role at {company} "
        f"({tier_label}). {top_reason}."
    )

    # --- Enrich and return ---
    enriched = dict(job)
    enriched.update({
        "fit_score": score,
        "fit_tier": fit_tier,
        "company_tier": company_tier,
        "fit_signals": {
            "green_flags": green_flags,
            "yellow_flags": yellow_flags,
            "red_flags": red_flags,
        },
        "fit_summary": fit_summary,
        "recommended_action": recommended_action,
        "keywords_for_resume": keywords_for_resume,
    })
    return enriched


def _llm_refine(client, model: str, title: str, company: str,
                description: str) -> dict | None:
    """Ask the LLM to refine a yellow-tier job's classification."""
    prompt = f"""Given this job description and these career preferences, is this a green, yellow, or red fit?

Job: {title} at {company}
Description (first 3000 chars): {description}

Career preferences summary:
- Target: Applied AI Engineer building LLM apps, RAG, agents, copilots
- Green: RAG, agentic, copilot, tool-use, LLM evaluation, context engineering
- Red: Dashboarding, BI, ETL-only, pre-training/fine-tuning-only, no LLM/AI content
- Yellow: Data Scientist with AI product work, fine-tuning as component, classical ML emphasis

Respond with JSON:
{{"tier": "green|yellow|red", "reason": "one sentence explanation", "additional_keywords": ["list", "of", "resume", "keywords"]}}"""

    response = client.chat.completions.create(
        model=model,
        temperature=0.1,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You classify job fit. Return valid JSON only."},
            {"role": "user", "content": prompt},
        ],
    )
    content = (response.choices[0].message.content or "{}").strip()
    return json.loads(content)


# ---------------------------------------------------------------------------
# Main / CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate job fit against career preferences."
    )
    parser.add_argument("--jobs-json", required=True, help="Path to jobs_*.json")
    parser.add_argument("--output-json", default="",
                        help="Output path (default: output/jobs_evaluated_<ts>.json)")
    parser.add_argument("--output-csv", default="", help="Output CSV path")
    parser.add_argument("--no-llm", action="store_true",
                        help="Skip LLM refinement for yellow jobs")
    parser.add_argument("--min-score", type=int, default=0,
                        help="Only output jobs >= this score")
    args = parser.parse_args()

    # Load jobs
    jobs_path = Path(args.jobs_json)
    if not jobs_path.exists():
        print(f"Error: {jobs_path} not found")
        sys.exit(1)

    with open(jobs_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    jobs = payload.get("jobs", [])
    print(f"Loaded {len(jobs)} jobs from {jobs_path.name}")

    # Set up LLM client if needed
    use_llm = not args.no_llm
    openai_client = None
    openai_model = "gpt-4o"

    if use_llm:
        try:
            from config import OPENAI_API_KEY, OPENAI_MODEL, FIT_USE_LLM_FOR_YELLOW
            if not FIT_USE_LLM_FOR_YELLOW:
                use_llm = False
            elif OPENAI_API_KEY:
                from openai import OpenAI
                openai_client = OpenAI(api_key=OPENAI_API_KEY)
                openai_model = OPENAI_MODEL
            else:
                print("Warning: OPENAI_API_KEY not set, skipping LLM refinement")
                use_llm = False
        except ImportError:
            print("Warning: openai/config not available, skipping LLM refinement")
            use_llm = False

    # Evaluate each job
    evaluated_jobs = []
    green_count = 0
    yellow_count = 0
    red_count = 0
    llm_refinements = 0

    for job in jobs:
        result = evaluate_job(
            job, use_llm=use_llm, openai_client=openai_client,
            openai_model=openai_model,
        )
        tier = result["fit_tier"]
        if tier == "green":
            green_count += 1
        elif tier == "yellow":
            yellow_count += 1
        else:
            red_count += 1

        if any("LLM refinement" in f for f in
               result.get("fit_signals", {}).get("green_flags", [])):
            llm_refinements += 1

        if result["fit_score"] >= args.min_score:
            evaluated_jobs.append(result)

    print(f"\nResults: {green_count} green, {yellow_count} yellow, {red_count} red")
    if use_llm:
        print(f"LLM refinements: {llm_refinements}")
    print(f"Jobs passing min-score filter ({args.min_score}): {len(evaluated_jobs)}")

    # Build output
    output_payload = {
        "metadata": {
            "source_jobs_file": jobs_path.name,
            "evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
            "total_jobs": len(jobs),
            "green_count": green_count,
            "yellow_count": yellow_count,
            "red_count": red_count,
            "llm_refinements": llm_refinements,
        },
        "jobs": evaluated_jobs,
    }

    # Preserve original metadata
    if "metadata" in payload:
        output_payload["metadata"]["original_metadata"] = payload["metadata"]

    # Write JSON
    if args.output_json:
        out_path = Path(args.output_json)
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = Path("output") / f"jobs_evaluated_{ts}.json"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, indent=2, ensure_ascii=False)
    print(f"Saved: {out_path}")

    # Optionally write CSV
    if args.output_csv:
        try:
            import pandas as pd
            rows = []
            for j in evaluated_jobs:
                row = {
                    "title": j.get("title"),
                    "company": j.get("company"),
                    "location": j.get("location"),
                    "fit_score": j.get("fit_score"),
                    "fit_tier": j.get("fit_tier"),
                    "company_tier": j.get("company_tier"),
                    "recommended_action": j.get("recommended_action"),
                    "fit_summary": j.get("fit_summary"),
                    "keywords_for_resume": ", ".join(j.get("keywords_for_resume", [])),
                    "url": j.get("url"),
                }
                rows.append(row)
            pd.DataFrame(rows).to_csv(args.output_csv, index=False)
            print(f"Saved CSV: {args.output_csv}")
        except ImportError:
            print("Warning: pandas not available, skipping CSV output")


if __name__ == "__main__":
    main()
