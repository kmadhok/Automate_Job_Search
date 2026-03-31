#!/usr/bin/env python3
"""
Job deduplication and folder management for the unified job search pipeline.

Integrates the workspace registry (seen_jobs.json) with the repo's pipeline.
Maintains folder structure and generates tailored resumes in the workspace.

Usage:
    mgr = JobManager()  # uses WORKSPACE_DAILY_DIR from config

    # Check by company+title
    if not mgr.is_seen("palantir", "Forward Deployed AI Engineer"):
        mgr.register_job(...)

    # Check by URL (checks linkedin_job_id)
    if not mgr.is_seen_by_url("https://linkedin.com/jobs/view/12345"):
        mgr.register_job(...)

    # Register from scraped job data (with optional fit evaluation)
    mgr.register_from_scraped(job_data, fit_data, source="gmail")
"""

import json
import re
from datetime import date
from pathlib import Path
from typing import Optional


class JobManager:
    """Manages job registry, deduplication, and per-job folder creation."""

    def __init__(self, base_dir: str | None = None):
        """
        Initialize JobManager.

        Args:
            base_dir: Path to the directory containing seen_jobs.json and jobs/.
                      Defaults to WORKSPACE_DAILY_DIR from config.
        """
        if base_dir is None:
            from config import WORKSPACE_DAILY_DIR
            base_dir = str(WORKSPACE_DAILY_DIR)
        self.base_dir = Path(base_dir)
        self.jobs_dir = self.base_dir / "jobs"
        self.registry_path = self.base_dir / "seen_jobs.json"
        self._registry = self._load_registry()

    def _load_registry(self) -> dict:
        """Load the seen jobs registry, or create empty one."""
        if self.registry_path.exists():
            with open(self.registry_path, "r") as f:
                return json.load(f)
        return {"jobs": {}}

    def _save_registry(self):
        """Persist the registry to disk."""
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.registry_path, "w") as f:
            json.dump(self._registry, f, indent=2, default=str)

    @staticmethod
    def _make_key(company: str, title: str) -> str:
        """Create a stable dedup key from company + title."""
        slug = f"{company}_{title}".lower()
        slug = re.sub(r'[^a-z0-9]+', '-', slug).strip('-')
        return slug

    @staticmethod
    def _make_folder_name(company: str, title: str) -> str:
        """Create a filesystem-safe folder name."""
        slug = f"{company}_{title}".lower()
        slug = re.sub(r'[^a-z0-9]+', '-', slug).strip('-')
        # Cap length to avoid filesystem issues
        return slug[:80]

    @staticmethod
    def _extract_linkedin_job_id(url: str) -> Optional[str]:
        """Extract LinkedIn job ID from a URL.

        Handles formats like:
        - https://www.linkedin.com/jobs/view/1234567890/
        - https://linkedin.com/jobs/view/1234567890?ref=a
        - https://www.linkedin.com/jobs/collections/recommended/?currentJobId=1234567890
        """
        if not url:
            return None

        # Pattern for /jobs/view/12345
        match = re.search(r'/jobs/view/(\d+)', url)
        if match:
            return match.group(1)

        # Pattern for currentJobId=12345
        match = re.search(r'currentJobId=(\d+)', url)
        if match:
            return match.group(1)

        return None

    def is_seen(self, company: str, title: str) -> bool:
        """Check if a job has already been registered by company+title."""
        key = self._make_key(company, title)
        return key in self._registry["jobs"]

    def is_seen_by_url(self, url: str) -> bool:
        """Check if a job URL has already been registered.

        Checks both the company+title slug AND the LinkedIn job ID.
        """
        job_id = self._extract_linkedin_job_id(url)
        if not job_id:
            # Fall back to just checking if any job has this URL
            return any(
                j.get("url") == url
                for j in self._registry["jobs"].values()
            )

        return any(
            j.get("linkedin_job_id") == job_id
            for j in self._registry["jobs"].values()
        )

    def get_job_by_url(self, url: str) -> Optional[dict]:
        """Get a job entry by its URL (checks linkedin_job_id)."""
        job_id = self._extract_linkedin_job_id(url)
        if job_id:
            for key, job in self._registry["jobs"].items():
                if job.get("linkedin_job_id") == job_id:
                    return {"key": key, **job}
        # Fallback to URL matching
        for key, job in self._registry["jobs"].items():
            if job.get("url") == url:
                return {"key": key, **job}
        return None

    def register_job(
        self,
        company: str,
        title: str,
        location: str,
        url: str,
        fit_notes: str,
        keywords: list[str],
        linkedin_job_id: Optional[str] = None,
        fit_score: int = 0,
        fit_tier: str = "unscored",
        source: str = "manual",
    ) -> dict:
        """
        Register a new job:
        1. Add to seen_jobs.json (with linkedin_job_id and fit data)
        2. Create a folder under jobs/
        3. Write a job_info.md
        4. Generate a tailored resume PDF (if generate_tailored_resume is available)

        Args:
            company: Company name
            title: Job title
            location: Job location
            url: Job posting URL
            fit_notes: Notes about why this job is a good fit
            keywords: Keywords to use for resume tailoring
            linkedin_job_id: LinkedIn job ID (extracted from URL)
            fit_score: Fit score from evaluation (0-100)
            fit_tier: Fit tier (green/yellow/red/unscored)
            source: Source of the job (gmail/daily_report/manual/scraped)

        Returns:
            Dict with paths created and metadata
        """
        key = self._make_key(company, title)
        folder_name = self._make_folder_name(company, title)
        job_dir = self.jobs_dir / folder_name
        job_dir.mkdir(parents=True, exist_ok=True)

        today = date.today().isoformat()

        # Extract LinkedIn job ID if not provided
        if not linkedin_job_id and url:
            linkedin_job_id = self._extract_linkedin_job_id(url)

        # 1. Write job_info.md
        info_path = job_dir / "job_info.md"
        info_content = f"""# {company} — {title}

| Field | Detail |
|-------|--------|
| **Company** | {company} |
| **Title** | {title} |
| **Location** | {location} |
| **URL** | [{url}]({url}) |
| **Date Found** | {today} |
| **Source** | {source} |
| **Fit Score** | {fit_score} |
| **Fit Tier** | {fit_tier} |
| **LinkedIn Job ID** | {linkedin_job_id or "N/A"} |

## Fit Notes
{fit_notes}

## Keywords Used for Resume Tailoring
{', '.join(keywords) if keywords else "None"}
"""
        with open(info_path, "w") as f:
            f.write(info_content)

        # 2. Generate tailored resume PDF (if reportlab is available)
        resume_path = None
        try:
            from generate_tailored_resume import generate_resume_pdf
            safe_company = re.sub(r'[^a-zA-Z0-9]', '_', company)
            resume_filename = f"Kanu_Madhok_Resume_{safe_company}.pdf"
            resume_path = job_dir / resume_filename
            generate_resume_pdf(str(resume_path), keywords)
        except ImportError:
            # reportlab not installed, skip PDF generation
            pass

        # 3. Update registry with new fields
        self._registry["jobs"][key] = {
            "company": company,
            "title": title,
            "location": location,
            "url": url,
            "linkedin_job_id": linkedin_job_id,
            "fit_score": fit_score,
            "fit_tier": fit_tier,
            "source": source,
            "date_first_seen": today,
            "folder": folder_name,
        }
        self._save_registry()

        result = {
            "folder": str(job_dir),
            "job_info": str(info_path),
            "key": key,
        }
        if resume_path:
            result["resume"] = str(resume_path)
        return result

    def register_from_scraped(
        self,
        job_data: dict,
        fit_data: Optional[dict] = None,
        source: str = "scraped",
    ) -> Optional[dict]:
        """
        Register a job from the scraping pipeline output.

        This is the primary integration point for the automation pipeline.

        Args:
            job_data: Dict with keys like company, title, location, url, job_id
            fit_data: Optional dict from evaluate_job_fit.py with fit_score,
                     fit_tier, fit_summary, keywords_for_resume
            source: Source identifier (gmail/daily_report/manual/scraped)

        Returns:
            Dict with paths created, or None if job already seen
        """
        # Check if already seen
        url = job_data.get("url", "")
        if self.is_seen_by_url(url):
            return None

        company = job_data.get("company", "Unknown")
        title = job_data.get("title", "Unknown")

        if self.is_seen(company, title):
            return None

        # Extract fit data if available
        fit_score = 0
        fit_tier = "unscored"
        fit_notes = ""
        keywords = []

        if fit_data:
            fit_score = fit_data.get("fit_score", 0)
            fit_tier = fit_data.get("fit_tier", "unscored")
            fit_notes = fit_data.get("fit_summary", "")
            keywords = fit_data.get("keywords_for_resume", [])

        # Fallback to basic fit notes if no fit_data
        if not fit_notes:
            fit_notes = job_data.get("fit_notes", "")

        # Fallback keywords
        if not keywords:
            keywords = job_data.get("keywords", [])

        # Fallback to description-based extraction
        if not keywords and job_data.get("description"):
            keywords = self._extract_keywords_from_description(job_data.get("description", ""))

        return self.register_job(
            company=company,
            title=title,
            location=job_data.get("location", "Unknown"),
            url=url,
            fit_notes=fit_notes,
            keywords=keywords,
            linkedin_job_id=job_data.get("job_id") or job_data.get("linkedin_job_id"),
            fit_score=fit_score,
            fit_tier=fit_tier,
            source=source,
        )

    def _extract_keywords_from_description(self, description: str) -> list[str]:
        """Extract resume-relevant keywords from a job description."""
        # Common keywords for AI/ML roles
        keyword_library = [
            "RAG", "retrieval-augmented", "agentic", "multi-agent", "copilot",
            "tool use", "LLM evaluation", "context engineering", "prompt engineering",
            "fine-tuning", "pre-training", "embedding", "vector database",
            "LangChain", "LlamaIndex", "OpenAI", "Anthropic", "Gemini",
            "MCP", "model context protocol", "knowledge graph",
            "Python", "SQL", "BigQuery", "AWS", "GCP", "Azure",
            "Docker", "Kubernetes", "FastAPI", "Streamlit",
        ]
        found = []
        desc_lower = description.lower()
        for kw in keyword_library:
            if kw.lower() in desc_lower:
                found.append(kw)
        return found[:10]  # Return top 10

    def update_fit_evaluation(self, company: str, title: str, fit_data: dict) -> bool:
        """
        Update an existing job entry with fit evaluation data.

        Useful when fit evaluation happens after initial registration.

        Args:
            company: Company name
            title: Job title
            fit_data: Dict with fit_score, fit_tier, fit_summary, etc.

        Returns:
            True if updated, False if job not found
        """
        key = self._make_key(company, title)
        if key not in self._registry["jobs"]:
            return False

        self._registry["jobs"][key]["fit_score"] = fit_data.get("fit_score", 0)
        self._registry["jobs"][key]["fit_tier"] = fit_data.get("fit_tier", "unscored")
        self._save_registry()
        return True

    def get_stats(self) -> dict:
        """Return stats about the registry."""
        jobs = self._registry.get("jobs", {})
        return {
            "total_seen": len(jobs),
            "companies": list(set(j["company"] for j in jobs.values())),
            "by_tier": {
                "green": len([j for j in jobs.values() if j.get("fit_tier") == "green"]),
                "yellow": len([j for j in jobs.values() if j.get("fit_tier") == "yellow"]),
                "red": len([j for j in jobs.values() if j.get("fit_tier") == "red"]),
                "unscored": len([j for j in jobs.values() if j.get("fit_tier") == "unscored"]),
            },
            "by_source": {
                source: len([j for j in jobs.values() if j.get("source") == source])
                for source in set(j.get("source", "manual") for j in jobs.values())
            },
        }

    def get_jobs_by_tier(self, tier: str) -> list[dict]:
        """Get all jobs matching a specific fit tier."""
        jobs = self._registry.get("jobs", {})
        return [
            {"key": key, **job}
            for key, job in jobs.items()
            if job.get("fit_tier") == tier
        ]

    def export_to_list(self) -> list[dict]:
        """Export all jobs as a list for upload to Google Sheets."""
        jobs = self._registry.get("jobs", {})
        return [
            {
                "key": key,
                "company": job.get("company"),
                "title": job.get("title"),
                "location": job.get("location"),
                "url": job.get("url"),
                "fit_score": job.get("fit_score", 0),
                "fit_tier": job.get("fit_tier", "unscored"),
                "source": job.get("source", "manual"),
                "date_first_seen": job.get("date_first_seen"),
                "folder": job.get("folder"),
                "linkedin_job_id": job.get("linkedin_job_id"),
            }
            for key, job in jobs.items()
        ]


    def save_tailoring_notes(self, company: str, title: str, tailoring_data: dict) -> str | None:
        """Save resume tailoring suggestions to the job's folder."""
        key = self._make_key(company, title)
        entry = self._registry["jobs"].get(key)
        if not entry:
            return None

        folder_name = entry["folder"]
        job_dir = self.jobs_dir / folder_name
        notes_path = job_dir / "tailoring_notes.md"

        content = f"""# Resume Tailoring Notes \u2014 {company}, {title}

## Summary
{tailoring_data.get('tailored_summary', 'N/A')}

## Suggested Bullet Rewrites
{chr(10).join('- ' + b for b in tailoring_data.get('resume_bullets', []))}

## ATS Keywords
{', '.join(tailoring_data.get('ats_keywords', []))}

## Language to Mirror
{', '.join(tailoring_data.get('language_to_mirror', []))}

## Gaps to Address
{chr(10).join('- ' + g for g in tailoring_data.get('gaps_to_address', []))}

## Cover Note
{tailoring_data.get('cover_note', 'N/A')}
"""
        with open(notes_path, "w") as f:
            f.write(content)
        return str(notes_path)


def main():
    """CLI test: register a sample job."""
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace-dir", default=None, help="Path to workspace (default: from config)")
    parser.add_argument("--company", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--location", default="Remote")
    parser.add_argument("--url", default="https://example.com")
    parser.add_argument("--fit-notes", default="Test job")
    parser.add_argument("--keywords", default="RAG,LLM,agentic")
    parser.add_argument("--fit-score", type=int, default=0)
    parser.add_argument("--fit-tier", default="unscored")
    parser.add_argument("--source", default="manual")
    args = parser.parse_args()

    mgr = JobManager(args.workspace_dir)
    if mgr.is_seen(args.company, args.title):
        print(f"SKIP: {args.company} - {args.title} (already seen)")
    else:
        result = mgr.register_job(
            company=args.company,
            title=args.title,
            location=args.location,
            url=args.url,
            fit_notes=args.fit_notes,
            keywords=[k.strip() for k in args.keywords.split(",")],
            fit_score=args.fit_score,
            fit_tier=args.fit_tier,
            source=args.source,
        )
        print(f"CREATED: {result['folder']}")
        print(f"  Job info: {result['job_info']}")
        if result.get("resume"):
            print(f"  Resume: {result['resume']}")
        print(f"  Key: {result['key']}")


if __name__ == "__main__":
    main()
