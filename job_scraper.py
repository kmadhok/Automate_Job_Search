"""Deterministic + LLM-powered web scraping for job descriptions."""

import asyncio
import json
import re
import time
from datetime import datetime
from typing import Dict, List, Optional

from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from crawl4ai.extraction_strategy import LLMExtractionStrategy
try:
    from crawl4ai.types import create_llm_config
except Exception:
    create_llm_config = None
from pydantic import BaseModel, Field

from config import OPENAI_API_KEY, OPENAI_MODEL, SCRAPE_DELAY, SCRAPE_TIMEOUT


class JobPosting(BaseModel):
    """Schema for LLM job posting extraction."""

    title: Optional[str] = Field(None, description="Job title")
    company: Optional[str] = Field(None, description="Company name")
    location: Optional[str] = Field(None, description="Job location (city, state, or remote)")
    employment_type: Optional[str] = Field(None, description="Full-time, Part-time, Contract, etc.")
    salary_range: Optional[str] = Field(None, description="Salary or compensation range if mentioned")
    posted_date: Optional[str] = Field(
        None,
        description="When the job was posted, preferably YYYY-MM-DD if available",
    )
    description: Optional[str] = Field(None, description="Full job description text")
    requirements: Optional[List[str]] = Field(None, description="Key requirements or qualifications")
    responsibilities: Optional[List[str]] = Field(None, description="Key responsibilities")
    benefits: Optional[List[str]] = Field(None, description="Benefits mentioned")
    application_deadline: Optional[str] = Field(None, description="Application deadline if mentioned")


def _text_or_none(value: Optional[str], min_len: int = 1) -> Optional[str]:
    if not value:
        return None
    collapsed = re.sub(r"\s+", " ", value).strip()
    if len(collapsed) < min_len:
        return None
    return collapsed


def _extract_job_id_from_url(url: str) -> Optional[str]:
    match = re.search(r"/jobs/view/(\d+)", url)
    if match:
        return match.group(1)
    return None


def _normalize_date(value: Optional[str]) -> Optional[str]:
    text = _text_or_none(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.date().isoformat()
    except Exception:
        return text


def _read_jsonld_blocks(soup: BeautifulSoup) -> List[Dict]:
    blocks = []
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        if isinstance(parsed, list):
            blocks.extend(item for item in parsed if isinstance(item, dict))
        elif isinstance(parsed, dict):
            blocks.append(parsed)
    return blocks


def _deterministic_extract(raw_html: Optional[str], markdown: Optional[str]) -> Dict:
    """
    Extract high-value fields without LLM.

    This is the primary path for fields commonly available in JSON-LD and visible page DOM.
    """
    extracted = {
        "title": None,
        "company": None,
        "location": None,
        "posted_date": None,
        "description": None,
    }

    if raw_html:
        soup = BeautifulSoup(raw_html, "html.parser")

        for block in _read_jsonld_blocks(soup):
            block_type = str(block.get("@type", "")).lower()
            if block_type == "jobposting":
                title = _text_or_none(block.get("title"))

                company = None
                hiring_org = block.get("hiringOrganization")
                if isinstance(hiring_org, dict):
                    company = _text_or_none(hiring_org.get("name"))
                elif isinstance(hiring_org, str):
                    company = _text_or_none(hiring_org)

                location = None
                job_location = block.get("jobLocation")
                if isinstance(job_location, dict):
                    address = job_location.get("address")
                    if isinstance(address, dict):
                        locality = _text_or_none(address.get("addressLocality"))
                        region = _text_or_none(address.get("addressRegion"))
                        country = _text_or_none(address.get("addressCountry"))
                        location = ", ".join([x for x in [locality, region, country] if x]) or None

                description = None
                raw_desc = block.get("description")
                if raw_desc:
                    description = _text_or_none(
                        BeautifulSoup(str(raw_desc), "html.parser").get_text("\n")
                    )

                extracted["title"] = extracted["title"] or title
                extracted["company"] = extracted["company"] or company
                extracted["location"] = extracted["location"] or location
                extracted["posted_date"] = extracted["posted_date"] or _normalize_date(
                    block.get("datePosted")
                )
                extracted["description"] = extracted["description"] or description

        if not extracted["title"]:
            title_node = soup.select_one("h1") or soup.select_one("title")
            if title_node:
                extracted["title"] = _text_or_none(title_node.get_text(" "))

        if not extracted["description"]:
            description_selectors = [
                ".show-more-less-html__markup",
                ".jobs-description__content",
                ".jobs-description-content__text",
                "[data-test-id='job-description']",
                "article",
            ]
            for selector in description_selectors:
                node = soup.select_one(selector)
                if not node:
                    continue
                text = _text_or_none(node.get_text("\n"), min_len=100)
                if text:
                    extracted["description"] = text
                    break

    if not extracted["description"] and markdown:
        # Last deterministic fallback: cleaned markdown from crawler output.
        extracted["description"] = _text_or_none(markdown, min_len=150)

    return extracted


def _parse_llm_extraction(extracted_content: Optional[str]) -> Dict:
    if not extracted_content:
        return {}
    try:
        parsed = json.loads(extracted_content)
        if isinstance(parsed, list):
            return parsed[0] if parsed else {}
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return {}
    return {}


def _merge_job_data(primary: Dict, secondary: Dict) -> Dict:
    merged = primary.copy()
    for key, value in secondary.items():
        if value in (None, "", [], {}):
            continue
        merged[key] = value
    return merged


def _build_extraction_strategy() -> LLMExtractionStrategy:
    kwargs = {
        "schema": JobPosting.model_json_schema(),
        "extraction_type": "schema",
        "instruction": (
            "Extract job posting information from this page. "
            "Focus on core listing content. Ignore site navigation and unrelated sections."
        ),
    }

    if create_llm_config is not None:
        kwargs["llm_config"] = create_llm_config(
            provider=f"openai/{OPENAI_MODEL}",
            api_token=OPENAI_API_KEY,
        )
    else:
        kwargs["provider"] = f"openai/{OPENAI_MODEL}"
        kwargs["api_token"] = OPENAI_API_KEY

    return LLMExtractionStrategy(**kwargs)


async def scrape_job_page(url: str, crawler: AsyncWebCrawler) -> Dict:
    """
    Scrape a single job posting using deterministic extraction with LLM enrichment.
    """
    try:
        print(f"  Scraping: {url}")

        extraction_strategy = _build_extraction_strategy()

        result = await crawler.arun(
            url=url,
            extraction_strategy=extraction_strategy,
            bypass_cache=True,
            timeout=SCRAPE_TIMEOUT,
        )

        if not result.success:
            print(f"  Failed to crawl: {result.error_message}")
            return {
                "url": url,
                "job_id": _extract_job_id_from_url(url),
                "error": result.error_message or "Unknown crawl error",
                "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }

        raw_html = getattr(result, "cleaned_html", None) or getattr(result, "html", None)
        markdown = getattr(result, "markdown", None)

        deterministic_data = _deterministic_extract(raw_html, markdown)
        llm_data = _parse_llm_extraction(getattr(result, "extracted_content", None))

        # Deterministic data is preferred for stability; LLM fills missing fields.
        job_data = _merge_job_data(deterministic_data, llm_data)

        if not job_data.get("description") and llm_data.get("description"):
            job_data["description"] = llm_data["description"]

        if not any(job_data.get(field) for field in ("title", "company", "description")):
            return {
                "url": url,
                "job_id": _extract_job_id_from_url(url),
                "error": "No extractable job content found",
                "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "raw_markdown": (markdown or "")[:1000] or None,
            }

        job_data["url"] = url
        job_data["job_id"] = job_data.get("job_id") or _extract_job_id_from_url(url)
        job_data["scraped_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        job_data["raw_markdown"] = (markdown or "")[:1000] or None

        print(f"  Success: {job_data.get('title', 'Unknown title')}")
        return job_data

    except Exception as e:
        print(f"  Error scraping {url}: {e}")
        return {
            "url": url,
            "job_id": _extract_job_id_from_url(url),
            "error": str(e),
            "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }


async def scrape_multiple_jobs(urls: List[str]) -> List[Dict]:
    """
    Scrape multiple job URLs asynchronously.
    """
    jobs = []

    async with AsyncWebCrawler(verbose=True) as crawler:
        print(f"\nStarting to scrape {len(urls)} job postings...")

        for i, url in enumerate(urls):
            print(f"\n[{i + 1}/{len(urls)}]")
            job_data = await scrape_job_page(url, crawler)
            jobs.append(job_data)

            if i < len(urls) - 1:
                print(f"  Waiting {SCRAPE_DELAY}s before next request...")
                await asyncio.sleep(SCRAPE_DELAY)

    return jobs


def scrape_jobs_sync(urls: List[str]) -> List[Dict]:
    """
    Synchronous wrapper for async scraping.
    """
    return asyncio.run(scrape_multiple_jobs(urls))
