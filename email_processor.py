"""Email fetching, tag filtering, and LinkedIn job URL extraction."""

import base64
import re
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from config import GMAIL_QUERY, GMAIL_QUERY_DAYS, LINKEDIN_SENDERS, MAX_EMAILS


LINKEDIN_HOSTS = ("linkedin.com", "www.linkedin.com", "lnkd.in")


def build_gmail_query(job_tags: Optional[List[str]] = None) -> str:
    """
    Build a Gmail query focused on LinkedIn job emails and optional tag terms.

    Args:
        job_tags: Optional list of terms to search for in subject/body

    Returns:
        str: Gmail search query
    """
    if GMAIL_QUERY.strip():
        return GMAIL_QUERY.strip()

    query_parts = []

    if LINKEDIN_SENDERS:
        sender_terms = " OR ".join(f"from:{sender}" for sender in LINKEDIN_SENDERS)
        query_parts.append(f"({sender_terms})")

    if GMAIL_QUERY_DAYS > 0:
        query_parts.append(f"newer_than:{GMAIL_QUERY_DAYS}d")

    if job_tags:
        tag_terms = " OR ".join(f"\"{tag}\"" for tag in job_tags)
        query_parts.append(f"({tag_terms})")

    return " ".join(query_parts)


def get_emails_by_label(service, label_id, max_results=MAX_EMAILS, query: str = ""):
    """
    Fetch emails from a specific Gmail label with optional Gmail query filtering.

    Args:
        service: Authenticated Gmail service
        label_id: Gmail label ID
        max_results: Maximum number of emails to fetch
        query: Optional Gmail search query string

    Returns:
        list: List of email message objects
    """
    try:
        list_kwargs = {
            "userId": "me",
            "labelIds": [label_id],
            "maxResults": max_results,
        }
        if query:
            list_kwargs["q"] = query

        results = service.users().messages().list(**list_kwargs).execute()
        messages = results.get("messages", [])

        if not messages:
            print("No messages found with this label/query.")
            return []

        email_list = []
        for msg in messages:
            message = service.users().messages().get(
                userId="me",
                id=msg["id"],
                format="full",
            ).execute()
            email_list.append(message)

        return email_list

    except Exception as e:
        print(f"Error fetching emails: {e}")
        return []


def extract_email_content(message) -> Dict[str, str]:
    """
    Extract subject, sender, date, and body content from Gmail message.

    Args:
        message: Gmail API message object

    Returns:
        dict: Email content fields
    """
    headers = message["payload"]["headers"]

    subject = next((h["value"] for h in headers if h["name"] == "Subject"), "No Subject")
    sender = next((h["value"] for h in headers if h["name"] == "From"), "Unknown")
    date = next((h["value"] for h in headers if h["name"] == "Date"), "")

    body_text = ""
    body_html = ""

    def parse_parts(parts):
        nonlocal body_text, body_html
        for part in parts:
            mime_type = part.get("mimeType", "")
            data = part.get("body", {}).get("data", "")

            if "parts" in part:
                parse_parts(part["parts"])

            if data:
                decoded = base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
                if mime_type == "text/plain":
                    body_text += decoded
                elif mime_type == "text/html":
                    body_html += decoded

    if "parts" in message["payload"]:
        parse_parts(message["payload"]["parts"])
    else:
        data = message["payload"].get("body", {}).get("data", "")
        if data:
            decoded = base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
            mime_type = message["payload"].get("mimeType", "")
            if mime_type == "text/plain":
                body_text = decoded
            elif mime_type == "text/html":
                body_html = decoded

    return {
        "subject": subject,
        "from": sender,
        "date": date,
        "body_text": body_text,
        "body_html": body_html,
        "message_id": message["id"],
    }


def extract_links(email_content: Dict[str, str]) -> List[str]:
    """
    Extract URLs from email content (all hosts).

    Args:
        email_content: Dictionary with email content

    Returns:
        list: Unique URLs
    """
    urls = set()

    if email_content["body_html"]:
        soup = BeautifulSoup(email_content["body_html"], "html.parser")
        for link in soup.find_all("a", href=True):
            href = link["href"].strip()
            if href.startswith("http"):
                urls.add(href)

    text = email_content["body_text"] or email_content["body_html"]
    if text:
        url_pattern = r"https?://[^\s<>\"{}|\\^`\[\]]+"
        urls.update(re.findall(url_pattern, text))

    cleaned_urls = []
    for url in urls:
        cleaned_url = url.rstrip(".,;:)")
        if cleaned_url:
            cleaned_urls.append(cleaned_url)

    return list(set(cleaned_urls))


def match_job_tags(email_content: Dict[str, str], job_tags: Optional[List[str]]) -> List[str]:
    """
    Match configured job tags against email subject/body.

    Args:
        email_content: Parsed email data
        job_tags: Tags to search for

    Returns:
        list: Matched tags in lowercase canonical form
    """
    if not job_tags:
        return []

    searchable_text = " ".join(
        [
            email_content.get("subject", ""),
            email_content.get("body_text", ""),
            BeautifulSoup(email_content.get("body_html", ""), "html.parser").get_text(" "),
        ]
    ).lower()

    matched = []
    for tag in job_tags:
        pattern = r"\b" + re.escape(tag.lower()) + r"\b"
        if re.search(pattern, searchable_text):
            matched.append(tag)

    return sorted(set(matched))


def normalize_linkedin_job_url(url: str) -> Optional[Dict[str, Optional[str]]]:
    """
    Normalize LinkedIn job links and extract a stable job ID when present.

    Args:
        url: Raw URL extracted from email

    Returns:
        dict|None: URL details with canonical URL and job_id, or None for non-LinkedIn links
    """
    cleaned = url.strip().rstrip(".,;:)")
    parsed = urlparse(cleaned)
    host = parsed.netloc.lower()

    if not any(host == valid or host.endswith(f".{valid}") for valid in LINKEDIN_HOSTS):
        return None

    path = parsed.path or ""
    query_params = parse_qs(parsed.query)

    job_id_match = re.search(r"/jobs/view/(\d+)", path)
    job_id = job_id_match.group(1) if job_id_match else None

    if not job_id:
        for key in ("currentJobId", "jobId"):
            if query_params.get(key):
                candidate = query_params[key][0]
                if candidate and candidate.isdigit():
                    job_id = candidate
                    break

    # Only keep direct job posting links with a stable LinkedIn job ID.
    # This avoids scraping /comm, /feed, unsubscribe, and other non-job URLs.
    if not job_id:
        return None

    canonical_url = f"https://www.linkedin.com/jobs/view/{job_id}/"

    return {
        "raw_url": cleaned,
        "canonical_url": canonical_url,
        "job_id": job_id,
    }


def extract_linkedin_job_links(email_content: Dict[str, str]) -> List[Dict[str, Optional[str]]]:
    """
    Extract and normalize LinkedIn job links from a single email.

    Args:
        email_content: Parsed email content

    Returns:
        list: Unique link records with raw_url, canonical_url, and job_id
    """
    records = []
    seen_keys = set()

    for url in extract_links(email_content):
        normalized = normalize_linkedin_job_url(url)
        if not normalized:
            continue

        key = normalized["job_id"] or normalized["canonical_url"]
        if key in seen_keys:
            continue
        seen_keys.add(key)
        records.append(normalized)

    return records
