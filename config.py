"""Configuration settings for the job email scraper."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def env_bool(name: str, default: bool = False) -> bool:
    """Parse a boolean environment variable."""
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def env_path(name: str, default: Path) -> Path:
    """Parse a path environment variable (supports relative paths)."""
    raw_value = os.getenv(name)
    if not raw_value:
        return default
    candidate = Path(raw_value)
    if candidate.is_absolute():
        return candidate
    return (Path(__file__).parent / candidate).resolve()


# Project paths
BASE_DIR = Path(__file__).parent
OUTPUT_DIR = env_path("OUTPUT_DIR", BASE_DIR / "output")
CREDENTIALS_FILE = env_path("CREDENTIALS_FILE", BASE_DIR / "credentials.json")
TOKEN_FILE = env_path("TOKEN_FILE", BASE_DIR / "token.json")
PROCESSED_MESSAGE_IDS_FILE = OUTPUT_DIR / "processed_message_ids.json"
PROCESSED_JOB_IDS_FILE = OUTPUT_DIR / "processed_job_ids.json"

# Gmail settings
GMAIL_LABEL = os.getenv("GMAIL_LABEL", "LinkedIn Jobs")
MAX_EMAILS = int(os.getenv("MAX_EMAILS", "50"))
MAX_JOB_URLS = int(os.getenv("MAX_JOB_URLS", "50"))
GMAIL_QUERY_DAYS = int(os.getenv("GMAIL_QUERY_DAYS", "14"))
GMAIL_QUERY = os.getenv("GMAIL_QUERY", "")
LINKEDIN_SENDERS = [
    sender.strip() for sender in os.getenv(
        "LINKEDIN_SENDERS",
        "jobs-noreply@linkedin.com,jobalerts-noreply@linkedin.com,notifications-noreply@linkedin.com"
    ).split(",")
    if sender.strip()
]
JOB_TAGS = [
    tag.strip() for tag in os.getenv("JOB_TAGS", "").split(",") if tag.strip()
]
GMAIL_INTERACTIVE_AUTH = env_bool("GMAIL_INTERACTIVE_AUTH", True)

# OpenAI settings
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
OPENAI_MODEL = "gpt-4o"  # or "gpt-4o-mini" for cheaper/faster option

# Scraping settings
SCRAPE_TIMEOUT = 30  # Seconds to wait for page loads
SCRAPE_DELAY = 2  # Seconds between scraping requests

# Output settings
SAVE_JSON = True
SAVE_CSV = True

# Email discovery settings (Hunter.io)
HUNTER_IO_API_KEY = os.getenv("HUNTER_IO_API_KEY", "")

# Gmail draft settings
GMAIL_DRAFT_ENABLED = env_bool("GMAIL_DRAFT_ENABLED", True)

# Default job URLs file
JOB_URLS_FILE = env_path("JOB_URLS_FILE", BASE_DIR / "job_urls.txt")

# Workspace paths (Job Search workspace for human-facing output)
WORKSPACE_DIR = env_path(
    "WORKSPACE_DIR",
    Path("/Users/kanumadhok/Documents/Claude/Projects/Job Search"),
)
WORKSPACE_DAILY_DIR = env_path(
    "WORKSPACE_DAILY_DIR",
    WORKSPACE_DIR / "daily_searches",
)
CAREER_PREFERENCES_PATH = env_path(
    "CAREER_PREFERENCES_PATH",
    WORKSPACE_DIR / "career_preferences.md",
)
MASTER_EXPERIENCE_PATH = env_path(
    "MASTER_EXPERIENCE_PATH",
    WORKSPACE_DIR / "kanu_madhok_master_experience.md",
)

# Fit evaluation settings (Phase 1)
FIT_MIN_SCORE_FOR_OUTREACH = int(os.getenv("FIT_MIN_SCORE_FOR_OUTREACH", "70"))
FIT_MIN_SCORE_FOR_RESUME = int(os.getenv("FIT_MIN_SCORE_FOR_RESUME", "40"))
FIT_USE_LLM_FOR_YELLOW = env_bool("FIT_USE_LLM_FOR_YELLOW", True)

# Resume tailoring settings (Phase 3)
INCLUDE_ADVANCED_AI_BULLETS = env_bool("INCLUDE_ADVANCED_AI_BULLETS", True)

# Safety rails
MAX_DRAFTS_PER_RUN = int(os.getenv("MAX_DRAFTS_PER_RUN", "10"))

# Create output directory if it doesn't exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
