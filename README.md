# Automated Job Search Email Processor

Automatically extract job postings from your Gmail, scrape the job descriptions using LLM-powered extraction, and save them to structured formats (JSON/CSV).

## Features

- 📧 **Gmail Integration**: Fetch emails from specific labels
- 🔗 **Link Extraction**: Automatically extract job posting URLs from emails
- 🤖 **LLM-Powered Scraping**: Uses OpenAI GPT-4 to intelligently extract job information
- 📊 **Structured Output**: Saves to JSON and CSV formats
- ⚡ **Async Scraping**: Fast, concurrent web scraping with crawl4ai
- 🎯 **Smart Extraction**: Extracts title, company, location, salary, requirements, and more

## Prerequisites

1. **Python 3.11 recommended**
2. **OpenAI API Key** - Get one at [platform.openai.com](https://platform.openai.com/api-keys)
3. **Google Cloud Project with Gmail API enabled**

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

After installing, run the crawl4ai setup:

```bash
crawl4ai-setup
```

This will install Playwright browsers needed for web scraping.

### 2. Set Up Gmail API

#### Enable Gmail API:
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select existing)
3. Enable the Gmail API:
   - Go to "APIs & Services" > "Library"
   - Search for "Gmail API"
   - Click "Enable"

#### Create OAuth Credentials:
1. Go to "APIs & Services" > "Credentials"
2. Click "Create Credentials" > "OAuth client ID"
3. Choose "Desktop app" as application type
4. Download the credentials JSON file
5. Rename it to `credentials.json` and place it in this directory

### 3. Configure Environment Variables

```bash
cp .env.example .env
```

Edit `.env` and add your OpenAI API key:

```
OPENAI_API_KEY=sk-your-actual-api-key-here
```

### 4. Create a Gmail Label

1. In Gmail, create a label (e.g., "Jobs")
2. Add job-related emails to this label
3. Update `config.py` if you named your label differently:

```python
GMAIL_LABEL = "Your-Label-Name"
```

### 5. Run the Script

```bash
python main.py
```

On first run, a browser window will open asking you to authorize the app to access your Gmail (read-only).

For unattended environments (OpenClaw/cron), set:

```bash
GMAIL_INTERACTIVE_AUTH=0
```

This disables browser OAuth and requires a valid `token.json` already present.

### 6. Generate Outreach Targets (Managers, Recruiters, Team Members)

Build per-job people-search queries automatically:

```bash
python find_outreach_contacts.py --json output/jobs_YYYYMMDD_HHMMSS.json
```

Auto-fetch likely people from search results (requires Serper API key):

```bash
SERPER_DEV_API_KEY=your-key-here python find_outreach_contacts.py \
  --json output/jobs_YYYYMMDD_HHMMSS.json \
  --fetch \
  --per-role 3
```

This writes:
- `output/outreach_targets_*.json` (job-level team guess + queries + candidates)
- `output/outreach_targets_*.csv` (flat list for tracking/outreach workflows)

Run the full contact pipeline (trigger statement + fetch + upload to Google Sheets):

```bash
python run_job_contacts_pipeline.py \
  --trigger "run job contacts" \
  --jobs-json output/jobs_YYYYMMDD_HHMMSS.json \
  --sheet "https://docs.google.com/spreadsheets/d/<id>/edit" \
  --tab job_contacts \
  --messages-tab job_messages \
  --per-role 3 \
  --mode replace
```

Message generation is role-specific:
- `recruiter`: fit + process-forward ask
- `manager`: impact-forward intro + short conversation ask
- `team_member`: peer-level curiosity + quick guidance ask

For profile personalization, you can either:
- set `OUTREACH_PROFILE_SUMMARY` in `.env`, or
- set `OUTREACH_RESUME_PDF` in `.env` (or pass `--resume-pdf`) to auto-derive the summary from your resume PDF.

Outputs added by pipeline:
- `output/outreach_messages_*.json`
- `output/outreach_messages_*.csv`

### 7. Discover Networking Contacts (Internal + External)

Find people on LinkedIn who are working on applied AI problems, both inside Walmart and outside.

Query-only mode (build search queries without fetching people):

```bash
python find_networking_contacts.py \
  --internal-companies "Walmart,Walmart Global Tech" \
  --title-terms "Data Scientist,AI Engineer,Machine Learning Engineer,Data Analyst" \
  --keywords "agents,large language models,llm,applied ai,production ai" \
  --focus-terms "customer impact,supply chain,automation,recommendation systems"
```

Fetch candidate profiles from Serper (recommended):

```bash
SERPER_DEV_API_KEY=your-key-here python find_networking_contacts.py \
  --fetch \
  --internal-companies "Walmart,Walmart Global Tech" \
  --external-companies "Microsoft,Amazon,Meta,OpenAI,NVIDIA" \
  --title-terms "Data Scientist,AI Engineer,Machine Learning Engineer,Data Analyst" \
  --keywords "agents,large language models,llm,applied ai,production ai" \
  --focus-terms "customer impact,supply chain,automation,recommendation systems" \
  --per-query 10
```

This writes:
- `output/networking_targets_*.json` (queries + grouped candidates by internal/external scope)
- `output/networking_targets_*.csv` (flat rows for tracking and outreach)

Generate networking message drafts from those contacts:

```bash
python generate_networking_messages.py \
  --json output/networking_targets_YYYYMMDD_HHMMSS.json \
  --profile-summary "I work on applied AI products and enjoy learning from practitioners."
```

This writes:
- `output/networking_messages_*.json`
- `output/networking_messages_*.csv`

### 8. Tailor Resume Language for Each Job

Generate per-job resume tailoring guidance (ATS keywords, language to mirror, bullet rewrites):

```bash
python generate_resume_tailoring.py \
  --jobs-json output/jobs_YYYYMMDD_HHMMSS.json \
  --resume-pdf /absolute/path/to/resume.pdf
```

This writes:
- `output/resume_tailoring_*.json`
- `output/resume_tailoring_*.csv`

### 9. Run Complete Package (End-to-End)

One command to run:
- job discovery from Gmail/LinkedIn emails
- job upload to Sheets
- outreach contact discovery + upload
- outreach message generation + upload
- resume tailoring + upload
- networking contact discovery + upload
- networking message generation + upload

```bash
set -a && source .env && set +a && python run_complete_career_pipeline.py \
  --trigger "run complete package" \
  --sheet "https://docs.google.com/spreadsheets/d/<id>/edit" \
  --jobs-tab jobs \
  --job-contacts-tab job_contacts \
  --job-messages-tab job_messages \
  --resume-tab resume_tailoring \
  --networking-contacts-tab networking_contacts \
  --networking-messages-tab networking_messages
```

If you already have a recent jobs JSON and want to skip Gmail scraping:

```bash
set -a && source .env && set +a && python run_complete_career_pipeline.py \
  --trigger "run complete package" \
  --sheet "https://docs.google.com/spreadsheets/d/<id>/edit" \
  --skip-job-discovery \
  --jobs-json output/jobs_YYYYMMDD_HHMMSS.json
```

## Configuration

Edit `config.py` to customize:

```python
# Gmail settings
GMAIL_LABEL = "Jobs"           # Your Gmail label name
MAX_EMAILS = 5                  # Emails to process per run
MAX_JOB_URLS = 25               # Hard cap on URLs scraped per run
GMAIL_INTERACTIVE_AUTH = False  # Disable browser auth for cron jobs

# OpenAI settings
OPENAI_MODEL = "gpt-4o"        # or "gpt-4o-mini" for cheaper option

# Scraping settings
SCRAPE_TIMEOUT = 30            # Seconds to wait for page loads
SCRAPE_DELAY = 2               # Seconds between requests (rate limiting)

# Output settings
SAVE_JSON = True
SAVE_CSV = True
```

## OpenClaw Cron Setup

Use the included runner script:

```bash
./run_openclaw_daily.sh
```

What it handles:
- loads `.env`
- defaults to `MAX_EMAILS=5`
- enforces single-run lock (prevents overlapping cron runs)
- writes per-run logs to `output/logs/`
- exits non-zero on real failures

### Example cron entry

```cron
0 8 * * * cd /path/to/automate_job_search && ./run_openclaw_daily.sh
```

If your cron host supports `CRON_TZ`, set your preferred timezone explicitly.

## Full Pipeline Daily Cron Setup

Use the full-pipeline wrapper for unattended daily runs:

```bash
./run_complete_career_daily.sh
```

Before scheduling it, add these to `.env`:

```bash
COMPLETE_PIPELINE_SHEET=https://docs.google.com/spreadsheets/d/<id>/edit
SERPER_DEV_API_KEY=your-serper-api-key-here
GMAIL_INTERACTIVE_AUTH=0
```

What it handles:
- loads `.env`
- runs the end-to-end pipeline with job discovery included
- passes `--non-interactive-sheets` by default for cron safety
- enforces a single-run lock
- writes per-run logs to `output/logs/`
- exits non-zero on real failures

### Example daily cron entry

```cron
CRON_TZ=America/Chicago
15 9 * * * cd /path/to/automate_job_search && ./run_complete_career_daily.sh
```

Make sure both Gmail (`token.json`) and Google Sheets (`token_sheets.pickle`) tokens have already been created successfully before relying on cron.

## Output

Results are saved in the `output/` directory:

- **JSON**: `jobs_YYYYMMDD_HHMMSS.json` - Complete data with metadata
- **CSV**: `jobs_YYYYMMDD_HHMMSS.csv` - Flattened table format

### Extracted Job Fields

- Title
- Company
- Location
- Employment Type (Full-time, Contract, etc.)
- Salary Range
- Description
- Requirements
- Responsibilities
- Benefits
- Application Deadline

## Project Structure

```
automate_job_search/
├── main.py                 # Main orchestration script
├── config.py              # Configuration settings
├── gmail_auth.py          # Gmail API authentication
├── email_processor.py     # Email fetching and link extraction
├── job_scraper.py         # LLM-powered web scraping
├── requirements.txt       # Python dependencies
├── .env                   # Environment variables (create from .env.example)
├── credentials.json       # Gmail OAuth credentials (download from Google)
├── token.json            # Auto-generated auth token
└── output/               # Output directory for results
```

## How It Works

1. **Authenticate**: Connects to Gmail using OAuth2
2. **Fetch Emails**: Retrieves emails from your specified label
3. **Extract Links**: Parses email HTML/text to find job posting URLs
4. **Scrape Jobs**: Uses crawl4ai + OpenAI to intelligently extract job information
5. **Save Results**: Exports to JSON and CSV files

## Troubleshooting

### "credentials.json not found"
- Download OAuth credentials from Google Cloud Console
- Place in project root directory

### "Label not found"
- Check label name matches exactly (case-sensitive)
- Update `GMAIL_LABEL` in `config.py`

### "OPENAI_API_KEY not found"
- Create `.env` file from `.env.example`
- Add your OpenAI API key

### Scraping failures
- Some websites block automated scraping
- Increase `SCRAPE_TIMEOUT` in `config.py`
- Check if site requires login or has anti-bot measures

## Cost Estimates

**OpenAI API costs** (approximate):
- GPT-4o: ~$0.01-0.05 per job posting
- GPT-4o-mini: ~$0.001-0.005 per job posting

50 job postings with GPT-4o ≈ $0.50-$2.50

## Privacy & Security

- Gmail access is **read-only**
- Credentials stored locally in `token.json`
- No data sent anywhere except OpenAI for job extraction
- Add `.env` and `token.json` to `.gitignore` if using version control

## License

MIT License - feel free to modify and use for your job search!

## Contributing

Found a bug or have a feature request? Open an issue or submit a pull request!
