# Automated Job Search Pipeline

An end-to-end system that discovers job postings, evaluates fit against your career preferences, generates tailored resumes, finds outreach contacts, writes personalized messages, and creates Gmail drafts -- with minimal manual intervention.

## How It Works

```
Gmail alerts / Daily search reports / Manual URLs
    |
    v
Scrape job postings (crawl4ai + GPT-4o)
    |
    v
Evaluate fit against career_preferences.md
    |
    +-- Green (score >= 70): full pipeline
    |     - Find outreach contacts (Serper)
    |     - Discover emails (Hunter.io)
    |     - Generate personalized messages
    |     - Create Gmail drafts
    |
    +-- Yellow (score 40-69): resume only
    |     - Generate tailored resume PDF
    |     - Generate resume tailoring suggestions
    |
    +-- Red (score < 40): logged, skipped
    |
    v
Upload everything to Google Sheets
Log pipeline run stats
```

## Two Directories, One System

This repo (`automate_job_search/`) is the automation engine. It works together with a **workspace directory** that holds career documents, the job registry, and per-job output folders.

| Directory | Role | Contents |
|-----------|------|----------|
| `automate_job_search/` | Engine | All Python scripts, config, credentials, pipeline output |
| `Job Search/` (workspace) | Human-facing workspace | Career preferences, master experience doc, `seen_jobs.json` registry, per-job folders with tailored resumes |

The workspace path is configured via `WORKSPACE_DIR` in `.env`. All per-job folders (resumes, job info) are created there. All code runs from here.

---

## Prerequisites

1. **Python 3.11** (a `.venv311` virtualenv is used)
2. **API Keys:**
   - [OpenAI](https://platform.openai.com/api-keys) -- job scraping, fit refinement, message generation
   - [Serper](https://serper.dev/) -- contact discovery via Google search
   - [Hunter.io](https://hunter.io/) -- email address discovery (25 free/month)
3. **Google Cloud Project** with Gmail API and Google Sheets API enabled
4. **OAuth credentials** (`credentials.json`) for Gmail read + compose + Sheets access

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
crawl4ai-setup    # installs Playwright browsers for web scraping
```

### 2. Configure Gmail API

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project (or select existing)
3. Enable **Gmail API** and **Google Sheets API** under APIs & Services > Library
4. Create OAuth credentials (Desktop app) under APIs & Services > Credentials
5. Download the JSON file, rename to `credentials.json`, place in this directory

### 3. Set up environment variables

```bash
cp .env.example .env
```

Edit `.env` with your keys and paths. The required variables:

```bash
# API keys
OPENAI_API_KEY=sk-...
SERPER_DEV_API_KEY=...
HUNTER_IO_API_KEY=...

# Gmail
GMAIL_LABEL=LinkedIn Jobs
GMAIL_INTERACTIVE_AUTH=0          # 0 for cron, 1 for first-time browser auth
CREDENTIALS_FILE=credentials.json

# Workspace (where career docs and job folders live)
WORKSPACE_DIR=/path/to/Job Search
CAREER_PREFERENCES_PATH=/path/to/Job Search/career_preferences.md
MASTER_EXPERIENCE_PATH=/path/to/Job Search/kanu_madhok_master_experience.md

# Fit evaluation thresholds
FIT_MIN_SCORE_FOR_OUTREACH=70     # Only find contacts for green-tier jobs
FIT_MIN_SCORE_FOR_RESUME=40       # Generate resumes for green + yellow jobs
FIT_USE_LLM_FOR_YELLOW=true       # Use GPT-4o to refine ambiguous scores

# Safety
MAX_DRAFTS_PER_RUN=10

# Google Sheets (for the full pipeline)
COMPLETE_PIPELINE_SHEET=https://docs.google.com/spreadsheets/d/<id>/edit
```

See `.env.example` for the full list of optional variables.

### 4. First-time authentication

Run once interactively to create OAuth tokens:

```bash
GMAIL_INTERACTIVE_AUTH=1 python main.py
```

A browser window will open for Gmail authorization. After that, `token.json` and `token_sheets.pickle` are cached locally for unattended runs.

---

## Usage

### Run the full pipeline (recommended)

One command that does everything:

```bash
python run_complete_career_pipeline.py \
  --trigger "run complete package" \
  --sheet "https://docs.google.com/spreadsheets/d/<id>/edit" \
  --auto-discover-reports
```

This will:
1. Check Gmail for new job alert emails
2. Scan the workspace for unprocessed daily search reports (`*_new_jobs.md`)
3. Deduplicate against the unified registry (`seen_jobs.json`)
4. Scrape new job postings
5. Evaluate fit against `career_preferences.md`
6. For green + yellow jobs: create per-job folders and tailored resume PDFs
7. For green jobs: find contacts, generate messages, create Gmail drafts
8. Upload everything to Google Sheets
9. Log the pipeline run

To skip Gmail and use an existing jobs file:

```bash
python run_complete_career_pipeline.py \
  --trigger "run complete package" \
  --sheet "https://docs.google.com/spreadsheets/d/<id>/edit" \
  --skip-job-discovery \
  --jobs-json output/jobs_YYYYMMDD_HHMMSS.json
```

To ingest specific daily reports:

```bash
python run_complete_career_pipeline.py \
  --trigger "run complete package" \
  --sheet "https://docs.google.com/spreadsheets/d/<id>/edit" \
  --daily-reports "/path/to/Job Search/daily_searches/2026-03-30_new_jobs.md"
```

### Run individual steps

Each step can be run standalone:

**Scrape jobs from Gmail:**
```bash
python main.py
```

**Evaluate fit on existing jobs:**
```bash
python evaluate_job_fit.py \
  --jobs-json output/jobs_YYYYMMDD_HHMMSS.json \
  --no-llm \
  --min-score 40
```

**Parse a daily search report:**
```bash
python parse_daily_report.py \
  "/path/to/Job Search/daily_searches/2026-03-30_new_jobs.md"
```

**Find outreach contacts:**
```bash
python find_outreach_contacts.py \
  --json output/jobs_YYYYMMDD_HHMMSS.json \
  --fetch \
  --per-role 3
```

**Generate outreach messages (with LLM personalization):**
```bash
python generate_outreach_messages.py \
  --contacts-json output/outreach_targets_YYYYMMDD_HHMMSS.json \
  --jobs-json output/jobs_evaluated_YYYYMMDD_HHMMSS.json \
  --use-llm
```

**Generate tailored resume PDF:**
```bash
python generate_tailored_resume.py \
  --company "Scale AI" \
  --title "Applied AI Engineer" \
  --keywords "RAG,agentic,LLM,production,evaluation" \
  --output "Scale_AI_Resume.pdf"
```

**Generate resume tailoring suggestions:**
```bash
python generate_resume_tailoring.py \
  --jobs-json output/jobs_YYYYMMDD_HHMMSS.json \
  --resume-pdf /path/to/resume.pdf
```

**Create Gmail drafts:**
```bash
python create_gmail_drafts.py \
  --messages-json output/outreach_messages_YYYYMMDD_HHMMSS.json \
  --dry-run    # preview without creating drafts
```

**Find networking contacts:**
```bash
python find_networking_contacts.py \
  --fetch \
  --internal-companies "Walmart,Walmart Global Tech" \
  --external-companies "Microsoft,Amazon,Meta,OpenAI" \
  --title-terms "Data Scientist,AI Engineer,ML Engineer" \
  --keywords "agents,llm,applied ai,production ai"
```

**Run batch pipeline from a URL list:**
```bash
python run_url_batch_pipeline.py \
  --urls job_urls.txt \
  --sheet "https://docs.google.com/spreadsheets/d/<id>/edit"
```

### Daily cron (automated)

The shell wrapper handles locking, logging, and non-interactive auth:

```bash
./run_complete_career_daily.sh
```

Cron entry:
```cron
CRON_TZ=America/Chicago
0 8 * * * cd /path/to/automate_job_search && ./run_complete_career_daily.sh
```

This runs the full pipeline with `--auto-discover-reports` enabled, so it picks up any new daily search markdown files automatically.

---

## Fit Evaluation

The fit evaluator (`evaluate_job_fit.py`) scores each job 0-100 using deterministic rules parsed from `career_preferences.md`:

| Step | What It Checks |
|------|---------------|
| Title check | Matches against preferred/excluded title lists |
| Keyword scan | Green flags (RAG, agentic, copilot...) vs red flags (dashboarding, BI, ETL-only...) |
| Company tier | Tier 1 (+15), Tier 2 (+10), Tier 3 (+5) from your company lists |
| Location | Chicago, SF/Bay Area, NYC, Seattle, Remote |
| LLM refinement | Optional GPT-4o pass for yellow-tier jobs that score ambiguously |

Scoring starts at 50. Green flags add +10 each (capped at +40), red flags subtract -20 each, yellow flags add +5 each. Final tier: green >= 70, yellow 40-69, red < 40.

Each evaluated job gets:
```json
{
  "fit_score": 85,
  "fit_tier": "green",
  "company_tier": 2,
  "fit_signals": {
    "green_flags": ["mentions RAG", "mentions agentic systems"],
    "yellow_flags": ["startup indicator"],
    "red_flags": []
  },
  "fit_summary": "Strong fit -- Applied AI Engineer role building RAG systems...",
  "recommended_action": "apply",
  "keywords_for_resume": ["RAG", "agentic", "LLM", "production"]
}
```

---

## Workspace Structure

The workspace (`WORKSPACE_DIR`) is where human-facing output lives:

```
Job Search/
├── career_preferences.md              # Your target roles, companies, keywords, locations
├── kanu_madhok_master_experience.md   # Full experience doc for resume tailoring
├── daily_searches/
│   ├── seen_jobs.json                 # Unified job registry (single source of truth)
│   ├── 2026-03-30_new_jobs.md         # Daily search reports (parsed by pipeline)
│   ├── 2026-03-29_new_jobs.md
│   └── jobs/                          # Per-job folders
│       ├── scale-ai-applied-ai-engineer/
│       │   ├── job_info.md            # Company, title, fit score, notes
│       │   └── Kanu_Madhok_Resume_Scale_AI.pdf
│       ├── palantir-forward-deployed-ai-engineer/
│       │   ├── job_info.md
│       │   └── Kanu_Madhok_Resume_Palantir.pdf
│       └── ...
└── weekly_digests/
    └── 2026-03-29_weekly_digest.md
```

**`seen_jobs.json`** is the unified dedup registry. Every job from every source (Gmail, daily reports, manual URLs) is registered here by company+title slug and LinkedIn job ID. This prevents duplicate processing across runs and sources.

**Daily search reports** (`*_new_jobs.md`) are markdown files with job tables you create from manual searching. The pipeline parses these automatically when `--auto-discover-reports` is set.

**Per-job folders** are created automatically by `job_manager.py` when a job is registered. Each folder gets a `job_info.md` with metadata and a keyword-tailored resume PDF.

---

## Project Structure

```
automate_job_search/
│
├── Pipeline orchestration
│   ├── run_complete_career_pipeline.py   # Full end-to-end pipeline
│   ├── run_complete_career_daily.sh      # Cron wrapper (locking, logging, env)
│   ├── run_job_contacts_pipeline.py      # Contacts-only pipeline
│   ├── run_url_batch_pipeline.py         # Process a list of URLs
│   └── run_openclaw_daily.sh             # Legacy Gmail-only cron wrapper
│
├── Job discovery
│   ├── main.py                           # Gmail job alert ingestion
│   ├── email_processor.py                # Gmail email fetching + parsing
│   ├── extract_links.py                  # LinkedIn URL extraction from emails
│   ├── load_job_urls.py                  # URL loading from files
│   ├── job_scraper.py                    # crawl4ai + GPT-4o job scraping
│   ├── scrape_jobs.py                    # Batch scraping orchestrator
│   └── parse_daily_report.py             # Extract jobs from markdown reports
│
├── Fit evaluation
│   └── evaluate_job_fit.py               # Score jobs against career preferences
│
├── Job management + resume
│   ├── job_manager.py                    # Unified registry, folder creation, dedup
│   ├── generate_tailored_resume.py       # Keyword-reordered PDF resume generation
│   └── generate_resume_tailoring.py      # LLM-powered tailoring suggestions
│
├── Contact discovery
│   ├── find_outreach_contacts.py         # Find managers/recruiters/team via Serper
│   ├── find_networking_contacts.py       # Find networking contacts (internal/external)
│   └── find_contact_emails.py            # Email discovery via Hunter.io + patterns
│
├── Message generation
│   ├── generate_outreach_messages.py     # Context-aware outreach (uses fit signals)
│   └── generate_networking_messages.py   # Networking message templates
│
├── Gmail integration
│   ├── gmail_auth.py                     # OAuth2 authentication
│   └── create_gmail_drafts.py            # Create drafts (never auto-sends)
│
├── Google Sheets upload
│   ├── upload_jobs_to_sheets.py          # Jobs + fit scores
│   ├── upload_contacts_to_sheets.py      # Outreach contacts
│   ├── upload_messages_to_sheets.py      # Outreach messages
│   ├── upload_drafts_to_sheets.py        # Draft metadata
│   ├── upload_resume_tailoring_to_sheets.py
│   ├── upload_networking_contacts_to_sheets.py
│   ├── upload_networking_messages_to_sheets.py
│   └── upload_pipeline_run_to_sheets.py  # Pipeline run stats
│
├── Utilities
│   ├── migrate_registry.py              # One-time migration of old dedup data
│   └── config.py                        # All settings (env-var backed)
│
├── Credentials (not committed)
│   ├── .env                             # API keys + config (from .env.example)
│   ├── credentials.json                 # Google OAuth client credentials
│   ├── token.json                       # Cached Gmail token
│   └── token_sheets.pickle              # Cached Sheets token
│
├── output/                              # Pipeline output (dated files)
│   ├── jobs_*.json/csv                  # Scraped job data
│   ├── jobs_evaluated_*.json            # Jobs with fit scores
│   ├── outreach_targets_*.json/csv      # Contact discovery results
│   ├── outreach_messages_*.json/csv     # Generated messages
│   ├── networking_targets_*.json/csv    # Networking contacts
│   ├── networking_messages_*.json/csv   # Networking messages
│   ├── resume_tailoring_*.json/csv      # Tailoring suggestions
│   ├── processed_job_ids.json           # Legacy dedup (read-only fallback)
│   ├── processed_reports.json           # Tracks ingested daily reports
│   ├── processed_message_ids.json       # Tracks Gmail drafts created
│   └── logs/                            # Per-run log files
│
├── requirements.txt
├── .env.example
└── README.md
```

---

## Google Sheets Structure

The pipeline uploads to these tabs:

| Tab | Contents |
|-----|----------|
| `jobs` | All discovered jobs with fit scores, tier, flags, summary |
| `job_contacts` | Outreach contacts (manager, recruiter, team member per job) |
| `job_messages` | Generated outreach messages with personalization context |
| `gmail_drafts` | Draft metadata (subject, recipient, creation time) |
| `resume_tailoring` | Per-job ATS keywords, bullet rewrites, tailoring tips |
| `networking_contacts` | Internal + external networking contacts |
| `networking_messages` | Networking message drafts |
| `pipeline_runs` | Run timestamp, source, job counts by tier, errors |

---

## Configuration Reference

All settings live in `config.py` and are overridable via environment variables in `.env`:

| Setting | Default | Description |
|---------|---------|-------------|
| `WORKSPACE_DIR` | (required) | Path to the Job Search workspace |
| `CAREER_PREFERENCES_PATH` | `WORKSPACE_DIR/career_preferences.md` | Career preferences for fit evaluation |
| `MASTER_EXPERIENCE_PATH` | `WORKSPACE_DIR/kanu_madhok_master_experience.md` | Full experience doc for resume tailoring |
| `FIT_MIN_SCORE_FOR_OUTREACH` | `70` | Minimum fit score to trigger contact discovery |
| `FIT_MIN_SCORE_FOR_RESUME` | `40` | Minimum fit score to generate a tailored resume |
| `FIT_USE_LLM_FOR_YELLOW` | `true` | Use GPT-4o to refine yellow-tier evaluations |
| `INCLUDE_ADVANCED_AI_BULLETS` | `true` | Include MCP/agent/knowledge graph bullets for AI roles |
| `MAX_DRAFTS_PER_RUN` | `10` | Cap on Gmail drafts created per pipeline run |
| `GMAIL_LABEL` | `LinkedIn Jobs` | Gmail label to scan for job alerts |
| `MAX_EMAILS` | `50` | Emails to process per run |
| `MAX_JOB_URLS` | `50` | Hard cap on URLs scraped per run |
| `GMAIL_QUERY_DAYS` | `14` | How far back to look for emails |
| `GMAIL_INTERACTIVE_AUTH` | `true` | Set to `0` for cron / unattended runs |
| `OPENAI_MODEL` | `gpt-4o` | Model for scraping and message generation |

---

## Safety Rails

- **Draft-only:** Gmail drafts are created, never sent automatically. You review and click send.
- **Max drafts per run:** Capped at 10 by default (`MAX_DRAFTS_PER_RUN`).
- **Dry run mode:** `create_gmail_drafts.py --dry-run` previews without creating anything.
- **Lock file:** `run_complete_career_daily.sh` uses a lock directory with a 12-hour TTL to prevent concurrent runs.
- **Isolated error handling:** If one job fails at any step, the pipeline continues with the next.
- **Dedup:** The unified registry prevents reprocessing the same job across runs and sources.

---

## Cost Estimates

Per pipeline run processing ~50 jobs:

| Service | Usage | Approximate Cost |
|---------|-------|-----------------|
| OpenAI GPT-4o | Scraping + fit refinement + messages | $0.50 - $2.50 |
| Serper | Contact discovery searches | $0.01 - $0.10 |
| Hunter.io | Email lookups (25 free/month) | Free tier usually sufficient |

---

## Troubleshooting

**"credentials.json not found"** -- Download OAuth credentials from Google Cloud Console, place in project root.

**"Label not found"** -- Check `GMAIL_LABEL` matches your Gmail label exactly (case-sensitive).

**"OPENAI_API_KEY not found"** -- Create `.env` from `.env.example` and add your key.

**"No module named 'reportlab'"** -- Install it: `.venv311/bin/pip install reportlab==4.0.4`

**Scraping failures** -- Some sites block automated scraping. Increase `SCRAPE_TIMEOUT` in config or check if the site requires login.

**Token expired for cron** -- Delete `token.json` and/or `token_sheets.pickle`, run once interactively with `GMAIL_INTERACTIVE_AUTH=1` to re-authorize, then switch back to `0`.

---

## License

MIT License
