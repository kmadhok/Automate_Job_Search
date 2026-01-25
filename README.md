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

1. **Python 3.8+**
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

## Configuration

Edit `config.py` to customize:

```python
# Gmail settings
GMAIL_LABEL = "Jobs"           # Your Gmail label name
MAX_EMAILS = 50                 # Max emails to process per run

# OpenAI settings
OPENAI_MODEL = "gpt-4o"        # or "gpt-4o-mini" for cheaper option

# Scraping settings
SCRAPE_TIMEOUT = 30            # Seconds to wait for page loads
SCRAPE_DELAY = 2               # Seconds between requests (rate limiting)

# Output settings
SAVE_JSON = True
SAVE_CSV = True
```

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
