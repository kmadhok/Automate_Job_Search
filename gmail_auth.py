"""Gmail API authentication and service creation."""

import os.path
import pickle
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from config import CREDENTIALS_FILE, GMAIL_INTERACTIVE_AUTH, TOKEN_FILE

# Gmail API scopes - we only need read access
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


def get_gmail_service():
    """
    Authenticate and return Gmail API service.

    On first run, this will open a browser for OAuth2 authentication.
    Subsequent runs will use the saved token.

    Returns:
        googleapiclient.discovery.Resource: Authenticated Gmail service
    """
    creds = None

    credentials_path = str(CREDENTIALS_FILE)
    token_path = str(TOKEN_FILE)

    # Check if we have a saved token
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    # If no valid credentials, authenticate
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception:
                creds = None

        if not creds or not creds.valid:
            if not os.path.exists(credentials_path):
                raise FileNotFoundError(
                    f"credentials.json not found at {credentials_path}. "
                    "Please download it from Google Cloud Console."
                )

            if not GMAIL_INTERACTIVE_AUTH:
                raise RuntimeError(
                    "No valid Gmail token available and GMAIL_INTERACTIVE_AUTH=0. "
                    "Delete token.json and re-authenticate interactively once before cron runs."
                )

            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_path, SCOPES
            )
            creds = flow.run_local_server(port=0)

        # Save credentials for next run
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)

    # Build and return Gmail service
    service = build('gmail', 'v1', credentials=creds)
    return service


def get_label_id(service, label_name):
    """
    Get the ID of a Gmail label by name.

    Args:
        service: Authenticated Gmail service
        label_name: Name of the label (e.g., "Jobs")

    Returns:
        str: Label ID or None if not found
    """
    try:
        results = service.users().labels().list(userId='me').execute()
        labels = results.get('labels', [])

        for label in labels:
            if label['name'].lower() == label_name.lower():
                return label['id']

        return None
    except Exception as e:
        print(f"Error getting label ID: {e}")
        return None
