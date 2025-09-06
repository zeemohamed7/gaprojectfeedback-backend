# backend/config.py
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
CLIENT_SECRETS_FILE = BASE_DIR / "credentials.json"
TOKEN_FILE = BASE_DIR / "token.pkl"

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]
