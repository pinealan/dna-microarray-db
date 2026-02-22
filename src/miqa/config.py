"""
Central configuration — reads all environment variables from .env or the environment.
"""

import logging
import os

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "")

S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

_log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_LEVEL: int = getattr(logging, _log_level_str, logging.INFO)
