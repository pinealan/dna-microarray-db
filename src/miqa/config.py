"""
Central configuration — reads all environment variables from .env or the environment.
"""

import logging
import os

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL', '')

S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL', '')
S3_BUCKET = os.environ.get('S3_BUCKET', '')
S3_KEY = os.environ.get('S3_KEY', '')
S3_SECRET = os.environ.get('S3_SECRET', '')
