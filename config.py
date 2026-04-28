from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-me')
    DEBUG = os.getenv('FLASK_DEBUG', '1') == '1'

    DATA_DIR = os.getenv('JOBGUARD_DATA_DIR', str(BASE_DIR / 'data'))
    DATA_FILE = os.getenv('JOBGUARD_DATA_FILE', str(Path(DATA_DIR) / 'jobguard_data.json'))

    UPLOAD_FOLDER = os.getenv('JOBGUARD_UPLOAD_FOLDER', str(BASE_DIR / 'uploads'))
    MAX_CONTENT_LENGTH = int(os.getenv('MAX_CONTENT_LENGTH', str(10 * 1024 * 1024)))