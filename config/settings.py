"""
Settings for overseas stock asset management system (Korea Investment & Securities).
All sensitive values are loaded from .env file.
"""

from pathlib import Path
from pydantic_settings import BaseSettings

BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """
    Application settings loaded from .env file.
    Create a .env file in the project root with your credentials.
    """

    # KIS API credentials
    APP_KEY: str
    SECRET_KEY: str
    BASE_URL: str
    SOCKET_URL: str = ""  # Optional

    # Account information
    CANO: str  # 계좌번호 앞 8자리
    ACNT_PRDT_CD: str  # 계좌번호 뒤 2자리

    # Database configuration
    DB_HOST: str = "localhost"
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str = "asset_us"

    model_config = {
        "env_file": str(BASE_DIR / ".env"),
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }
