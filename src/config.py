import os
from functools import lru_cache
from pathlib import Path

import yaml
from dotenv import load_dotenv


# Project paths

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SETTINGS_FILE = PROJECT_ROOT / "config" / "settings.yaml"

ENV_FILE = PROJECT_ROOT / ".env"


load_dotenv(ENV_FILE)


# Helper for missing required fields/columns
def raise_if_missing_required(obj: dict, required: list[str], label: str = "fields") -> None:
    missing = [item for item in required if item not in obj]
    if missing:
        raise ValueError(f"Missing required {label}: {', '.join(missing)}")


# Database Configuration

class DatabaseConfig:

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg://"
            f"{self.user}:{self.password}"
            f"@{self.host}:{self.port}"
            f"/{self.database}"
        )


# Settings Configuration

class Settings:

    def __init__(self):

        self.project_root = PROJECT_ROOT

        self._config = self._load_yaml()

        self._load_pipeline()

        self._load_paths()

        self._load_validation()

        self._load_batch()

        self._load_schema()

        self._load_logging()

        self.database = self._load_database()

    # Load YAML configuration
    def _load_yaml(self):

        if not SETTINGS_FILE.exists():
            raise FileNotFoundError(f"Configuration file not found: {SETTINGS_FILE}")

        with SETTINGS_FILE.open("r", encoding="utf-8") as file:
            settings = yaml.safe_load(file) or {}

        required_sections = ["pipeline", "paths", "validation", "batch", "schema", "logging"]
        raise_if_missing_required(settings, required_sections, "sections")

        return settings

    # Load pipeline configuration
    def _load_pipeline(self):

        pipeline = self._config["pipeline"]

        self.pipeline_name = pipeline.get("name", "healthcare_claims_pipeline")

        self.pipeline_version = str(pipeline.get("version", "1.0.0"))

    # Load paths configuration
    def _load_paths(self):

        paths = self._config["paths"]

        required_paths = ["incoming", "processed", "rejected", "archive", "logs"]
        raise_if_missing_required(paths, required_paths, "paths")

        self.incoming_dir = PROJECT_ROOT / paths["incoming"]

        self.processed_dir = PROJECT_ROOT / paths["processed"]

        self.rejected_dir = PROJECT_ROOT / paths["rejected"]

        self.archive_dir = PROJECT_ROOT / paths["archive"]

        self.logs_dir = PROJECT_ROOT / paths["logs"]

    # Load validation configuration
    def _load_validation(self):

        validation = self._config["validation"]

        self.high_value_threshold = float(validation.get("high_value_threshold", 10000))

        self.late_submission_days = int(validation.get("late_submission_days", 90))

    # Load batch configuration
    def _load_batch(self):

        batch = self._config["batch"]

        self.chunk_size = int(batch.get("chunk_size", 5000))

        self.archive_after_success = bool(batch.get("archive_after_success", True))

    # Load schema configuration
    def _load_schema(self):

        schema = self._config["schema"]

        required_columns = [
            "source_columns",
            "required_fields",
            "allowed_statuses",
            "allowed_claim_types",
            "duplicate_key_fields",
            "lineage_columns",
        ]
        raise_if_missing_required(schema, required_columns, "columns")

        self.source_columns = list(schema["source_columns"])

        self.required_fields = list(schema["required_fields"])

        self.allowed_statuses = list(schema["allowed_statuses"])

        self.allowed_claim_types = list(schema["allowed_claim_types"])

        self.duplicate_key_fields = list(schema["duplicate_key_fields"])

        self.lineage_columns = list(schema["lineage_columns"])

    # Load logging configuration

    def _load_logging(self):

        logging_config = self._config["logging"]

        self.log_level = logging_config.get("level", "INFO").upper()

    
    # Require environment variable
    def _require_env(self, key: str):

        value = os.getenv(key)

        if value is None:
            raise ValueError(f"Missing environment variable: {key}")

        return value

    # Load database configuration
    def _load_database(self):

        return DatabaseConfig(
            host=self._require_env("POSTGRES_HOST"),
            port=int(self._require_env("POSTGRES_PORT")),
            database=self._require_env("POSTGRES_DB"),
            user=self._require_env("POSTGRES_USER"),
            password=self._require_env("POSTGRES_PASSWORD")
        )


# Cached settings
@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def clear_settings_cache() -> None:
    get_settings.cache_clear()