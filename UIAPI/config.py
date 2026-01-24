"""
Configuration module for UI Timeseries API.
Supports PostgreSQL and Watsonx Data (Presto).
"""
import logging
import os
from enum import Enum
from typing import Optional, Set

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

try:
    load_dotenv()
except (UnicodeDecodeError, IOError, Exception) as exc:
    logger.warning(
        "Could not load .env file: %s. Using environment defaults.",
        exc,
    )


class DatabaseType(str, Enum):
    POSTGRESQL = "postgresql"
    PRESTO = "presto"
    WATSONX = "watsonx.data"


class Config:
    # Database
    DB_TYPE: str = os.getenv("DB_TYPE", DatabaseType.POSTGRESQL.value).lower()
    DB_HOST: Optional[str] = os.getenv("DB_HOST")
    DB_PORT: Optional[int] = int(os.getenv("DB_PORT")) if os.getenv("DB_PORT") else None
    DB_NAME: Optional[str] = os.getenv("DB_NAME")
    DB_USER: Optional[str] = os.getenv("DB_USER")
    DB_PASSWORD: Optional[str] = os.getenv("DB_PASSWORD")
    DB_SCHEMA: str = os.getenv("DB_SCHEMA", "public")

    PRESTO_CATALOG: Optional[str] = os.getenv("PRESTO_CATALOG", None)
    PRESTO_SCHEMA: Optional[str] = os.getenv("PRESTO_SCHEMA", None)

    # API
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8001"))
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"

    # Metrics mapping table
    METRICS_TABLE_NAME: str = os.getenv("METRICS_TABLE_NAME", "metricsandtables")
    METRICS_TABLE_METRIC_COLUMN: str = os.getenv("METRICS_TABLE_METRIC_COLUMN", "metrics")
    METRICS_TABLE_TABLE_COLUMN: str = os.getenv("METRICS_TABLE_TABLE_COLUMN", "tablename")

    RULE_EXECUTION_TABLE_NAME: Optional[str] = os.getenv(
        "RULE_EXECUTION_TABLE_NAME"
    )
    ENRICHMENT_TABLE_NAME: str = os.getenv("ENRICHMENT_TABLE_NAME", "enrichmentlookup")
    ENRICHMENT_TABLE_ENTITY_COLUMN: str = os.getenv(
        "ENRICHMENT_TABLE_ENTITY_COLUMN", "entityid"
    )

    # Allowed tables
    ALLOWED_TABLES: Set[str] = {
        "acpf_gnb_samsung",
        "aupf_gnb_samsung",
        "acpf_vcu_samsung",
        "aupf_vcu_samsung",
        "aupf_vm_samsung",
        "carrier_corning",
        "carrier_samsung",
        "carrier_ericsson",
        "du_corning",
        "du_samsung",
        "gnb_corning",
        "gnb_ericsson",
        "mkt_corning",
        "mkt_samsung",
        "mkt_ericsson",
        "sector_corning",
        "sector_samsung",
        "sector_ericsson",
        "ruleexecutionresults",
        "rule_execution_results",
    }

    @classmethod
    def validate_required(cls) -> None:
        missing = []
        for key in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"):
            if getattr(cls, key) in (None, ""):
                missing.append(key)
        if missing:
            raise ValueError(
                "Missing required configuration in .env: " + ", ".join(missing)
            )
        if cls.DB_TYPE not in {
            DatabaseType.POSTGRESQL.value,
            DatabaseType.PRESTO.value,
            DatabaseType.WATSONX.value,
        }:
            raise ValueError(
                "DB_TYPE must be 'postgresql', 'presto', or 'watsonx.data'"
            )

    @classmethod
    def is_postgresql(cls) -> bool:
        return cls.DB_TYPE == DatabaseType.POSTGRESQL.value

    @classmethod
    def is_presto(cls) -> bool:
        return cls.DB_TYPE in {DatabaseType.PRESTO.value, DatabaseType.WATSONX.value}
