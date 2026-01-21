"""
Database connection module supporting PostgreSQL and Presto (Watsonx Data).
"""
from contextlib import contextmanager
import logging
from typing import Optional, Dict, Any, List

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import requests
from requests.auth import HTTPBasicAuth
import urllib3

from config import Config, DatabaseType

logger = logging.getLogger(__name__)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PRESTO_STATEMENT_ENDPOINT = "/v1/statement"
PRESTO_TIMEOUT_SUBMIT = 30
PRESTO_TIMEOUT_FETCH = 120
PRESTO_FINAL_STATES = {"FINISHED", "FAILED"}


class PrestoConnection:
    """Presto connection using HTTP REST API."""

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        catalog: str,
        schema: str,
    ):
        self.username = username
        self.catalog = catalog
        self.schema = schema
        self.base_url = f"https://{host}:{port}"

        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(username, password)
        self.session.verify = False
        self.session.proxies = {"http": "", "https": ""}
        self.session.trust_env = False

        logger.info("Presto connection initialized: %s/%s", catalog, schema)

    def _get_headers(self, schema: Optional[str] = None) -> Dict[str, str]:
        return {
            "X-Presto-User": self.username,
            "X-Presto-Catalog": self.catalog,
            "X-Presto-Schema": schema or self.schema,
            "Content-Type": "text/plain",
        }

    def _submit_query(self, sql_statement: str, headers: Dict[str, str]) -> Dict[str, Any]:
        response = self.session.post(
            f"{self.base_url}{PRESTO_STATEMENT_ENDPOINT}",
            data=sql_statement,
            headers=headers,
            timeout=PRESTO_TIMEOUT_SUBMIT,
        )
        if response.status_code != 200:
            raise Exception(
                f"Query submission failed (HTTP {response.status_code}): {response.text[:200]}"
            )
        return response.json()

    def _fetch_results(self, initial_result: Dict[str, Any]) -> tuple[List[List], List[Dict]]:
        all_data = []
        columns = None
        result = initial_result

        if result.get("data"):
            all_data.extend(result["data"])
        if result.get("columns"):
            columns = result["columns"]

        while "nextUri" in result:
            response = self.session.get(result["nextUri"], timeout=PRESTO_TIMEOUT_FETCH)
            if response.status_code != 200:
                logger.warning(
                    "Pagination failed at %s: HTTP %s",
                    result["nextUri"],
                    response.status_code,
                )
                break

            result = response.json()
            if result.get("data"):
                all_data.extend(result["data"])
            if result.get("columns"):
                columns = result["columns"]

            if result.get("stats", {}).get("state") in PRESTO_FINAL_STATES:
                break

        if result.get("stats", {}).get("state") == "FAILED":
            error_msg = result.get("error", {}).get("message", "Unknown error")
            raise Exception(f"Query failed: {error_msg}")

        return all_data, columns

    def execute_query(self, sql_statement: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        headers = self._get_headers(schema)
        initial_result = self._submit_query(sql_statement, headers)
        all_data, columns = self._fetch_results(initial_result)

        if columns and all_data:
            column_names = [col["name"] for col in columns]
            return [dict(zip(column_names, row)) for row in all_data]
        return []

    def close(self):
        if self.session:
            self.session.close()


class DatabaseConnection:
    """Unified database connection manager."""

    def __init__(self):
        Config.validate_required()
        self.db_type = Config.DB_TYPE
        self.connection_pool: Optional[pool.ThreadedConnectionPool] = None
        self.presto_conn: Optional[PrestoConnection] = None
        self._initialize_connection()

    def _initialize_connection(self):
        if Config.is_postgresql():
            self._init_postgresql()
        elif Config.is_presto():
            self._init_presto()
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def _init_postgresql(self):
        self.connection_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            database=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
        )
        logger.info("PostgreSQL connection pool initialized")

    def _init_presto(self):
        self.presto_conn = PrestoConnection(
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            username=Config.DB_USER,
            password=Config.DB_PASSWORD,
            catalog=Config.PRESTO_CATALOG or "hive",
            schema=Config.PRESTO_SCHEMA or Config.DB_NAME,
        )
        logger.info(
            "Presto connection initialized: %s/%s",
            Config.PRESTO_CATALOG or "hive",
            Config.PRESTO_SCHEMA or Config.DB_NAME,
        )

    @contextmanager
    def get_connection(self):
        if Config.is_postgresql():
            if not self.connection_pool:
                raise RuntimeError("PostgreSQL connection pool not initialized")
            conn = self.connection_pool.getconn()
            try:
                yield conn
            finally:
                self.connection_pool.putconn(conn)
        elif Config.is_presto():
            if not self.presto_conn:
                raise RuntimeError("Presto connection not initialized")
            yield self.presto_conn
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def execute_query(self, query: str, params: Optional[list] = None) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            if Config.is_postgresql():
                return self._execute_postgresql_query(conn, query, params)
            return conn.execute_query(query)

    @staticmethod
    def _execute_postgresql_query(conn, query: str, params: Optional[list]) -> List[Dict[str, Any]]:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, tuple(params) if params else None)
        return [dict(row) for row in cursor.fetchall()]

    def close(self):
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("PostgreSQL connection pool closed")
        if self.presto_conn:
            self.presto_conn.close()
            logger.info("Presto connection closed")


db_connection = DatabaseConnection()
