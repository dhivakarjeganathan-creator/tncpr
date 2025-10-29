"""
Database connection and utility functions for threshold execution system.
"""

import os
import logging
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv('config.env')

# Configure logging
logging.basicConfig(level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Database connection handler for threshold execution system."""
    
    def __init__(self):
        """Initialize database connection parameters."""
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = os.getenv('DB_PORT', '5432')
        self.database = os.getenv('DB_NAME')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        
        if not all([self.database, self.user, self.password]):
            raise ValueError("Database configuration is incomplete. Please check your config.env file.")
    
    def get_connection(self):
        """Get a database connection."""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                cursor_factory=RealDictCursor
            )
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def get_sqlalchemy_engine(self):
        """Get SQLAlchemy engine for pandas operations."""
        connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return create_engine(connection_string)
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries."""
        conn = None
        try:
            conn = self.get_connection()
            logger.info(f"Executing query: {query}")
            logger.info(f"Params: {params}")
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query_pandas(self, query: str) -> pd.DataFrame:
        """Execute a query and return results as pandas DataFrame."""
        try:
            engine = self.get_sqlalchemy_engine()
            return pd.read_sql_query(query, engine)
        except Exception as e:
            logger.error(f"Error executing query with pandas: {e}")
            raise
    
    def execute_ddl(self, ddl: str) -> None:
        """Execute DDL statements (CREATE, ALTER, DROP, etc.)."""
        conn = None
        try:
            conn = self.get_connection()
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(ddl)
            logger.info("DDL executed successfully")
        except Exception as e:
            logger.error(f"Error executing DDL: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def insert_data(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """Insert data into a table."""
        if not data:
            return
        
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Get column names from first row
                columns = list(data[0].keys())
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join(columns)
                
                query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                
                # Prepare data for insertion
                values = []
                for row in data:
                    values.append(tuple(row[col] for col in columns))
                
                cursor.executemany(query, values)
                conn.commit()
                logger.info(f"Inserted {len(data)} rows into {table_name}")
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

def get_threshold_query() -> str:
    """Get the threshold query as specified in requirements."""
    return """
    select b.tablename, a.threshold_id, lower(replace(replace(replace(replace(replace(a.metric, 'smax_',''), 'savg_', ''), 'ssum_', ''), 'smin_', ''), '.','_')) metricname, 
    a.mode, a.category, a.lowerlimit::numeric, a.upperlimit::numeric,
    a.occurrence, a.clearoccurrence, a.cleartime, a.time, a.periodgranularity, a.schedule,
    a.resource, a.threshold_group, COALESCE(c.execution_datetime, a.created_at) execution_datetime 
    from threshold_rules a inner join metricsandtables b on lower(replace(replace(replace(replace(replace(a.metric, 'smax_',''), 'savg_', ''), 'ssum_', ''), 'smin_', ''), '.','_')) = lower(replace(replace(replace(replace(replace(b.metricname, 'smax_',''), 'savg_', ''), 'ssum_', ''), 'smin_', ''), '.','_')) 
	left join threshold_generated_queries c on a.threshold_id = c.threshold_id::integer
    where (a.activeuntil IS NULL 
           OR a.activeuntil = 'No end date'
           OR a.activeuntil::timestamp > now());
    """
