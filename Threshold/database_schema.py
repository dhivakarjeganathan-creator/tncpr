"""
Database schema and table creation for threshold rules processing.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, Dict, Any
import os

class ThresholdDatabase:
    """Database manager for threshold rules."""
    
    def __init__(self, db_config: Dict[str, Any] = None):
        """Initialize database connection with PostgreSQL configuration."""
        self.db_config = db_config or self._get_default_config()
        self.conn = None
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default database configuration from environment variables."""
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'threshold_rules'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password')
        }
    
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            self.conn.autocommit = False  # Use transactions
            print(f"Connected to PostgreSQL database: {self.db_config['database']}")
        except psycopg2.Error as e:
            raise Exception(f"Failed to connect to PostgreSQL: {e}")
    
    def create_table(self):
        """Create the threshold rules table."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS threshold_rules (
            threshold_id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            metric VARCHAR(255) NOT NULL,
            mode VARCHAR(50) NOT NULL,
            category VARCHAR(50) NOT NULL,
            lowerlimit DECIMAL(12,2),
            upperlimit DECIMAL(12,2),
            occurrence INTEGER,
            clearoccurrence INTEGER,
            cleartime INTEGER,
            time INTEGER,
            activeuntil TEXT,
            periodgranularity INTEGER,
            schedule TEXT,
            tag TEXT,
            user_groups TEXT,
            resource TEXT,
            threshold_group TEXT,
            target_rule VARCHAR(255),
            can_edit BOOLEAN,
            owner VARCHAR(255),
            update_time BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        if self.conn:
            cursor = self.conn.cursor()
            cursor.execute(create_table_sql)
            self.conn.commit()
            print("Table 'threshold_rules' created successfully.")
        else:
            raise Exception("Database connection not established. Call connect() first.")
    
    def insert_threshold_rule(self, rule_data: dict) -> int:
        """Insert a threshold rule into the database."""
        insert_sql = """
        INSERT INTO threshold_rules 
        (name, metric, mode, category, lowerlimit, upperlimit, occurrence, 
         clearoccurrence, cleartime, time, activeuntil, periodgranularity, schedule,
         tag, user_groups, resource, threshold_group, target_rule, can_edit, owner, update_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING threshold_id
        """
        
        cursor = self.conn.cursor()
        cursor.execute(insert_sql, (
            rule_data['name'],
            rule_data['metric'],
            rule_data['mode'],
            rule_data['category'],
            rule_data['lowerlimit'],
            rule_data['upperlimit'],
            rule_data['occurrence'],
            rule_data['clearoccurrence'],
            rule_data['cleartime'],
            rule_data['time'],
            rule_data['activeuntil'],
            rule_data['periodgranularity'],
            rule_data['schedule'],
            rule_data['tag'],
            rule_data['user_groups'],
            rule_data['resource'],
            rule_data['threshold_group'],
            rule_data['target_rule'],
            rule_data['can_edit'],
            rule_data['owner'],
            rule_data['update_time']
        ))
        self.conn.commit()
        return cursor.fetchone()[0]
    
    def get_all_rules(self):
        """Retrieve all threshold rules from the database."""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT * FROM threshold_rules ORDER BY threshold_id")
        return cursor.fetchall()
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
