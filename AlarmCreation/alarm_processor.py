#!/usr/bin/env python3
"""
Alarm Processing Script

This script executes a query to retrieve alarm data and generates INSERT statements
for each row returned. It also tracks the last execution time to avoid duplicate processing.
"""

import os
import sys
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Tuple, Optional

# Load environment variables
load_dotenv('database.env')

class AlarmProcessor:
    def __init__(self):
        """Initialize the AlarmProcessor with database connection parameters."""
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'hierarchy_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'test@1234')
        }
        self.connection = None
        self.cursor = None

    def connect(self):
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.cursor = self.connection.cursor()
            print("Successfully connected to the database.")
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            sys.exit(1)

    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Database connection closed.")

    def create_last_execution_table(self):
        """Create table to store last execution time if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS alarm_last_execution (
            id SERIAL PRIMARY KEY,
            execution_time TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        try:
            self.cursor.execute(create_table_sql)
            self.connection.commit()
            print("Last execution table created/verified successfully.")
        except psycopg2.Error as e:
            print(f"Error creating last execution table: {e}")
            sys.exit(1)

    def get_last_execution_time(self) -> Optional[datetime]:
        """Get the last execution time from the database."""
        try:
            self.cursor.execute("""
                SELECT execution_time 
                FROM alarm_last_execution 
                ORDER BY created_at DESC 
                LIMIT 1
            """)
            result = self.cursor.fetchone()
            return result[0] if result else None
        except psycopg2.Error as e:
            print(f"Error retrieving last execution time: {e}")
            return None

    def update_last_execution_time(self, execution_time: datetime):
        """Update the last execution time in the database."""
        try:
            self.cursor.execute("""
                INSERT INTO alarm_last_execution (execution_time) 
                VALUES (%s)
            """, (execution_time,))
            self.connection.commit()
            print(f"Last execution time updated to: {execution_time}")
        except psycopg2.Error as e:
            print(f"Error updating last execution time: {e}")
            sys.exit(1)

    def _get_base_query(self) -> str:
        """Get the base query without WHERE clause."""
        return """
        SELECT distinct
            b.nfname_expr_value AS node,
            CASE 
                WHEN lower(a.alarm_severity) = 'critical' THEN 5
                WHEN lower(a.alarm_severity) = 'major' THEN 4
                WHEN lower(a.alarm_severity) = 'minor' THEN 3
                ELSE 0
            END AS severity,
            a.alarm_message AS summary,
            a.metricname AS alertgroup,
            a.record_id AS alertkey,
            a.record_id AS identifier,
            '1' AS type,
            b.region AS Region
        FROM threshold_alarms a 
        LEFT JOIN nfname_results b ON a.record_id = b.id_expr
        """

    def _execute_query_with_results(self, query: str, params: tuple = None) -> List[Tuple]:
        """Execute query and return results with error handling."""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            results = self.cursor.fetchall()
            print(f"Query executed successfully. Found {len(results)} records.")
            return results
        except psycopg2.Error as e:
            print(f"Error executing alarm query: {e}")
            sys.exit(1)

    def execute_alarm_query(self, last_execution_time: Optional[datetime]) -> List[Tuple]:
        """Execute the alarm query and return results."""
        base_query = self._get_base_query()
        
        if last_execution_time is None:
            # No previous execution, get all records
            return self._execute_query_with_results(base_query)
            print(f"Base query: {base_query}")
        else:
            # Use last execution time to filter records
            query = base_query + " WHERE a.created_at > %s"
            print(f"Query: {query}")
            print(f"Last execution time: {last_execution_time}")
            return self._execute_query_with_results(query, (last_execution_time,))

    def generate_insert_statements(self, results: List[Tuple]) -> List[str]:
        """Generate INSERT statements for each row."""
        insert_statements = []
        
        for row in results:
            node, severity, summary, alertgroup, alertkey, identifier, type_val, region = row
            
            # Escape single quotes in string values
            def escape_string(value):
                if value is None:
                    return 'NULL'
                return f"'{str(value).replace("'", "''")}'"
            
            insert_sql = f"""INSERT INTO alerts.status(Node,Severity,Summary,AlertGroup,AlertKey,Identifier,Type,Region) VALUES 
({escape_string(node)}, {severity}, {escape_string(summary)}, {escape_string(alertgroup)}, {escape_string(alertkey)}, {escape_string(identifier)}, {type_val}, {escape_string(region)});"""
            
            insert_statements.append(insert_sql)
        
        return insert_statements

    def save_sql_to_file(self, insert_statements: List[str], filename: str = None):
        """Save all INSERT statements to a SQL file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"alarm_inserts_{timestamp}.sql"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                
                for statement in insert_statements:
                    f.write(statement + "\n")
            
            print(f"SQL statements saved to: {filename}")
        except IOError as e:
            print(f"Error saving SQL file: {e}")
            sys.exit(1)

    def process_alarms(self):
        """Main method to process alarms."""
        print("Starting alarm processing...")
        
        # Connect to database
        self.connect()
        
        try:
            # Create last execution table
            self.create_last_execution_table()
            
            # Get last execution time
            last_execution_time = self.get_last_execution_time()
            # if last_execution_time:
            #     print(f"Last execution time: {last_execution_time}")
            # else:
            #     print("No previous execution found. Using current time.")
            #     last_execution_time = None
            
            # Execute alarm query
            results = self.execute_alarm_query(last_execution_time)
            
            if not results:
                print("No new alarms found.")
                return
            
            # Generate INSERT statements
            insert_statements = self.generate_insert_statements(results)
            
            # Save to SQL file
            self.save_sql_to_file(insert_statements)
            
            # Update last execution time
            current_time = datetime.now()
            self.update_last_execution_time(current_time)
            
            print(f"Processing completed successfully. Created {len(insert_statements)} alarms in TNCA.")
            
        finally:
            # Always disconnect
            self.disconnect()

def main():
    """Main entry point."""
    processor = AlarmProcessor()
    processor.process_alarms()

if __name__ == "__main__":
    main()
