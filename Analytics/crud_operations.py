"""
CRUD operations module for analytics database.
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from database_config import DatabaseConfig
from table_schemas import TableSchemas
import pandas as pd

logger = logging.getLogger(__name__)

class CRUDOperations:
    """CRUD operations for analytics tables."""
    
    def __init__(self, db_config: DatabaseConfig):
        """Initialize CRUD operations."""
        self.db_config = db_config
    
    def create_tables(self) -> bool:
        """Create all tables if they don't exist."""
        try:
            table_names = TableSchemas.get_all_table_names()
            
            for table_name in table_names:
                sql = TableSchemas.get_table_creation_sql(table_name)
                self.db_config.execute_query(sql)
                logger.info(f"Created table: {table_name}")
            
            return True
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            return False
    
    def drop_tables(self) -> bool:
        """Drop all tables (use with caution)."""
        try:
            table_names = TableSchemas.get_all_table_names()
            
            for table_name in table_names:
                sql = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
                self.db_config.execute_query(sql)
                logger.info(f"Dropped table: {table_name}")
            
            return True
        except Exception as e:
            logger.error(f"Error dropping tables: {e}")
            return False
    
    def insert_data(self, table_name: str, data: List[Tuple], columns: List[str]) -> int:
        """Insert data into table."""
        try:
            if not data:
                logger.warning(f"No data to insert into {table_name}")
                return 0
            
            # Generate INSERT SQL
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join(columns)
            
            sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
            """
            
            # Execute batch insert
            rows_inserted = self.db_config.execute_batch(sql, data)
            logger.info(f"Inserted {rows_inserted} rows into {table_name}")
            return rows_inserted
            
        except Exception as e:
            logger.error(f"Error inserting data into {table_name}: {e}")
            return 0
    
    def update_data(self, table_name: str, data: Dict[str, Any], 
                   where_conditions: Dict[str, Any]) -> int:
        """Update data in table."""
        try:
            # Build SET clause
            set_clauses = []
            set_values = []
            
            for column, value in data.items():
                set_clauses.append(f"{column} = %s")
                set_values.append(value)
            
            # Build WHERE clause
            where_clauses = []
            where_values = []
            
            for column, value in where_conditions.items():
                where_clauses.append(f"{column} = %s")
                where_values.append(value)
            
            sql = f"""
            UPDATE {table_name}
            SET {', '.join(set_clauses)}
            WHERE {' AND '.join(where_clauses)}
            """
            
            all_values = set_values + where_values
            rows_updated = self.db_config.execute_query(sql, tuple(all_values))
            logger.info(f"Updated {rows_updated} rows in {table_name}")
            return rows_updated
            
        except Exception as e:
            logger.error(f"Error updating data in {table_name}: {e}")
            return 0
    
    def delete_data(self, table_name: str, where_conditions: Dict[str, Any]) -> int:
        """Delete data from table."""
        try:
            # Build WHERE clause
            where_clauses = []
            where_values = []
            
            for column, value in where_conditions.items():
                where_clauses.append(f"{column} = %s")
                where_values.append(value)
            
            sql = f"""
            DELETE FROM {table_name}
            WHERE {' AND '.join(where_clauses)}
            """
            
            rows_deleted = self.db_config.execute_query(sql, tuple(where_values))
            logger.info(f"Deleted {rows_deleted} rows from {table_name}")
            return rows_deleted
            
        except Exception as e:
            logger.error(f"Error deleting data from {table_name}: {e}")
            return 0
    
    def select_data(self, table_name: str, columns: Optional[List[str]] = None,
                   where_conditions: Optional[Dict[str, Any]] = None,
                   limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Select data from table."""
        try:
            # Build SELECT clause
            if columns:
                columns_str = ', '.join(columns)
            else:
                columns_str = '*'
            
            sql = f"SELECT {columns_str} FROM {table_name}"
            values = []
            
            # Build WHERE clause
            if where_conditions:
                where_clauses = []
                for column, value in where_conditions.items():
                    where_clauses.append(f"{column} = %s")
                    values.append(value)
                
                sql += f" WHERE {' AND '.join(where_clauses)}"
            
            # Add LIMIT clause
            if limit:
                sql += f" LIMIT {limit}"
            
            results = self.db_config.execute_query(sql, tuple(values) if values else None)
            logger.info(f"Retrieved {len(results)} rows from {table_name}")
            return results
            
        except Exception as e:
            logger.error(f"Error selecting data from {table_name}: {e}")
            return []
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get table information."""
        try:
            # Get column information
            sql = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """
            
            columns = self.db_config.execute_query(sql, (table_name,))
            
            # Get row count
            count_sql = f"SELECT COUNT(*) FROM {table_name}"
            count_result = self.db_config.execute_query(count_sql)
            row_count = count_result[0]['count'] if count_result else 0
            
            return {
                'table_name': table_name,
                'columns': columns,
                'row_count': row_count
            }
            
        except Exception as e:
            logger.error(f"Error getting table info for {table_name}: {e}")
            return {}
    
    def get_all_tables_info(self) -> Dict[str, Dict[str, Any]]:
        """Get information for all tables."""
        table_names = TableSchemas.get_all_table_names()
        tables_info = {}
        
        for table_name in table_names:
            tables_info[table_name] = self.get_table_info(table_name)
        
        return tables_info
    
    def bulk_insert_from_dataframe(self, table_name: str, df: pd.DataFrame) -> int:
        """Bulk insert data from pandas DataFrame."""
        try:
            if df.empty:
                logger.warning(f"No data to insert into {table_name}")
                return 0
            
            # Prepare data
            data_tuples = []
            for _, row in df.iterrows():
                row_data = []
                for col in df.columns:
                    value = row[col]
                    if pd.isna(value) or value is None:
                        row_data.append(None)
                    else:
                        row_data.append(value)
                data_tuples.append(tuple(row_data))
            
            # Insert data
            return self.insert_data(table_name, data_tuples, list(df.columns))
            
        except Exception as e:
            logger.error(f"Error bulk inserting data into {table_name}: {e}")
            return 0
    
    def truncate_table(self, table_name: str) -> bool:
        """Truncate table (remove all data)."""
        try:
            sql = f"TRUNCATE TABLE {table_name} CASCADE"
            self.db_config.execute_query(sql)
            logger.info(f"Truncated table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error truncating table {table_name}: {e}")
            return False
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        try:
            sql = f"SELECT COUNT(*) FROM {table_name}"
            result = self.db_config.execute_query(sql)
            return result[0]['count'] if result else 0
        except Exception as e:
            logger.error(f"Error getting row count for {table_name}: {e}")
            return 0
