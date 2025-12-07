"""
Query builder module for constructing SQL queries compatible with both PostgreSQL and Presto
"""
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from config import Config

logger = logging.getLogger(__name__)


class QueryBuilder:
    """Build SQL queries compatible with both PostgreSQL and Presto"""
    
    def __init__(self, table_name: str, entity_columns: List[str], timestamp_column: str = "timestamp"):
        """
        Initialize query builder
        
        Args:
            table_name: Name of the table
            entity_columns: List of entity column names
            timestamp_column: Name of the timestamp column
        """
        self.table_name = table_name
        self.entity_columns = entity_columns
        self.timestamp_column = timestamp_column
        self.db_type = Config.DB_TYPE
    
    def _escape_identifier(self, identifier: str) -> str:
        """Escape SQL identifier based on database type"""
        if self.db_type == "postgresql":
            return f'"{identifier}"'
        else:  # Presto
            return f'"{identifier}"'
    
    def _format_timestamp(self, timestamp: Any) -> str:
        """
        Format timestamp for SQL query
        Since timestamp is stored as character varying(50) containing Unix timestamps in milliseconds,
        we compare as strings
        """
        if isinstance(timestamp, (int, float)):
            # Convert to string for comparison with character varying column
            timestamp_str = str(int(timestamp))
            return f"'{timestamp_str}'"
        elif isinstance(timestamp, str):
            # Try to parse as Unix timestamp first
            try:
                # Validate it's a numeric string (Unix timestamp in milliseconds)
                ts_int = int(timestamp)
                return f"'{timestamp}'"
            except ValueError:
                # If not numeric, use as-is (shouldn't happen for Unix timestamps)
                return f"'{timestamp}'"
        else:
            raise ValueError(f"Unsupported timestamp format: {type(timestamp)}")
    
    def _get_aggregation_function(self, granularity: Optional[str]) -> str:
        """
        Get SQL aggregation function for timestamp based on granularity
        
        Since timestamp is stored as character varying containing Unix timestamps in milliseconds,
        we need to convert to timestamp first, then truncate, then convert back to string
        
        Args:
            granularity: Time granularity like "1-hour", "1-day", etc.
            
        Returns:
            SQL expression for grouping timestamp
        """
        if not granularity:
            return self._escape_identifier(self.timestamp_column)
        
        # Parse granularity (e.g., "1-hour", "30-minute", "1-day")
        parts = granularity.split("-")
        if len(parts) != 2:
            raise ValueError(f"Invalid granularity format: {granularity}. Expected format: 'N-unit' (e.g., '1-hour')")
        
        value = int(parts[0])
        unit = parts[1].lower()
        
        # Since timestamp is stored as character varying with Unix timestamp in milliseconds,
        # we need to: convert string to bigint, divide by 1000, truncate, multiply by 1000, convert back to string
        timestamp_col = self._escape_identifier(self.timestamp_column)
        
        if self.db_type == "postgresql":
            # Convert string to bigint, truncate time, convert back to milliseconds string
            if unit in ["hour", "hours"]:
                # Truncate to hour: convert to timestamp, truncate, convert back to milliseconds string
                return f"CAST(EXTRACT(EPOCH FROM DATE_TRUNC('hour', TO_TIMESTAMP({timestamp_col}::bigint / 1000.0))) * 1000 AS BIGINT)::text"
            elif unit in ["minute", "minutes"]:
                return f"CAST(EXTRACT(EPOCH FROM DATE_TRUNC('minute', TO_TIMESTAMP({timestamp_col}::bigint / 1000.0))) * 1000 AS BIGINT)::text"
            elif unit in ["day", "days"]:
                return f"CAST(EXTRACT(EPOCH FROM DATE_TRUNC('day', TO_TIMESTAMP({timestamp_col}::bigint / 1000.0))) * 1000 AS BIGINT)::text"
            else:
                raise ValueError(f"Unsupported time unit: {unit}")
        else:  # Presto
            # Similar logic for Presto - convert back to Unix milliseconds string
            if unit in ["hour", "hours"]:
                return f"CAST(TO_UNIXTIME(DATE_TRUNC('hour', FROM_UNIXTIME(CAST({timestamp_col} AS BIGINT) / 1000))) * 1000 AS VARCHAR)"
            elif unit in ["minute", "minutes"]:
                return f"CAST(TO_UNIXTIME(DATE_TRUNC('minute', FROM_UNIXTIME(CAST({timestamp_col} AS BIGINT) / 1000))) * 1000 AS VARCHAR)"
            elif unit in ["day", "days"]:
                return f"CAST(TO_UNIXTIME(DATE_TRUNC('day', FROM_UNIXTIME(CAST({timestamp_col} AS BIGINT) / 1000))) * 1000 AS VARCHAR)"
            else:
                raise ValueError(f"Unsupported time unit: {unit}")
    
    def build_query(
        self,
        metrics: Optional[List[str]] = None,
        entity_filters: Optional[Dict[str, List[str]]] = None,
        start_time: Optional[Any] = None,
        end_time: Optional[Any] = None,
        properties: Optional[List[str]] = None,
        granularity: Optional[str] = None,
        order_by: Optional[str] = None
    ) -> tuple[str, list]:
        """
        Build SQL query for timeseries data
        
        Args:
            metrics: List of KPI/metric column names to select
            entity_filters: Dictionary mapping entity column names to lists of values
            start_time: Start timestamp (Unix timestamp in milliseconds or ISO string)
            end_time: End timestamp (Unix timestamp in milliseconds or ISO string)
            properties: List of entity columns to return in results
            granularity: Time granularity for aggregation (e.g., "1-hour")
            order_by: Order by clause (e.g., "time+" for ascending, "time-" for descending)
            
        Returns:
            Tuple of (SQL query string, parameters list for PostgreSQL or empty list for Presto)
        """
        # Build SELECT clause
        select_parts = []
        
        # Add timestamp (possibly aggregated)
        timestamp_expr = self._get_aggregation_function(granularity)
        select_parts.append(f"{timestamp_expr} AS {self._escape_identifier(self.timestamp_column)}")
        
        # Add entity columns (properties)
        if properties:
            for prop in properties:
                if prop in self.entity_columns:
                    select_parts.append(self._escape_identifier(prop))
        else:
            # Include all entity columns if not specified
            for entity_col in self.entity_columns:
                select_parts.append(self._escape_identifier(entity_col))
        
        # Add metrics/KPI columns
        if metrics:
            for metric in metrics:
                # Validate metric column name (basic sanitization)
                metric_col = self._escape_identifier(metric)
                if granularity:
                    # Aggregate metrics - handle both character varying and bigint/numeric types
                    # Some columns are bigint, some are character varying
                    if self.db_type == "postgresql":
                        # PostgreSQL: Cast to text first to handle both text and numeric types
                        # Then validate with regex and cast to numeric for aggregation
                        # Pattern matches: optional sign, digits, optional decimal point and digits
                        select_parts.append(
                            f"AVG(CASE WHEN TRIM(CAST({metric_col} AS TEXT)) ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN CAST(TRIM(CAST({metric_col} AS TEXT)) AS NUMERIC) ELSE NULL END) AS {metric_col}"
                        )
                    else:  # Presto
                        # Presto: use TRY_CAST which returns NULL for invalid values
                        # Cast to text first, then to double
                        select_parts.append(
                            f"AVG(TRY_CAST(CAST({metric_col} AS VARCHAR) AS DOUBLE)) AS {metric_col}"
                        )
                else:
                    select_parts.append(metric_col)
        else:
            # If no metrics specified, we need to get all non-entity, non-timestamp columns
            # This would require schema introspection, so for now we'll require metrics
            raise ValueError("At least one metric must be specified")
        
        select_clause = ", ".join(select_parts)
        
        # Build FROM clause
        from_clause = f"FROM {self._escape_identifier(self.table_name)}"
        
        # Build WHERE clause
        where_conditions = []
        params = []  # Use list for PostgreSQL compatibility
        
        # Entity filters
        if entity_filters:
            for entity_col, values in entity_filters.items():
                if entity_col in self.entity_columns and values:
                    if len(values) == 1:
                        where_conditions.append(f"{self._escape_identifier(entity_col)} = %s")
                        params.append(values[0])
                    else:
                        placeholders = ", ".join([f"%s" for _ in values])
                        where_conditions.append(f"{self._escape_identifier(entity_col)} IN ({placeholders})")
                        params.extend(values)
        
        # Time range filters
        if start_time:
            where_conditions.append(f"{self._escape_identifier(self.timestamp_column)} >= {self._format_timestamp(start_time)}")
        if end_time:
            where_conditions.append(f"{self._escape_identifier(self.timestamp_column)} <= {self._format_timestamp(end_time)}")
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # Build GROUP BY clause (if aggregation)
        group_by_clause = ""
        if granularity:
            group_by_parts = [timestamp_expr]
            if properties:
                for prop in properties:
                    if prop in self.entity_columns:
                        group_by_parts.append(self._escape_identifier(prop))
            else:
                for entity_col in self.entity_columns:
                    group_by_parts.append(self._escape_identifier(entity_col))
            group_by_clause = "GROUP BY " + ", ".join(group_by_parts)
        
        # Build ORDER BY clause
        order_by_clause = ""
        if order_by:
            if order_by.lower() == "time+" or order_by.lower() == "time":
                order_by_clause = f"ORDER BY {self._escape_identifier(self.timestamp_column)} ASC"
            elif order_by.lower() == "time-":
                order_by_clause = f"ORDER BY {self._escape_identifier(self.timestamp_column)} DESC"
        else:
            # Default ordering by timestamp ascending
            order_by_clause = f"ORDER BY {self._escape_identifier(self.timestamp_column)} ASC"
        
        # Construct full query
        query = f"""
            SELECT {select_clause}
            {from_clause}
            {where_clause}
            {group_by_clause}
            {order_by_clause}
        """.strip()
        
        # For Presto, we need to handle parameterized queries differently
        # For now, we'll use string formatting for Presto (with proper escaping)
        if self.db_type == "presto":
            # Replace %s placeholders with actual values
            query_parts = query.split("%s")
            final_query = ""
            for i, part in enumerate(query_parts):
                final_query += part
                if i < len(params):
                    # Escape and format the value
                    val = params[i]
                    if isinstance(val, str):
                        final_query += f"'{val.replace(chr(39), chr(39)+chr(39))}'"  # Escape single quotes
                    else:
                        final_query += str(val)
            query = final_query
            params = []
        
        return query, params

