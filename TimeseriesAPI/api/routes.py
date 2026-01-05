"""
API routes for KPI Timeseries endpoints - FIXED VERSION
Fixed: Removed non-existent map_column_name call in transform_to_flat_format
"""
import logging
import re
from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
from .response_transformer import get_transformer
from database.connection import db_connection
from database.query_builder import QueryBuilder
from utils.error_handler import handle_exception, ValidationError, DatabaseError
from utils.validators import (
    validate_metrics,
    validate_timestamp,
    validate_granularity,
    validate_entity_filters
)
from config import Config

logger = logging.getLogger(__name__)

router = APIRouter()

# Table configuration mapping
# 18 horizontal tables + 1 vertical table (rule_execution_results)
TABLE_CONFIG = {
    "mkt_corning": {
        "table_name": "mkt_corning",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"  # KPIs as columns
    },
    "mkt_ericsson": {
        "table_name": "mkt_ericsson",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    "mkt_samsung": {
        "table_name": "mkt_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    "gnb_corning": {
        "table_name": "gnb_corning",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    "gnb_ericsson": {
        "table_name": "gnb_ericsson",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    "sector_corning": {
        "table_name": "sector_corning",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    "sector_ericsson": {
        "table_name": "sector_ericsson",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    "sector_samsung": {
        "table_name": "sector_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    "carrier_corning": {
        "table_name": "carrier_corning",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    "carrier_ericsson": {
        "table_name": "carrier_ericsson",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    "carrier_samsung": {
        "table_name": "carrier_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    "du_corning": {
        "table_name": "du_corning",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    "du_samsung": {
        "table_name": "du_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    "acpf_gnb_samsung": {
        "table_name": "acpf_gnb_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    "acpf_vcu_samsung": {
        "table_name": "acpf_vcu_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    "aupf_gnb_samsung": {
        "table_name": "aupf_gnb_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    "aupf_vcu_samsung": {
        "table_name": "aupf_vcu_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    "aupf_vm_samsung": {
        "table_name": "aupf_vm_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp",
        "table_type": "horizontal"
    },
    # NEW: 19th table - Vertical structure (KPIs as rows)
    "ruleexecutionresults": {
        "table_name": "rule_execution_results",
        "entity_columns": ["resultsid", "id"],  # Only these entity columns
        "timestamp_column": "timestamp",
        "table_type": "vertical",  # KPIs as rows
        "kpi_name_column": "udc_config_name",  # Column containing KPI names
        "kpi_value_column": "udc_config_value"  # Column containing KPI values
    }
}


def parse_search_by_properties(search_by_properties: str) -> Dict[str, List[str]]:
    """
    Parse searchByProperties parameter
    Format: resource.column==value or resource.column==value1,value2
    
    Examples:
    - resource.id==143
    - resource.market==US,EU
    - resource.id==143&resource.market==US
    """
    entity_filters = {}
    
    if not search_by_properties:
        return entity_filters
    
    # Split by & to handle multiple conditions
    conditions = search_by_properties.split("&")
    
    for condition in conditions:
        condition = condition.strip()
        if not condition:
            continue
            
        # Match pattern: resource.column==value
        match = re.match(r'resource\.(\w+)==(.+)', condition)
        if match:
            column = match.group(1)
            values_str = match.group(2)
            
            # Handle multiple values separated by comma
            values = [v.strip() for v in values_str.split(",") if v.strip()]
            if values:
                entity_filters[column] = values
        else:
            logger.warning(f"Invalid searchByProperties format: {condition}")
    
    return entity_filters


def build_vertical_table_query(
    table_name: str,
    metrics: List[str],
    entity_filters: Dict[str, List[str]],
    start_time: Optional[str],
    end_time: Optional[str],
    kpi_name_column: str,
    kpi_value_column: str,
    timestamp_column: str,
    properties: Optional[List[str]] = None,
    granularity: Optional[str] = None,
    order_by: Optional[str] = None,
    limit: Optional[int] = None
) -> tuple:
    """
    Build query for vertical tables (like rule_execution_results)
    where KPIs are stored as rows instead of columns
    """
    query_parts = []
    query_params = []
    db_type = Config.DB_TYPE
    unit = None  # Initialize unit variable for use in GROUP BY
    
    # SELECT clause - always include timestamp, id, kpi name and value
    select_columns = [timestamp_column, "id", kpi_name_column, kpi_value_column]
    
    # Add resultsid if requested in properties
    if properties and "resultsid" in properties:
        select_columns.insert(0, "resultsid")
    
    # Handle aggregation by granularity
    if granularity:
        # Extract time window from granularity (e.g., "1-hour" -> 1, "hour")
        granularity_match = re.match(r'(\d+)-(minute|hour|day)', granularity)
        if granularity_match:
            amount = granularity_match.group(1)
            unit = granularity_match.group(2)
            
            # Build timestamp truncation based on database type
            if db_type == "postgresql":
                # PostgreSQL: timestamp column is stored as character varying with Unix milliseconds
                # Convert to timestamp, truncate, then convert back
                timestamp_expr = f"CAST(EXTRACT(EPOCH FROM DATE_TRUNC('{unit}', TO_TIMESTAMP(CAST({timestamp_column} AS BIGINT) / 1000.0))) * 1000 AS BIGINT)::text"
            else:
                # Presto: timestamp column is TIMESTAMP type
                timestamp_expr = f"date_trunc('{unit}', {timestamp_column})"
            
            # Group by time window
            query_parts.append(f"""
                SELECT 
                    {timestamp_expr} as {timestamp_column},
                    id,
                    {kpi_name_column},
                    AVG({kpi_value_column}) as {kpi_value_column}
                FROM {table_name}
            """)
        else:
            query_parts.append(f"SELECT {', '.join(select_columns)} FROM {table_name}")
    else:
        query_parts.append(f"SELECT {', '.join(select_columns)} FROM {table_name}")
    
    # WHERE clause
    where_conditions = []
    
    # Filter by KPI names (metrics)
    if metrics:
        escaped_metrics = ["'" + m.replace("'", "''") + "'" for m in metrics]
        metrics_list = ', '.join(escaped_metrics)
        where_conditions.append(f"{kpi_name_column} IN ({metrics_list})")
    
    # Filter by entity columns
    for column, values in entity_filters.items():
        if values:
            escaped_values = ["'" + str(v).replace("'", "''") + "'" for v in values]
            values_list = ', '.join(escaped_values)
            where_conditions.append(f"{column} IN ({values_list})")
    
    # Filter by time range - handle differently for PostgreSQL vs Presto
    if start_time:
        if db_type == "postgresql":
            # PostgreSQL: timestamp is stored as character varying with Unix milliseconds
            where_conditions.append(f"{timestamp_column} >= '{int(start_time)}'")
        else:
            # Presto: timestamp is TIMESTAMP type, use from_unixtime
            where_conditions.append(f"{timestamp_column} >= from_unixtime({int(start_time)} / 1000.0)")
    
    if end_time:
        if db_type == "postgresql":
            # PostgreSQL: timestamp is stored as character varying with Unix milliseconds
            where_conditions.append(f"{timestamp_column} <= '{int(end_time)}'")
        else:
            # Presto: timestamp is TIMESTAMP type, use from_unixtime
            where_conditions.append(f"{timestamp_column} <= from_unixtime({int(end_time)} / 1000.0)")
    
    if where_conditions:
        query_parts.append("WHERE " + " AND ".join(where_conditions))
    
    # GROUP BY for aggregation
    if granularity and unit:
        if db_type == "postgresql":
            timestamp_expr = f"CAST(EXTRACT(EPOCH FROM DATE_TRUNC('{unit}', TO_TIMESTAMP(CAST({timestamp_column} AS BIGINT) / 1000.0))) * 1000 AS BIGINT)::text"
        else:
            timestamp_expr = f"date_trunc('{unit}', {timestamp_column})"
        query_parts.append(f"GROUP BY {timestamp_expr}, id, {kpi_name_column}")
    
    # ORDER BY
    if order_by:
        if order_by in ['time', 'time+']:
            query_parts.append(f"ORDER BY {timestamp_column} ASC")
        elif order_by == 'time-':
            query_parts.append(f"ORDER BY {timestamp_column} DESC")
    else:
        # Default ordering
        query_parts.append(f"ORDER BY {timestamp_column} DESC")
    
    if limit:
        query_parts.append(f"LIMIT {limit}")

    query = '\n'.join(query_parts)
    logger.debug(f"Vertical table query: {query}")
    return query, []


def transform_vertical_to_horizontal(results: List[Dict], metrics: List[str]) -> List[Dict]:
    """
    Transform vertical table results to horizontal format
    
    Input (vertical):
    [
        {"timestamp": "2024-01-01", "id": "123", "udc_config_name": "kpi1", "udc_config_value": 10},
        {"timestamp": "2024-01-01", "id": "123", "udc_config_name": "kpi2", "udc_config_value": 20},
    ]
    
    Output (horizontal):
    [
        {"timestamp": "2024-01-01", "id": "123", "kpi1": 10, "kpi2": 20}
    ]
    """
    if not results:
        return []
    
    # Group by timestamp and id
    grouped = {}
    
    for row in results:
        timestamp = row.get('timestamp')
        resource_id = row.get('id')
        kpi_name = row.get('udc_config_name')
        kpi_value = row.get('udc_config_value')
        resultsid = row.get('resultsid')
        
        # Create key for grouping
        key = (timestamp, resource_id)
        
        if key not in grouped:
            grouped[key] = {
                'timestamp': timestamp,
                'id': resource_id
            }
            if resultsid is not None:
                grouped[key]['resultsid'] = resultsid
        
        # Add KPI value
        if kpi_name:
            grouped[key][kpi_name] = kpi_value
    
    return list(grouped.values())


def get_csv_column_name(table_name: str, watsonx_column_name: str) -> str:
    """
    Get the CSV column name for a given WatsonX column name.
    Uses the transformer to map lowercase WatsonX names to original CSV names.
    
    Args:
        table_name: The table name (e.g., 'mkt_ericsson')
        watsonx_column_name: Lowercase WatsonX column name (e.g., 'ranmarket_5gnr_dl_mac_volume_mb')
    
    Returns:
        CSV column name (e.g., 'RANMarket_5GNR_DL_MAC_Volume_MB')
    """
    transformer = get_transformer()
    
    # Create a dummy result with just this column
    dummy_result = {watsonx_column_name: "dummy_value"}
    
    # Transform with map_columns=True to get CSV column name
    transformed = transformer.transform_results(
        results=[dummy_result],
        table_name=table_name,
        convert_timestamp=False,
        map_columns=True
    )
    
    # Find the mapped column name (it should be the only non-original column)
    if transformed and len(transformed) > 0:
        for key in transformed[0].keys():
            if key != watsonx_column_name:
                logger.info(f"Mapped '{watsonx_column_name}' -> '{key}' for table '{table_name}'")
                return key
    
    # If no mapping found, return the original name
    logger.info(f"No mapping found for '{watsonx_column_name}' in table '{table_name}'")
    return watsonx_column_name


def transform_to_flat_format(
    results: List[Dict], 
    metrics: List[str], 
    table_name: str, 
    timestamp_column: str = "timestamp", 
    resource_column: str = "id",
    map_to_csv_names: bool = False
) -> List[Dict]:
    """
    Transform horizontal table results to flat array format matching curl_output.txt
    
    Args:
        results: Query results with lowercase WatsonX column names
        metrics: List of lowercase WatsonX metric names
        table_name: Table name for CSV mapping
        timestamp_column: Name of timestamp column
        resource_column: Name of resource column
        map_to_csv_names: If True, map metric names to original CSV column names
    
    Input (horizontal):
    [
        {"timestamp": "1767052800000", "id": "0006355_0001_0005_10", "metric1": "0.0", "metric2": "1.5"},
        {"timestamp": "1767056400000", "id": "0006355_0001_0005_10", "metric1": "0.1", "metric2": "1.6"},
    ]
    
    Output (flat with CSV names):
    [
        {"metric": "Metric1_CSV_Name", "timestamp": "1767052800000", "value": "0.0", "tags": {"resource": "0006355_0001_0005_10"}},
        {"metric": "Metric1_CSV_Name", "timestamp": "1767056400000", "value": "0.1", "tags": {"resource": "0006355_0001_0005_10"}},
        {"metric": "Metric2_CSV_Name", "timestamp": "1767052800000", "value": "1.5", "tags": {"resource": "0006355_0001_0005_10"}},
        {"metric": "Metric2_CSV_Name", "timestamp": "1767056400000", "value": "1.6", "tags": {"resource": "0006355_0001_0005_10"}},
    ]
    """
    if not results:
        return []
    
    flat_results = []
    
    # Build metric name mapping if needed
    metric_name_map = {}
    if map_to_csv_names:
        for metric in metrics:
            csv_name = get_csv_column_name(table_name, metric)
            metric_name_map[metric] = csv_name
            logger.debug(f"Mapped {metric} -> {csv_name}")
    
    for row in results:
        timestamp = row.get(timestamp_column)
        resource = row.get(resource_column)
        
        # Convert timestamp to string (Unix milliseconds format)
        if timestamp is not None:
            # If timestamp is a datetime object, convert to Unix milliseconds
            if isinstance(timestamp, datetime):
                timestamp = str(int(timestamp.timestamp() * 1000))
            elif isinstance(timestamp, (int, float)):
                # Already a number, convert to string
                timestamp = str(int(timestamp))
            else:
                # Already a string, use as-is (should be Unix milliseconds)
                timestamp = str(timestamp)
        else:
            timestamp = ""
        
        # For each metric, create a separate entry
        for metric in metrics:
            if metric in row:
                value = row[metric]
                # Convert value to string
                if value is None:
                    value = "0.0"
                else:
                    value = str(value)
                
                # Use mapped CSV name if enabled, otherwise use WatsonX name
                output_metric_name = metric_name_map.get(metric, metric) if map_to_csv_names else metric
                
                flat_results.append({
                    "metric": output_metric_name,
                    "timestamp": timestamp,
                    "value": value,
                    "tags": {
                        "resource": str(resource) if resource is not None else ""
                    }
                })
    
    return flat_results


def get_table_name_from_metrics(metrics: List[str]) -> Optional[str]:
    """
    Query metricsandtables table to find the table name based on metrics.
    
    Returns the first table name found, or None if no table is found.
    (Kept for backward compatibility)
    """
    metrics_by_table = get_metrics_by_table(metrics)
    if not metrics_by_table:
        return None
    # Return the first table name found
    return list(metrics_by_table.keys())[0]


def execute_query_for_table(
    table_name_from_db: str,
    table_metrics: List[str],
    entity_filters: Dict[str, List[str]],
    validated_start: Optional[str],
    validated_end: Optional[str],
    properties_list: Optional[List[str]],
    validated_granularity: Optional[str],
    parsed_order_by: Optional[str],
    limit: Optional[int]
) -> List[Dict]:
    """
    Execute a query for a specific table with specific metrics.
    
    Returns the raw query results (before transformation to flat format).
    """
    # Validate and get table configuration
    table_lower = table_name_from_db.lower()
    if table_lower not in TABLE_CONFIG:
        raise ValidationError(
            f"Table '{table_name_from_db}' found in metricsandtables is not supported. Supported tables: {', '.join(TABLE_CONFIG.keys())}"
        )
    
    table_config = TABLE_CONFIG[table_lower]
    table_name = table_config["table_name"]
    entity_columns = table_config["entity_columns"]
    timestamp_column = table_config["timestamp_column"]
    table_type = table_config.get("table_type", "horizontal")
    
    logger.info(f"Executing query for {table_name} timeseries (type: {table_type}) with metrics: {table_metrics}")
    
    # Build and execute query based on table type
    if table_type == "vertical":
        # Vertical table (rule_execution_results)
        kpi_name_column = table_config["kpi_name_column"]
        kpi_value_column = table_config["kpi_value_column"]
        
        query, query_params = build_vertical_table_query(
            table_name=table_name,
            metrics=table_metrics,
            entity_filters=entity_filters,
            start_time=validated_start,
            end_time=validated_end,
            kpi_name_column=kpi_name_column,
            kpi_value_column=kpi_value_column,
            timestamp_column=timestamp_column,
            properties=properties_list,
            granularity=validated_granularity,
            order_by=parsed_order_by,
            limit=limit
        )
        
        logger.debug(f"Vertical table query: {query}\n{query_params}")
        
        try:
            results = db_connection.execute_query(query, query_params)
            
            # Transform vertical results to horizontal format
            results = transform_vertical_to_horizontal(results, table_metrics)
            
        except Exception as e:
            logger.error(f"Database query failed: {e}")
            raise DatabaseError(f"Failed to execute query: {str(e)}")
    
    else:
        # Horizontal table (standard 18 tables)
        query_builder = QueryBuilder(
            table_name=table_name,
            entity_columns=entity_columns,
            timestamp_column=timestamp_column
        )
        
        # Build query
        query, query_params = query_builder.build_query(
            metrics=table_metrics,
            entity_filters=entity_filters,
            start_time=validated_start,
            end_time=validated_end,
            properties=properties_list,
            granularity=validated_granularity,
            order_by=parsed_order_by,
            limit=limit
        )
        
        logger.debug(f"Horizontal table query: {query}\n{query_params}")
        
        # Execute query
        try:
            results = db_connection.execute_query(query, query_params)
        except Exception as e:
            logger.error(f"Database query failed: {e}")
            raise DatabaseError(f"Failed to execute query: {str(e)}")
    
    return results


def get_metrics_by_table(metrics: List[str]) -> Dict[str, List[str]]:
    """
    Query metricsandtables table to find which metrics belong to which tables.
    
    Returns a dictionary mapping table names to lists of metrics that belong to each table.
    
    Example:
        {
            "mkt_ericsson": ["metric1", "metric2"],
            "carrier_ericsson": ["metric3"]
        }
    """
    if not metrics:
        return {}
    
    try:
        # Build query: SELECT tablename, metricname FROM metricsandtables WHERE metricname IN ({metrics list})
        escaped_metrics = ["'" + m.replace("'", "''") + "'" for m in metrics]
        metrics_list = ', '.join(escaped_metrics)
        query = f"SELECT tablename, metricname FROM metricsandtables WHERE metricname IN ({metrics_list})"
        
        logger.debug(f"Querying metricsandtables: {query}")
        
        results = db_connection.execute_query(query)
        
        if not results or len(results) == 0:
            logger.warning(f"No table found for metrics: {metrics}")
            return {}
        
        # Group metrics by table name
        metrics_by_table = {}
        for row in results:
            table_name = row.get('tablename')
            metric_name = row.get('metricname')
            
            if table_name and metric_name:
                if table_name not in metrics_by_table:
                    metrics_by_table[table_name] = []
                # Only add if the metric is in our requested list (case-insensitive check)
                if metric_name in metrics:
                    metrics_by_table[table_name].append(metric_name)
        
        # Remove duplicates and log the mapping
        for table_name in metrics_by_table:
            metrics_by_table[table_name] = list(set(metrics_by_table[table_name]))
            logger.info(f"Found table '{table_name}' with metrics: {metrics_by_table[table_name]}")
        
        return metrics_by_table
            
    except Exception as e:
        logger.error(f"Error querying metricsandtables: {e}")
        raise DatabaseError(f"Failed to query metricsandtables table: {str(e)}")


@router.get("/timeserieswithtablename", response_model=Union[Dict[str, Any], List[Dict[str, Any]]])
async def get_timeseries(
    table: str = Query(..., description="Table name (e.g., 'mkt_corning', 'rule_execution_results')"),
    metrics: str = Query(..., description="Comma-separated list of KPI/metric names to retrieve"),
    start: Optional[str] = Query(None, description="Start timestamp (Unix timestamp in milliseconds)"),
    end: Optional[str] = Query(None, description="End timestamp (Unix timestamp in milliseconds)"),
    resource: Optional[List[str]] = Query(None, description="Resource ID(s) - can be repeated multiple times"),
    searchByProperties: Optional[str] = Query(None, description="Entity filters in format: resource.column==value (e.g., resource.id==143)"),
    properties: Optional[str] = Query(None, description="Comma-separated list of entity columns to return"),
    requestgranularity: Optional[str] = Query(None, description="Time granularity for aggregation (e.g., '1-hour', '30-minute', '1-day')"),
    orderBy: Optional[str] = Query(None, description="Order by timestamp: 'time' or 'time+' for ascending, 'time-' for descending, or 'resource,time+' for resource then time"),
    limit: Optional[int] = Query(1000, description="Maximum number of rows to return (default: 1000, max: 10000)", ge=1, le=10000),
    format: Optional[str] = Query(None, description="Response format: 'json' for flat array format matching curl_output.txt"),
    bulk: Optional[bool] = Query(None, description="Bulk mode (ignored, kept for compatibility)"),
    flatten: Optional[bool] = Query(None, description="Flatten response (ignored, kept for compatibility)"),
    debug: Optional[bool] = Query(False, description="Include query and debug information in response")
):
    """
    Get timeseries data from specified table
    
    Supports both horizontal tables (18 KPI tables) and vertical tables (rule_execution_results).
    
    Horizontal tables: KPIs are columns (mkt_*, gnb_*, sector_*, carrier_*, du_*, acpf_*, aupf_*)
    Vertical tables: KPIs are rows with udc_config_name and udc_config_value (rule_execution_results)
    
    Example for horizontal table:
    /timeserieswithtablename?table=mkt_corning&metrics=ranmarket_endc_sessions_rn&start=1749992400000&end=1750057199000
    
    Example for vertical table:
    /timeserieswithtablename?table=rule_execution_results&metrics=kpi_name1,kpi_name2&start=1749992400000&end=1750057199000&searchByProperties=resource.id==143
    """
    try:
        # Validate and get table configuration
        table_lower = table.lower()
        if table_lower not in TABLE_CONFIG:
            raise ValidationError(
                f"Table '{table}' is not supported. Supported tables: {', '.join(TABLE_CONFIG.keys())}"
            )
        
        table_config = TABLE_CONFIG[table_lower]
        table_name = table_config["table_name"]
        entity_columns = table_config["entity_columns"]
        timestamp_column = table_config["timestamp_column"]
        table_type = table_config.get("table_type", "horizontal")
        
        # Parse and validate metrics
        metrics_list = [m.strip() for m in metrics.split(",") if m.strip()]
        validated_metrics = validate_metrics(metrics_list)
        
        # Parse and validate timestamps (Unix timestamp in milliseconds)
        validated_start = validate_timestamp(start) if start else None
        validated_end = validate_timestamp(end) if end else None
        
        # Validate granularity
        validated_granularity = validate_granularity(requestgranularity)
        
        # Parse searchByProperties
        entity_filters = {}
        if searchByProperties:
            parsed_filters = parse_search_by_properties(searchByProperties)
            # Validate that all columns in searchByProperties are valid entity columns
            for col, values in parsed_filters.items():
                if col not in entity_columns:
                    raise ValidationError(
                        f"Invalid column '{col}' in searchByProperties. Valid columns: {', '.join(entity_columns)}"
                    )
            entity_filters = parsed_filters
        
        # Handle resource parameter (maps to 'id' column)
        if resource:
            if 'id' not in entity_filters:
                entity_filters['id'] = []
            entity_filters['id'].extend(resource)
        
        validated_entity_filters = validate_entity_filters(entity_filters)
        
        # Parse orderBy - handle 'resource,time+' format
        parsed_order_by = orderBy
        if orderBy and ',' in orderBy:
            # Format like 'resource,time+' - extract just the time part
            parts = [p.strip() for p in orderBy.split(',')]
            # Find the time-related part
            for part in parts:
                if part.startswith('time') or part == 'time+' or part == 'time-':
                    parsed_order_by = part
                    break
            # If no time part found, use the last part
            if parsed_order_by == orderBy:
                parsed_order_by = parts[-1] if parts else None
        
        # Parse properties (entity columns to return)
        properties_list = None
        if properties:
            properties_list = [p.strip() for p in properties.split(",") if p.strip()]
            # Validate each property
            for prop in properties_list:
                if prop not in entity_columns:
                    raise ValidationError(
                        f"Invalid property: {prop}. Valid properties are: {', '.join(entity_columns)}"
                    )
        
        logger.info(f"Executing query for {table_name} timeseries (type: {table_type})")
        
        # Build and execute query based on table type
        if table_type == "vertical":
            # Vertical table (rule_execution_results)
            kpi_name_column = table_config["kpi_name_column"]
            kpi_value_column = table_config["kpi_value_column"]
            
            query, query_params = build_vertical_table_query(
                table_name=table_name,
                metrics=validated_metrics,
                entity_filters=validated_entity_filters,
                start_time=validated_start,
                end_time=validated_end,
                kpi_name_column=kpi_name_column,
                kpi_value_column=kpi_value_column,
                timestamp_column=timestamp_column,
                properties=properties_list,
                granularity=validated_granularity,
                order_by=parsed_order_by,
                limit=limit
            )
            
            logger.info(f"Vertical table query: {query}\n{query_params}")
            
            try:
                results = db_connection.execute_query(query, query_params)
                
                # Transform vertical results to horizontal format
                results = transform_vertical_to_horizontal(results, validated_metrics)
                
            except Exception as e:
                logger.error(f"Database query failed: {e}")
                raise DatabaseError(f"Failed to execute query: {str(e)}")
        
        else:
            # Horizontal table (standard 18 tables)
            query_builder = QueryBuilder(
                table_name=table_name,
                entity_columns=entity_columns,
                timestamp_column=timestamp_column
            )
            
            # Build query
            query, query_params = query_builder.build_query(
                metrics=validated_metrics,
                entity_filters=validated_entity_filters,
                start_time=validated_start,
                end_time=validated_end,
                properties=properties_list,
                granularity=validated_granularity,
                order_by=parsed_order_by,
                limit=limit
            )
            
            logger.info(f"Horizontal table query: {query}\n{query_params}")
            
            # Execute query
            try:
                results = db_connection.execute_query(query, query_params)
            except Exception as e:
                logger.error(f"Database query failed: {e}")
                raise DatabaseError(f"Failed to execute query: {str(e)}")       
        
        # Always transform to flat array format matching curl_output.txt
        # Convert timestamps to epoch milliseconds (keep lowercase column names for now)
        transformer = get_transformer()
        results = transformer.transform_results(
            results=results,
            table_name=table_lower,
            convert_timestamp=True,
            map_columns=False  # Keep lowercase WatsonX names so transform_to_flat_format can find metrics
        )
        
        # Transform to flat array format AND map metric names to CSV column names
        flat_results = transform_to_flat_format(
            results, 
            validated_metrics, 
            table_lower, 
            timestamp_column, 
            "id",
            map_to_csv_names=True  # Enable CSV column name mapping
        )
        
        # Sort by resource then time if orderBy contains resource
        if orderBy and 'resource' in orderBy.lower():
            # Sort by resource (id), then by timestamp
            flat_results.sort(key=lambda x: (x.get('tags', {}).get('resource', ''), x.get('timestamp', '')))
        elif parsed_order_by:
            # Sort by timestamp only
            reverse = parsed_order_by == 'time-'
            flat_results.sort(key=lambda x: x.get('timestamp', ''), reverse=reverse)
        
        return flat_results
        
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except DatabaseError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.exception("Unexpected error in get_timeseries")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


@router.get("/timeseries", response_model=Union[Dict[str, Any], List[Dict[str, Any]]])
async def get_timeseries_no_table(
    metrics: str = Query(..., description="Comma-separated list of KPI/metric names to retrieve"),
    start: Optional[str] = Query(None, description="Start timestamp (Unix timestamp in milliseconds)"),
    end: Optional[str] = Query(None, description="End timestamp (Unix timestamp in milliseconds)"),
    resource: Optional[List[str]] = Query(None, description="Resource ID(s) - can be repeated multiple times"),
    searchByProperties: Optional[str] = Query(None, description="Entity filters in format: resource.column==value (e.g., resource.id==143)"),
    properties: Optional[str] = Query(None, description="Comma-separated list of entity columns to return"),
    requestgranularity: Optional[str] = Query(None, description="Time granularity for aggregation (e.g., '1-hour', '30-minute', '1-day')"),
    orderBy: Optional[str] = Query(None, description="Order by timestamp: 'time' or 'time+' for ascending, 'time-' for descending, or 'resource,time+' for resource then time"),
    limit: Optional[int] = Query(1000, description="Maximum number of rows to return (default: 1000, max: 10000)", ge=1, le=10000),
    format: Optional[str] = Query(None, description="Response format: 'json' for flat array format matching curl_output.txt"),
    bulk: Optional[bool] = Query(None, description="Bulk mode (ignored, kept for compatibility)"),
    flatten: Optional[bool] = Query(None, description="Flatten response (ignored, kept for compatibility)"),
    debug: Optional[bool] = Query(False, description="Include query and debug information in response")
):
    """
    Get timeseries data without specifying table name.
    
    The table name is automatically determined by querying the metricsandtables table
    based on the provided metrics.
    
    Supports both horizontal tables (18 KPI tables) and vertical tables (rule_execution_results).
    
    Example:
    /timeseries?metrics=ranmarket_endc_sessions_rn&start=1749992400000&end=1750057199000&searchByProperties=resource.id==143
    
    If no table is found for the given metrics, returns empty results.
    """
    try:
        # Parse and validate metrics
        metrics_list = [m.strip() for m in metrics.split(",") if m.strip()]
        validated_metrics = validate_metrics(metrics_list)
        
        # Get metrics grouped by table from metricsandtables
        metrics_by_table = get_metrics_by_table(validated_metrics)
        
        if not metrics_by_table:
            # No table found for the given metrics, return empty array in flat format
            logger.info(f"No table found for metrics: {validated_metrics}, returning empty results")
            return []
        
        # Parse and validate timestamps (Unix timestamp in milliseconds)
        validated_start = validate_timestamp(start) if start else None
        validated_end = validate_timestamp(end) if end else None
        
        # Validate granularity
        validated_granularity = validate_granularity(requestgranularity)
        
        # Parse orderBy - handle 'resource,time+' format
        parsed_order_by = orderBy
        if orderBy and ',' in orderBy:
            # Format like 'resource,time+' - extract just the time part
            parts = [p.strip() for p in orderBy.split(',')]
            # Find the time-related part
            for part in parts:
                if part.startswith('time') or part == 'time+' or part == 'time-':
                    parsed_order_by = part
                    break
            # If no time part found, use the last part
            if parsed_order_by == orderBy:
                parsed_order_by = parts[-1] if parts else None
        
        # Parse properties (entity columns to return)
        properties_list = None
        if properties:
            properties_list = [p.strip() for p in properties.split(",") if p.strip()]
        
        # Collect all results from all tables
        all_flat_results = []
        
        # Process each table separately
        for table_name_from_db, table_metrics in metrics_by_table.items():
            # Get table configuration to validate entity columns for searchByProperties
            table_lower = table_name_from_db.lower()
            if table_lower not in TABLE_CONFIG:
                logger.warning(f"Table '{table_name_from_db}' not in TABLE_CONFIG, skipping")
                continue
            
            table_config = TABLE_CONFIG[table_lower]
            entity_columns = table_config["entity_columns"]
            
            # Parse searchByProperties for this table
            entity_filters = {}
            if searchByProperties:
                parsed_filters = parse_search_by_properties(searchByProperties)
                # Validate that all columns in searchByProperties are valid entity columns for this table
                for col, values in parsed_filters.items():
                    if col not in entity_columns:
                        logger.warning(
                            f"Invalid column '{col}' in searchByProperties for table '{table_name_from_db}'. "
                            f"Valid columns: {', '.join(entity_columns)}. Skipping this filter for this table."
                        )
                        continue
                    entity_filters[col] = values
            
            # Handle resource parameter (maps to 'id' column)
            if resource:
                if 'id' not in entity_filters:
                    entity_filters['id'] = []
                entity_filters['id'].extend(resource)
            
            validated_entity_filters = validate_entity_filters(entity_filters)
            
            # Validate properties for this table
            validated_properties_list = properties_list
            if properties_list:
                validated_properties_list = []
                for prop in properties_list:
                    if prop not in entity_columns:
                        logger.warning(
                            f"Invalid property: {prop} for table '{table_name_from_db}'. "
                            f"Valid properties are: {', '.join(entity_columns)}. Skipping this property."
                        )
                        continue
                    validated_properties_list.append(prop)
            
            try:
                # Execute query for this table with its specific metrics
                results = execute_query_for_table(
                    table_name_from_db=table_name_from_db,
                    table_metrics=table_metrics,
                    entity_filters=validated_entity_filters,
                    validated_start=validated_start,
                    validated_end=validated_end,
                    properties_list=validated_properties_list if validated_properties_list else None,
                    validated_granularity=validated_granularity,
                    parsed_order_by=parsed_order_by,
                    limit=limit
                )
                
                # Transform results for this table
                transformer = get_transformer()
                results = transformer.transform_results(
                    results=results,
                    table_name=table_lower,
                    convert_timestamp=True,
                    map_columns=False  # Keep lowercase WatsonX names so transform_to_flat_format can find metrics
                )
                
                # Transform to flat array format AND map metric names to CSV column names
                timestamp_column = table_config["timestamp_column"]
                flat_results = transform_to_flat_format(
                    results, 
                    table_metrics,  # Use only the metrics for this table
                    table_lower, 
                    timestamp_column, 
                    "id",
                    map_to_csv_names=True  # Enable CSV column name mapping
                )
                
                # Add to combined results
                all_flat_results.extend(flat_results)
                
            except Exception as e:
                logger.error(f"Error processing table '{table_name_from_db}': {e}")
                # Continue with other tables even if one fails
                continue
        
        # Sort combined results by resource then time if orderBy contains resource
        if orderBy and 'resource' in orderBy.lower():
            # Sort by resource (id), then by timestamp
            all_flat_results.sort(key=lambda x: (x.get('tags', {}).get('resource', ''), x.get('timestamp', '')))
        elif parsed_order_by:
            # Sort by timestamp only
            reverse = parsed_order_by == 'time-'
            all_flat_results.sort(key=lambda x: x.get('timestamp', ''), reverse=reverse)
        
        return all_flat_results
        
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except DatabaseError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.exception("Unexpected error in get_timeseries_no_table")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        test_query = "SELECT 1 as test"
        db_connection.execute_query(test_query)
        return {
            "status": "healthy",
            "database": Config.DB_TYPE,
            "database_connected": True
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "database": Config.DB_TYPE,
                "database_connected": False,
                "error": str(e)
            }
        )
