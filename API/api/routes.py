"""
API routes for KPI Timeseries endpoints
"""
import logging
import re
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse

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
# All 18 tables with their entity columns and timestamp column
TABLE_CONFIG = {
    "mkt_corning": {
        "table_name": "mkt_corning",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "mkt_ericsson": {
        "table_name": "mkt_ericsson",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "mkt_samsung": {
        "table_name": "mkt_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "gnb_corning": {
        "table_name": "gnb_corning",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "gnb_ericsson": {
        "table_name": "gnb_ericsson",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "gnb_samsung": {
        "table_name": "gnb_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "sector_corning": {
        "table_name": "sector_corning",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "sector_ericsson": {
        "table_name": "sector_ericsson",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "sector_samsung": {
        "table_name": "sector_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "carrier_corning": {
        "table_name": "carrier_corning",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "carrier_ericsson": {
        "table_name": "carrier_ericsson",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "carrier_samsung": {
        "table_name": "carrier_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "du_corning": {
        "table_name": "du_corning",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "du_samsung": {
        "table_name": "du_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "acpf_gnb_samsung": {
        "table_name": "acpf_gnb_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "acpf_vcu_samsung": {
        "table_name": "acpf_vcu_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "aupf_gnb_samsung": {
        "table_name": "aupf_gnb_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "aupf_vcu_samsung": {
        "table_name": "aupf_vcu_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
    },
    "aupf_vm_samsung": {
        "table_name": "aupf_vm_samsung",
        "entity_columns": ["market", "region", "vcptype", "technology", "datacenter", "site", "id"],
        "timestamp_column": "timestamp"
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


@router.get("/timeseries", response_model=Dict[str, Any])
async def get_timeseries(
    table: str = Query(..., description="Table name (e.g., 'mkt_corning')"),
    metrics: str = Query(..., description="Comma-separated list of KPI/metric names to retrieve"),
    start: Optional[str] = Query(None, description="Start timestamp (Unix timestamp in milliseconds)"),
    end: Optional[str] = Query(None, description="End timestamp (Unix timestamp in milliseconds)"),
    searchByProperties: Optional[str] = Query(None, description="Entity filters in format: resource.column==value (e.g., resource.id==143)"),
    properties: Optional[str] = Query(None, description="Comma-separated list of entity columns to return"),
    requestgranularity: Optional[str] = Query(None, description="Time granularity for aggregation (e.g., '1-hour', '30-minute', '1-day')"),
    orderBy: Optional[str] = Query(None, description="Order by timestamp: 'time' or 'time+' for ascending, 'time-' for descending"),
    flatten: Optional[bool] = Query(None, description="Flatten response (ignored, kept for compatibility)"),
    debug: Optional[bool] = Query(False, description="Include query and debug information in response")
):
    """
    Get timeseries data from specified table
    
    Supports filtering by entity columns, time range, and selecting specific KPIs.
    Can aggregate data by time granularity.
    
    Example:
    /timeseries?table=mkt_corning&metrics=ranmarket_endc_sessions_rn,ranmarket_intra_cu_ho_attempts_number&start=1749992400000&end=1750057199000&searchByProperties=resource.id==143&properties=type,id&requestgranularity=1-hour
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
        
        validated_entity_filters = validate_entity_filters(entity_filters)
        
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
        
        # Initialize query builder
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
            order_by=orderBy
        )
        
        logger.info(f"Executing query for {table_name} timeseries")
        logger.debug(f"Query: {query}")
        
        # Execute query
        try:
            results = db_connection.execute_query(query, query_params)
        except Exception as e:
            logger.error(f"Database query failed: {e}")
            raise DatabaseError(f"Failed to execute query: {str(e)}")
        
        # Format response
        response = {
            "table": table_lower,  # Return the table name as provided (lowercase)
            "metrics": validated_metrics,
            "count": len(results),
            "data": results
        }
        
        if validated_start:
            response["start"] = validated_start
        if validated_end:
            response["end"] = validated_end
        if validated_granularity:
            response["granularity"] = validated_granularity
        
        # Add debug information if requested
        if debug:
            response["debug"] = {
                "query": query,
                "query_params": query_params if query_params else [],
                "table_name": table_name,
                "entity_filters": validated_entity_filters,
                "properties": properties_list
            }
        
        return response
        
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except DatabaseError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.exception("Unexpected error in get_timeseries")
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
