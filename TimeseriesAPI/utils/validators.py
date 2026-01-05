"""
Input validation utilities
"""
import re
from typing import List, Optional, Any
from utils.error_handler import ValidationError


def validate_table_name(table_name: str) -> None:
    """Validate table name to prevent SQL injection"""
    if not table_name or not isinstance(table_name, str):
        raise ValidationError("Table name must be a non-empty string")
    
    # Allow alphanumeric and underscore only
    if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
        raise ValidationError("Table name contains invalid characters")


def validate_column_name(column_name: str) -> None:
    """Validate column name to prevent SQL injection"""
    if not column_name or not isinstance(column_name, str):
        raise ValidationError("Column name must be a non-empty string")
    
    # Allow alphanumeric and underscore only
    if not re.match(r'^[a-zA-Z0-9_]+$', column_name):
        raise ValidationError("Column name contains invalid characters")


def validate_metrics(metrics: Optional[List[str]]) -> List[str]:
    """Validate and sanitize metrics list"""
    if not metrics:
        raise ValidationError("At least one metric must be specified")
    
    validated_metrics = []
    for metric in metrics:
        if not metric or not isinstance(metric, str):
            continue
        # Remove any whitespace
        metric = metric.strip()
        if metric:
            validate_column_name(metric)
            validated_metrics.append(metric)
    
    if not validated_metrics:
        raise ValidationError("No valid metrics provided")
    
    return validated_metrics


def validate_timestamp(timestamp: Any) -> Any:
    """Validate timestamp format"""
    if timestamp is None:
        return None
    
    # Accept Unix timestamp (int or string representation)
    if isinstance(timestamp, str):
        try:
            # Try to convert to int (Unix timestamp in milliseconds)
            return int(timestamp)
        except ValueError:
            # Assume ISO format string - basic validation
            if not re.match(r'^\d{4}-\d{2}-\d{2}', timestamp):
                raise ValidationError(f"Invalid timestamp format: {timestamp}")
            return timestamp
    elif isinstance(timestamp, (int, float)):
        return int(timestamp)
    else:
        raise ValidationError(f"Invalid timestamp type: {type(timestamp)}")


def validate_granularity(granularity: Optional[str]) -> Optional[str]:
    """Validate time granularity format"""
    if granularity is None:
        return None
    
    if not isinstance(granularity, str):
        raise ValidationError("Granularity must be a string")
    
    # Expected format: N-unit (e.g., "1-hour", "30-minute", "1-day")
    pattern = r'^\d+-(hour|minute|day|hours|minutes|days)$'
    if not re.match(pattern, granularity.lower()):
        raise ValidationError(
            f"Invalid granularity format: {granularity}. Expected format: 'N-unit' (e.g., '1-hour', '30-minute', '1-day')"
        )
    
    return granularity.lower()


def validate_entity_filters(entity_filters: Optional[dict]) -> dict:
    """Validate entity filter dictionary"""
    if entity_filters is None:
        return {}
    
    if not isinstance(entity_filters, dict):
        raise ValidationError("Entity filters must be a dictionary")
    
    validated_filters = {}
    for key, value in entity_filters.items():
        validate_column_name(key)
        if isinstance(value, str):
            validated_filters[key] = [value]
        elif isinstance(value, list):
            validated_filters[key] = [str(v) for v in value if v]
        else:
            validated_filters[key] = [str(value)]
    
    return validated_filters

