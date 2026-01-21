"""
Input validation helpers for UI Timeseries API.
"""
import re
from typing import List, Optional, Any

from utils.error_handler import ValidationError

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_.]+$")


def validate_metric_name(metric: str) -> str:
    if not metric or not isinstance(metric, str):
        raise ValidationError("Metric name must be a non-empty string")
    metric = metric.strip()
    if not metric:
        raise ValidationError("Metric name must be a non-empty string")
    if not _IDENTIFIER_PATTERN.match(metric):
        raise ValidationError(f"Metric name contains invalid characters: {metric}")
    return metric


def validate_metrics(metrics: Optional[List[str]]) -> List[str]:
    if not metrics:
        raise ValidationError("At least one metric must be specified")
    validated = []
    for metric in metrics:
        validated.append(validate_metric_name(metric))
    return validated


def validate_timestamp(timestamp: Any) -> Optional[int]:
    if timestamp is None:
        return None
    if isinstance(timestamp, str):
        timestamp = timestamp.strip()
        if not timestamp:
            return None
        try:
            return int(timestamp)
        except ValueError as exc:
            raise ValidationError(f"Invalid timestamp format: {timestamp}") from exc
    if isinstance(timestamp, (int, float)):
        return int(timestamp)
    raise ValidationError(f"Invalid timestamp type: {type(timestamp)}")
