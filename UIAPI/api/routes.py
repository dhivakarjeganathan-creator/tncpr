"""
API routes for UI timeseries endpoint.
"""
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query

from config import Config
from database.connection import db_connection
from utils.error_handler import ValidationError, DatabaseError
from utils.validators import validate_metrics, validate_timestamp

logger = logging.getLogger(__name__)

router = APIRouter()

SCOPE_VALUE = "ibm-itnm"
TABLE_COLUMNS_CACHE: Dict[str, List[str]] = {}
TABLE_COLUMN_TYPE_CACHE: Dict[Tuple[str, str], Optional[str]] = {}
ENTITY_SEARCH_FIELD_MAP: Dict[str, str] = {
    "id": "entityid",
    "type": "type",
    "market": "market",
    "nfname": "nfname",
    "nftype": "nftype",
    "region": "region",
    "domain": "domain",
    "datacenter": "datacenter",
    "band": "band",
    "transcelltype": "transcelltype",
    "frequency": "frequency",
    "duname": "duname",
    "primarysite_name": "primarysite_name",
}


def quote_identifier(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) + chr(34))}"'


def escape_literal(value: str) -> str:
    return value.replace("'", "''")


def normalize_sort(sort: Optional[str]) -> Optional[Tuple[str, str]]:
    if not sort:
        return None
    sort = sort.strip()
    if not sort:
        return None

    direction = "ASC"
    column = sort
    if sort.startswith("-"):
        direction = "DESC"
        column = sort[1:]
    elif sort.startswith("+"):
        column = sort[1:]

    if column != "timestamp":
        return None
    return column, direction


def to_epoch_millis(timestamp_value: Any) -> Optional[int]:
    if timestamp_value is None:
        return None
    if isinstance(timestamp_value, int):
        return timestamp_value
    if isinstance(timestamp_value, float):
        return int(timestamp_value)
    if isinstance(timestamp_value, datetime):
        return int(timestamp_value.timestamp() * 1000)
    if isinstance(timestamp_value, str):
        timestamp_str = timestamp_value.strip()
        if not timestamp_str:
            return None
        try:
            return int(timestamp_str)
        except ValueError:
            pass
        # Try ISO-8601 with timezone offsets (e.g., 2025-09-27T05:00:00-04:00)
        try:
            dt = datetime.fromisoformat(timestamp_str)
            return int(dt.timestamp() * 1000)
        except ValueError:
            pass
        timestamp_str = timestamp_str.replace("T", " ")
        timestamp_str = timestamp_str.split(" UTC")[0].split(" GMT")[0].split(" EST")[0]
        try:
            dt = datetime.fromisoformat(timestamp_str)
            return int(dt.timestamp() * 1000)
        except ValueError:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(timestamp_str, fmt)
                return int(dt.timestamp() * 1000)
            except ValueError:
                continue
    return None


def format_date_time(epoch_millis: Optional[int]) -> Tuple[str, str]:
    if epoch_millis is None:
        return "", ""
    dt = datetime.utcfromtimestamp(epoch_millis / 1000.0)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%I:%M %p")


def normalize_value(value: Any, force_double: bool) -> Any:
    if not force_double:
        return value
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_properties(properties: Optional[str]) -> List[str]:
    if not properties:
        return []
    return [prop.strip() for prop in properties.split(",") if prop.strip()]


def parse_search_by_properties(search_by: str) -> List[Tuple[str, str]]:
    if not search_by or not search_by.strip():
        raise ValidationError("searchByProperties is required")

    parts = re.split(r"\s+AND\s+", search_by.strip(), flags=re.IGNORECASE)
    parsed: List[Tuple[str, str]] = []
    for part in parts:
        part = part.strip()
        match = re.fullmatch(
            r"resource\.([A-Za-z0-9_]+)\s*==\s*'([^']*)'",
            part,
        )
        if not match:
            raise ValidationError(f"Invalid searchByProperties filter: {part}")
        field = match.group(1).strip().lower()
        value = match.group(2)
        mapped = ENTITY_SEARCH_FIELD_MAP.get(field)
        if not mapped:
            raise ValidationError(f"Unsupported resource field: {match.group(1)}")
        parsed.append((mapped, value))
    return parsed


def resolve_table_columns(table_name: str) -> Dict[str, str]:
    table_columns = get_table_columns(table_name)
    if not table_columns:
        schema = Config.DB_SCHEMA if Config.is_postgresql() else (Config.PRESTO_SCHEMA or Config.DB_NAME)
        raise ValidationError(
            f"Enrichment table '{table_name}' not found in schema '{schema}'"
        )
    return {col.lower(): col for col in table_columns}


def normalize_metric_for_column(metric: str) -> str:
    return metric.replace(".", "_")


def build_metric_lookup(metrics: List[str]) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    for metric in metrics:
        lookup[metric] = metric
        lookup[normalize_metric_for_column(metric)] = metric
    return lookup


def build_in_condition(
    column_sql: str, values: List[str], params: List[Any], db_type: str
) -> str:
    if Config.is_postgresql():
        placeholders = ", ".join(["%s"] * len(values))
        params.extend(values)
        return f"{column_sql} IN ({placeholders})"
    escaped_values = [f"'{escape_literal(str(v))}'" for v in values]
    return f"{column_sql} IN ({', '.join(escaped_values)})"


def build_equals_condition(
    column_sql: str, value: str, params: List[Any], db_type: str
) -> str:
    if Config.is_postgresql():
        params.append(value)
        return f"{column_sql} = %s"
    return f"{column_sql} = '{escape_literal(str(value))}'"


def build_avg_expression(column_sql: str) -> str:
    if Config.is_postgresql():
        return f"AVG(CAST({column_sql} AS DOUBLE PRECISION))"
    return f"AVG(CAST({column_sql} AS DOUBLE))"


def build_time_conditions(
    timestamp_sql: str,
    start: Optional[int],
    end: Optional[int],
    params: List[Any],
    db_type: str,
    timestamp_type: Optional[str] = None,
) -> List[str]:
    conditions = []
    normalized_type = timestamp_type.lower() if timestamp_type else ""
    is_timestamp_type = "timestamp" in normalized_type
    is_text_type = normalized_type in {"text", "character varying", "varchar"}

    if start is not None:
        if Config.is_postgresql():
            if is_timestamp_type:
                conditions.append(f"{timestamp_sql} >= to_timestamp(%s / 1000.0)")
                params.append(start)
            elif is_text_type:
                conditions.append(f"CAST({timestamp_sql} AS TIMESTAMP) >= to_timestamp(%s / 1000.0)")
                params.append(start)
            else:
                conditions.append(f"CAST({timestamp_sql} AS BIGINT) >= %s")
                params.append(start)
        else:
            conditions.append(f"{timestamp_sql} >= from_unixtime({int(start)} / 1000.0)")
    if end is not None:
        if Config.is_postgresql():
            if is_timestamp_type:
                conditions.append(f"{timestamp_sql} <= to_timestamp(%s / 1000.0)")
                params.append(end)
            elif is_text_type:
                conditions.append(f"CAST({timestamp_sql} AS TIMESTAMP) <= to_timestamp(%s / 1000.0)")
                params.append(end)
            else:
                conditions.append(f"CAST({timestamp_sql} AS BIGINT) <= %s")
                params.append(end)
        else:
            conditions.append(f"{timestamp_sql} <= from_unixtime({int(end)} / 1000.0)")
    return conditions


def resolve_table_name(table_name: str) -> Optional[str]:
    table_lower = table_name.lower()
    if table_lower not in Config.ALLOWED_TABLES:
        return None
    if table_lower in {"ruleexecutionresults", "rule_execution_results"}:
        return Config.RULE_EXECUTION_TABLE_NAME or table_lower
    return table_lower


def is_vertical_table(table_name: str) -> bool:
    return table_name.lower() in {"ruleexecutionresults", "rule_execution_results"}


def get_table_columns(table_name: str) -> List[str]:
    cached = TABLE_COLUMNS_CACHE.get(table_name)
    if cached is not None and cached:
        return cached

    if Config.is_postgresql():
        query = (
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s"
        )
        params = [Config.DB_SCHEMA, table_name]
    else:
        schema = Config.PRESTO_SCHEMA or Config.DB_NAME
        query = (
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = '{escape_literal(schema)}' "
            f"AND table_name = '{escape_literal(table_name)}'"
        )
        params = []

    rows = db_connection.execute_query(query, params)
    columns = [row.get("column_name") for row in rows if row.get("column_name")]
    if columns:
        TABLE_COLUMNS_CACHE[table_name] = columns
    return columns


def get_column_type(table_name: str, column_name: str) -> Optional[str]:
    cache_key = (table_name, column_name.lower())
    cached = TABLE_COLUMN_TYPE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    if Config.is_postgresql():
        query = (
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s AND column_name = %s"
        )
        params = [Config.DB_SCHEMA, table_name, column_name]
    else:
        schema = Config.PRESTO_SCHEMA or Config.DB_NAME
        query = (
            "SELECT data_type FROM information_schema.columns "
            f"WHERE table_schema = '{escape_literal(schema)}' "
            f"AND table_name = '{escape_literal(table_name)}' "
            f"AND column_name = '{escape_literal(column_name)}'"
        )
        params = []

    rows = db_connection.execute_query(query, params)
    data_type = rows[0].get("data_type") if rows else None
    TABLE_COLUMN_TYPE_CACHE[cache_key] = data_type
    return data_type


def resolve_enrichment_columns(
    table_name: str, properties: List[str]
) -> Tuple[str, List[str]]:
    table_columns = get_table_columns(table_name)
    if not table_columns:
        schema = Config.DB_SCHEMA if Config.is_postgresql() else (Config.PRESTO_SCHEMA or Config.DB_NAME)
        raise ValidationError(
            f"Enrichment table '{table_name}' not found in schema '{schema}'"
        )
    column_map = {col.lower(): col for col in table_columns}

    entity_column = column_map.get(Config.ENRICHMENT_TABLE_ENTITY_COLUMN.lower())
    if not entity_column:
        entity_column = column_map.get("entityid")
    if not entity_column:
        raise ValidationError(
            f"Enrichment table is missing entity column. Found: {', '.join(table_columns)}"
        )

    resolved_properties = []
    missing = []
    for prop in properties:
        resolved = column_map.get(prop.lower())
        if resolved:
            resolved_properties.append(resolved)
        else:
            missing.append(prop)

    if missing:
        raise ValidationError(
            f"Enrichment properties not found: {', '.join(missing)}"
        )

    return entity_column, resolved_properties


def resolve_enrichment_property_map(
    properties: List[str], column_map: Dict[str, str], entity_column: str
) -> Tuple[List[Tuple[str, str]], List[str]]:
    resolved_properties: List[Tuple[str, str]] = []
    missing: List[str] = []
    for prop in properties:
        prop_lower = prop.lower()
        if prop_lower == "id":
            resolved_properties.append((prop, entity_column))
            continue
        resolved = column_map.get(prop_lower)
        if resolved:
            resolved_properties.append((prop, resolved))
        else:
            missing.append(prop)
    return resolved_properties, missing


def fetch_entity_ids_by_search(
    table_name: str, column_map: Dict[str, str], search_filters: List[Tuple[str, str]]
) -> List[str]:
    entity_column = column_map.get(Config.ENRICHMENT_TABLE_ENTITY_COLUMN.lower())
    if not entity_column:
        entity_column = column_map.get("entityid")
    if not entity_column:
        raise ValidationError("Enrichment table is missing entityid column")

    params: List[Any] = []
    where_conditions: List[str] = []
    for column_name, value in search_filters:
        resolved = column_map.get(column_name.lower())
        if not resolved:
            raise ValidationError(f"Search column not found: {column_name}")
        column_sql = quote_identifier(resolved)
        where_conditions.append(
            build_equals_condition(column_sql, value, params, Config.DB_TYPE)
        )

    where_sql = " AND ".join(where_conditions)
    query = (
        f"SELECT DISTINCT {quote_identifier(entity_column)} AS entityid "
        f"FROM {quote_identifier(table_name)} "
        f"WHERE {where_sql}"
    )

    rows = db_connection.execute_query(query, params)
    return [str(row.get("entityid")) for row in rows if row.get("entityid") is not None]


def fetch_enrichment_values_by_map(
    table_name: str,
    entity_ids: List[str],
    entity_column: str,
    resolved_properties: List[Tuple[str, str]],
) -> Dict[str, Dict[str, Any]]:
    if not entity_ids or not resolved_properties:
        return {}

    params: List[Any] = []
    entity_sql = quote_identifier(entity_column)
    select_columns = [entity_column]
    for _, resolved in resolved_properties:
        if resolved not in select_columns:
            select_columns.append(resolved)

    select_sql = ", ".join([quote_identifier(col) for col in select_columns])
    where_clause = build_in_condition(entity_sql, entity_ids, params, Config.DB_TYPE)

    query = (
        f"SELECT {select_sql} "
        f"FROM {quote_identifier(table_name)} "
        f"WHERE {where_clause}"
    )

    rows = db_connection.execute_query(query, params)

    enrichment: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        entity_value = row.get(entity_column)
        if entity_value is None:
            continue
        entity_key = str(entity_value)
        values: Dict[str, Any] = {}
        for prop, resolved in resolved_properties:
            if prop.lower() == "id":
                values[prop] = entity_key
                continue
            value = row.get(resolved)
            values[prop] = "" if value is None else value
        enrichment[entity_key] = values

    return enrichment


def fetch_enrichment_values(
    entity_ids: List[str], properties: List[str]
) -> Dict[str, Dict[str, Any]]:
    if not entity_ids or not properties:
        return {}

    table_name = Config.ENRICHMENT_TABLE_NAME
    entity_column, resolved_properties = resolve_enrichment_columns(table_name, properties)

    params: List[Any] = []
    entity_sql = quote_identifier(entity_column)
    select_sql = ", ".join([entity_sql] + [quote_identifier(col) for col in resolved_properties])
    where_clause = build_in_condition(entity_sql, entity_ids, params, Config.DB_TYPE)

    query = (
        f"SELECT {select_sql} "
        f"FROM {quote_identifier(table_name)} "
        f"WHERE {where_clause}"
    )

    rows = db_connection.execute_query(query, params)

    enrichment: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        entity_value = row.get(entity_column)
        if entity_value is None:
            continue
        entity_key = str(entity_value)
        values: Dict[str, Any] = {}
        for prop, resolved in zip(properties, resolved_properties):
            value = row.get(resolved)
            values[prop] = "" if value is None else value
        enrichment[entity_key] = values

    return enrichment


def map_metrics_to_columns(table_name: str, metrics: List[str]) -> List[str]:
    columns = get_table_columns(table_name)
    column_map = {col.lower(): col for col in columns}
    missing = []
    mapped = []
    for metric in metrics:
        if metric in columns:
            mapped.append(metric)
        else:
            mapped_col = column_map.get(metric.lower())
            if mapped_col:
                mapped.append(mapped_col)
            else:
                missing.append(metric)
    if missing:
        raise ValidationError(
            f"Metrics not found in table '{table_name}': {', '.join(missing)}"
        )
    return mapped


def map_metric_columns(
    table_name: str, metrics: List[str]
) -> List[Tuple[str, str]]:
    columns = get_table_columns(table_name)
    column_map = {col.lower(): col for col in columns}
    mapped: List[Tuple[str, str]] = []
    missing: List[str] = []

    for metric in metrics:
        normalized = normalize_metric_for_column(metric)
        if normalized in columns:
            mapped.append((metric, normalized))
            continue
        resolved = column_map.get(normalized.lower())
        if resolved:
            mapped.append((metric, resolved))
        else:
            missing.append(metric)

    if missing:
        raise ValidationError(
            f"Metrics not found in table '{table_name}': {', '.join(missing)}"
        )

    return mapped


def validate_required_columns(table_name: str, required_columns: List[str]) -> None:
    columns = get_table_columns(table_name)
    column_set = {col.lower() for col in columns}
    missing = [col for col in required_columns if col.lower() not in column_set]
    if missing:
        raise ValidationError(
            f"Required columns missing in '{table_name}': {', '.join(missing)}"
        )


def get_metrics_by_table(metrics: List[str]) -> Dict[str, List[str]]:
    metrics_table = Config.METRICS_TABLE_NAME
    table_columns = get_table_columns(metrics_table)
    column_map = {col.lower(): col for col in table_columns}

    metric_column = column_map.get(Config.METRICS_TABLE_METRIC_COLUMN.lower())
    if not metric_column:
        metric_column = column_map.get("metricname") or column_map.get("metrics")
    table_column = column_map.get(Config.METRICS_TABLE_TABLE_COLUMN.lower())
    if not table_column:
        table_column = column_map.get("tablename") or column_map.get("table_name")

    if not metric_column or not table_column:
        raise DatabaseError(
            "metricsandtables column detection failed. "
            f"Found columns: {', '.join(table_columns)}"
        )

    query_params: List[Any] = []
    metric_lookup = build_metric_lookup(metrics)
    lookup_values = list(metric_lookup.keys())
    column_sql = quote_identifier(metric_column)
    table_sql = quote_identifier(table_column)

    if Config.is_postgresql():
        placeholders = ", ".join(["%s"] * len(lookup_values))
        query_params.extend(lookup_values)
        metrics_filter = f"{column_sql} IN ({placeholders})"
    else:
        escaped = [f"'{escape_literal(m)}'" for m in lookup_values]
        metrics_filter = f"{column_sql} IN ({', '.join(escaped)})"

    query = (
        f"SELECT {table_sql} AS tablename, {column_sql} AS metric "
        f"FROM {quote_identifier(metrics_table)} "
        f"WHERE {metrics_filter}"
    )

    try:
        logger.info(f"Query: {query}")
        logger.info(f"Query params: {query_params}")
        rows = db_connection.execute_query(query, query_params)
    except Exception as exc:
        raise DatabaseError(f"Failed to query metrics table: {exc}") from exc

    metrics_by_table: Dict[str, List[str]] = {}
    for row in rows:
        table_value = row.get("tablename") or row.get("tableName") or row.get("TABLE_NAME")
        metric_value = row.get("metric") or row.get(metric_column)
        if not table_value or not metric_value:
            continue
        original_metric = metric_lookup.get(str(metric_value), str(metric_value))
        resolved = resolve_table_name(str(table_value))
        if not resolved:
            continue
        metrics_by_table.setdefault(resolved, []).append(original_metric)

    return metrics_by_table


def build_horizontal_query(
    table_name: str,
    metrics: List[str],
    entity_names: Optional[List[str]],
    start: Optional[int],
    end: Optional[int],
    sort: Optional[Tuple[str, str]],
) -> Tuple[str, List[Any]]:
    params: List[Any] = []
    timestamp_sql = quote_identifier("timestamp")
    id_sql = quote_identifier("id")
    metric_sql = ", ".join([quote_identifier(m) for m in metrics])
    select_clause = f"{timestamp_sql} AS timestamp, {id_sql} AS id, {metric_sql}"
    timestamp_type = get_column_type(table_name, "timestamp")

    where_conditions = []
    if entity_names:
        where_conditions.append(build_in_condition(id_sql, entity_names, params, Config.DB_TYPE))

    where_conditions.extend(
        build_time_conditions(
            timestamp_sql,
            start,
            end,
            params,
            Config.DB_TYPE,
            timestamp_type=timestamp_type,
        )
    )

    where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

    order_by = ""
    if sort:
        order_by = f"ORDER BY {timestamp_sql} {sort[1]}"
    else:
        order_by = f"ORDER BY {timestamp_sql} ASC"

    query = (
        f"SELECT {select_clause} "
        f"FROM {quote_identifier(table_name)} "
        f"{where_clause} "
        f"{order_by}"
    )
    return query, params


def build_vertical_query(
    table_name: str,
    metrics: List[str],
    entity_names: Optional[List[str]],
    start: Optional[int],
    end: Optional[int],
    sort: Optional[Tuple[str, str]],
) -> Tuple[str, List[Any]]:
    params: List[Any] = []
    timestamp_sql = quote_identifier("timestamp")
    id_sql = quote_identifier("id")
    metric_sql = quote_identifier("udc_config_name")
    value_sql = quote_identifier("udc_config_value")
    timestamp_type = get_column_type(table_name, "timestamp")

    where_conditions = []
    if metrics:
        where_conditions.append(build_in_condition(metric_sql, metrics, params, Config.DB_TYPE))
    if entity_names:
        where_conditions.append(build_in_condition(id_sql, entity_names, params, Config.DB_TYPE))
    where_conditions.extend(
        build_time_conditions(
            timestamp_sql,
            start,
            end,
            params,
            Config.DB_TYPE,
            timestamp_type=timestamp_type,
        )
    )

    where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

    order_by = ""
    if sort:
        order_by = f"ORDER BY {timestamp_sql} {sort[1]}"
    else:
        order_by = f"ORDER BY {timestamp_sql} ASC"

    query = (
        f"SELECT {timestamp_sql} AS timestamp, {id_sql} AS id, "
        f"{metric_sql} AS udc_config_name, {value_sql} AS udc_config_value "
        f"FROM {quote_identifier(table_name)} "
        f"{where_clause} "
        f"{order_by}"
    )
    return query, params


def build_summary_horizontal_query(
    table_name: str,
    metric_pairs: List[Tuple[str, str]],
    entity_ids: List[str],
    start: int,
    end: int,
) -> Tuple[str, List[Any]]:
    params: List[Any] = []
    id_sql = quote_identifier("id")
    select_parts = [f"{id_sql} AS id"]
    for _, column_metric in metric_pairs:
        column_sql = quote_identifier(column_metric)
        avg_sql = build_avg_expression(column_sql)
        select_parts.append(f"{avg_sql} AS {quote_identifier(column_metric)}")

    where_conditions = []
    if entity_ids:
        where_conditions.append(build_in_condition(id_sql, entity_ids, params, Config.DB_TYPE))

    timestamp_sql = quote_identifier("timestamp")
    where_conditions.extend(
        build_time_conditions(
            timestamp_sql,
            start,
            end,
            params,
            Config.DB_TYPE,
            timestamp_type=get_column_type(table_name, "timestamp"),
        )
    )

    where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

    query = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {quote_identifier(table_name)} "
        f"{where_clause} "
        f"GROUP BY {id_sql}"
    )
    return query, params


def build_summary_vertical_query(
    table_name: str,
    metrics: List[str],
    entity_ids: List[str],
    start: int,
    end: int,
) -> Tuple[str, List[Any]]:
    params: List[Any] = []
    id_sql = quote_identifier("id")
    metric_sql = quote_identifier("udc_config_name")
    value_sql = quote_identifier("udc_config_value")
    avg_sql = build_avg_expression(value_sql)

    where_conditions = []
    if metrics:
        where_conditions.append(build_in_condition(metric_sql, metrics, params, Config.DB_TYPE))
    if entity_ids:
        where_conditions.append(build_in_condition(id_sql, entity_ids, params, Config.DB_TYPE))

    timestamp_sql = quote_identifier("timestamp")
    where_conditions.extend(
        build_time_conditions(
            timestamp_sql,
            start,
            end,
            params,
            Config.DB_TYPE,
            timestamp_type=get_column_type(table_name, "timestamp"),
        )
    )

    where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

    query = (
        f"SELECT {id_sql} AS id, {metric_sql} AS metric, {avg_sql} AS avg_value "
        f"FROM {quote_identifier(table_name)} "
        f"{where_clause} "
        f"GROUP BY {id_sql}, {metric_sql}"
    )
    return query, params


@router.get("/service/dataset/metric/uitimeseries")
async def get_ui_timeseries(
    entityNames: Optional[str] = Query(None, description="Comma-separated list of entity IDs"),
    metrics: str = Query(..., description="Comma-separated list of metrics"),
    start: Optional[str] = Query(None, description="Start timestamp (epoch milliseconds)"),
    end: Optional[str] = Query(None, description="End timestamp (epoch milliseconds)"),
    metricDoubleValue: Optional[bool] = Query(False, description="Return metric values as double"),
    fillUpNull: Optional[bool] = Query(None, description="Ignored"),
    sort: Optional[str] = Query(None, description="Sort order, e.g. +timestamp or -timestamp"),
    properties: Optional[str] = Query(
        None,
        description="Comma-separated enrichment properties from enrichmentlookup table",
    ),
):
    try:
        metrics_list = [m.strip() for m in metrics.split(",") if m.strip()]
        validated_metrics = validate_metrics(metrics_list)

        entity_list = None
        if entityNames:
            entity_list = [e.strip() for e in entityNames.split(",") if e.strip()]

        validated_start = validate_timestamp(start)
        validated_end = validate_timestamp(end)
        parsed_sort = normalize_sort(sort)
        properties_list = parse_properties(properties)

        metrics_by_table = get_metrics_by_table(validated_metrics)
        if not metrics_by_table:
            return []

        results: List[Dict[str, Any]] = []

        for table_name, table_metrics in metrics_by_table.items():
            if is_vertical_table(table_name):
                validate_required_columns(
                    table_name,
                    ["timestamp", "id", "udc_config_name", "udc_config_value"],
                )
                query, params = build_vertical_query(
                    table_name,
                    table_metrics,
                    entity_list,
                    validated_start,
                    validated_end,
                    parsed_sort,
                )
            else:
                validate_required_columns(table_name, ["timestamp", "id"])
                metric_pairs = map_metric_columns(table_name, table_metrics)
                mapped_metrics = [pair[1] for pair in metric_pairs]
                query, params = build_horizontal_query(
                    table_name,
                    mapped_metrics,
                    entity_list,
                    validated_start,
                    validated_end,
                    parsed_sort,
                )

            try:
                logger.info(f"Executing query: {query}")
                logger.info(f"Query params: {params}")
                rows = db_connection.execute_query(query, params)
            except Exception as exc:
                raise DatabaseError(f"Failed to execute query: {exc}") from exc

            if is_vertical_table(table_name):
                for row in rows:
                    metric_name = row.get("udc_config_name")
                    if metric_name not in table_metrics:
                        continue
                    timestamp_val = to_epoch_millis(row.get("timestamp"))
                    date_str, time_str = format_date_time(timestamp_val)
                    results.append(
                        {
                            "parent": 0,
                            "timestamp": timestamp_val,
                            "scope": SCOPE_VALUE,
                            "entityName": row.get("id"),
                            "parentName": "",
                            "date": date_str,
                            "metric": metric_name,
                            "entity": 0,
                            "time": time_str,
                            "parentId": "",
                            "value": normalize_value(row.get("udc_config_value"), metricDoubleValue),
                        }
                    )
            else:
                for row in rows:
                    timestamp_val = to_epoch_millis(row.get("timestamp"))
                    date_str, time_str = format_date_time(timestamp_val)
                    entity_name = row.get("id")
                    for original_metric, column_metric in metric_pairs:
                        if column_metric not in row:
                            continue
                        results.append(
                            {
                                "parent": 0,
                                "timestamp": timestamp_val,
                                "scope": SCOPE_VALUE,
                                "entityName": entity_name,
                                "parentName": "",
                                "date": date_str,
                                "metric": original_metric,
                                "entity": 0,
                                "time": time_str,
                                "parentId": "",
                                "value": normalize_value(row.get(column_metric), metricDoubleValue),
                            }
                        )

        if properties_list:
            entity_ids = sorted(
                {str(item.get("entityName")) for item in results if item.get("entityName")}
            )
            enrichment_map = fetch_enrichment_values(entity_ids, properties_list)
            for item in results:
                entity_key = str(item.get("entityName") or "")
                enrichment_values = enrichment_map.get(entity_key, {})
                for prop in properties_list:
                    item[prop] = enrichment_values.get(prop, "")

        if parsed_sort:
            reverse = parsed_sort[1] == "DESC"
            results.sort(key=lambda item: item.get("timestamp") or 0, reverse=reverse)

        return results

    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except DatabaseError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected error in uitimeseries endpoint")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/service/dataset/metric/entity")
async def get_entity(
    searchByProperties: str = Query(
        ..., description="Search filters for enrichmentlookup table"
    ),
    properties: Optional[str] = Query(
        None, description="Comma-separated enrichment properties"
    ),
    format: Optional[str] = Query(None, description="Response format (json)"),
    flatten: Optional[bool] = Query(None, description="Flatten response (true)"),
    entityResourceTypes: Optional[str] = Query(
        None, description="Comma-separated resource types"
    ),
):
    try:
        if format and format.lower() != "json":
            raise ValidationError("format must be json")

        search_filters = parse_search_by_properties(searchByProperties)
        properties_list = parse_properties(properties)

        table_name = Config.ENRICHMENT_TABLE_NAME
        column_map = resolve_table_columns(table_name)

        entity_column = column_map.get(Config.ENRICHMENT_TABLE_ENTITY_COLUMN.lower())
        if not entity_column:
            entity_column = column_map.get("entityid")
        if not entity_column:
            raise ValidationError("Enrichment table is missing entityid column")

        resolved_properties: List[Tuple[str, str]] = []
        missing_properties: List[str] = []
        for prop in properties_list:
            prop_lower = prop.lower()
            if prop_lower == "id":
                resolved_properties.append((prop, entity_column))
                continue
            resolved = column_map.get(prop_lower)
            if resolved:
                resolved_properties.append((prop, resolved))
            else:
                missing_properties.append(prop)
        if missing_properties:
            raise ValidationError(
                f"Enrichment properties not found: {', '.join(missing_properties)}"
            )

        params: List[Any] = []
        where_conditions: List[str] = []
        for column_name, value in search_filters:
            resolved = column_map.get(column_name.lower())
            if not resolved:
                raise ValidationError(f"Search column not found: {column_name}")
            column_sql = quote_identifier(resolved)
            where_conditions.append(
                build_equals_condition(column_sql, value, params, Config.DB_TYPE)
            )

        if entityResourceTypes:
            types = [t.strip() for t in entityResourceTypes.split(",") if t.strip()]
            if types:
                resolved_type = column_map.get("type")
                if not resolved_type:
                    raise ValidationError("Enrichment table is missing type column")
                type_sql = quote_identifier(resolved_type)
                if len(types) == 1:
                    where_conditions.append(
                        build_equals_condition(type_sql, types[0], params, Config.DB_TYPE)
                    )
                else:
                    where_conditions.append(
                        build_in_condition(type_sql, types, params, Config.DB_TYPE)
                    )

        select_columns = [entity_column]
        for _, resolved in resolved_properties:
            if resolved not in select_columns:
                select_columns.append(resolved)

        select_sql = ", ".join([quote_identifier(col) for col in select_columns])
        where_sql = " AND ".join(where_conditions)
        query = (
            f"SELECT {select_sql} "
            f"FROM {quote_identifier(table_name)} "
            f"WHERE {where_sql}"
        )

        rows = db_connection.execute_query(query, params)

        results: List[Dict[str, Any]] = []
        for row in rows:
            entity_value = row.get(entity_column)
            if entity_value is None:
                continue
            item: Dict[str, Any] = {
                "entityId": 0,
                "shortDisplayName": "0.00",
                "entityName": "0.00",
                "id": str(entity_value),
                "parentName": "0",
                "parentEntityId": 0,
                "rawEntityName": "0",
                "rawParentName": "0",
                "parentId": "0",
            }
            for prop, resolved in resolved_properties:
                if prop.lower() == "id":
                    item[prop] = str(entity_value)
                    continue
                value = row.get(resolved)
                item[prop] = "" if value is None else value
            results.append(item)

        return results

    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except DatabaseError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected error in entity endpoint")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/service/dataset/metric/summary")
async def get_summary(
    metrics: str = Query(..., description="Comma-separated list of metrics"),
    start: Optional[str] = Query(None, description="Start timestamp (epoch milliseconds)"),
    end: Optional[str] = Query(None, description="End timestamp (epoch milliseconds)"),
    searchByProperties: str = Query(
        ..., description="Search filters for enrichmentlookup table"
    ),
    properties: Optional[str] = Query(
        None, description="Comma-separated enrichment properties"
    ),
    flatten: Optional[bool] = Query(None, description="Flatten response (true)"),
    metricDoubleValue: Optional[bool] = Query(False, description="Return metric values as double"),
    suppressSummary: Optional[bool] = Query(None, description="Suppress summary (true)"),
):
    try:
        metrics_list = [m.strip() for m in metrics.split(",") if m.strip()]
        validated_metrics = validate_metrics(metrics_list)

        validated_start = validate_timestamp(start)
        validated_end = validate_timestamp(end)
        if validated_start is None or validated_end is None:
            raise ValidationError("start and end are required")

        search_filters = parse_search_by_properties(searchByProperties)
        properties_list = parse_properties(properties)

        table_name = Config.ENRICHMENT_TABLE_NAME
        column_map = resolve_table_columns(table_name)

        entity_column = column_map.get(Config.ENRICHMENT_TABLE_ENTITY_COLUMN.lower())
        if not entity_column:
            entity_column = column_map.get("entityid")
        if not entity_column:
            raise ValidationError("Enrichment table is missing entityid column")

        resolved_properties, missing_properties = resolve_enrichment_property_map(
            properties_list,
            column_map,
            entity_column,
        )
        if missing_properties:
            raise ValidationError(
                f"Enrichment properties not found: {', '.join(missing_properties)}"
            )

        entity_ids = fetch_entity_ids_by_search(
            table_name,
            column_map,
            search_filters,
        )
        if not entity_ids:
            return []

        enrichment_map = fetch_enrichment_values_by_map(
            table_name,
            entity_ids,
            entity_column,
            resolved_properties,
        )

        metrics_by_table = get_metrics_by_table(validated_metrics)
        if not metrics_by_table:
            return []

        results_by_entity: Dict[str, Dict[str, Any]] = {}

        def ensure_row(entity_id: str) -> Dict[str, Any]:
            existing = results_by_entity.get(entity_id)
            if existing:
                return existing
            row = {
                "displayName": entity_id,
                "parent": 0,
                "endTimeStamp": validated_end,
                "scope": SCOPE_VALUE,
                "startTimeStamp": validated_start,
                "rank": 0,
                "entityName": entity_id,
                "parentName": "",
                "rawEntityName": entity_id,
                "rawParentName": "",
                "entity": 0,
                "parentId": "0",
            }
            results_by_entity[entity_id] = row
            return row

        for table_name, table_metrics in metrics_by_table.items():
            if is_vertical_table(table_name):
                validate_required_columns(
                    table_name,
                    ["timestamp", "id", "udc_config_name", "udc_config_value"],
                )
                query, params = build_summary_vertical_query(
                    table_name,
                    table_metrics,
                    entity_ids,
                    validated_start,
                    validated_end,
                )
                rows = db_connection.execute_query(query, params)
                for row in rows:
                    entity_id = row.get("id")
                    metric_name = row.get("metric")
                    if entity_id is None or metric_name is None:
                        continue
                    entity_key = str(entity_id)
                    summary_row = ensure_row(entity_key)
                    summary_row[metric_name] = normalize_value(
                        row.get("avg_value"), metricDoubleValue
                    )
            else:
                validate_required_columns(table_name, ["timestamp", "id"])
                metric_pairs = map_metric_columns(table_name, table_metrics)
                query, params = build_summary_horizontal_query(
                    table_name,
                    metric_pairs,
                    entity_ids,
                    validated_start,
                    validated_end,
                )
                rows = db_connection.execute_query(query, params)
                for row in rows:
                    entity_id = row.get("id")
                    if entity_id is None:
                        continue
                    entity_key = str(entity_id)
                    summary_row = ensure_row(entity_key)
                    for original_metric, column_metric in metric_pairs:
                        if column_metric not in row:
                            continue
                        summary_row[original_metric] = normalize_value(
                            row.get(column_metric), metricDoubleValue
                        )

        for entity_id, summary_row in results_by_entity.items():
            enrichment_values = enrichment_map.get(entity_id, {})
            for prop, _ in resolved_properties:
                summary_row[prop] = enrichment_values.get(prop, "")

        return list(results_by_entity.values())

    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except DatabaseError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected error in summary endpoint")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
