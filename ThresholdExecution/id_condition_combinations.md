# ID Condition Combinations - Supported Patterns

## Summary of All Supported `id` Condition Patterns

The updated `_parse_condition` and `_build_enrichment_query` functions now support the following combinations:

### 1. Single `id.like()` Pattern
**Pattern:** `resource.id.like('*pattern*')`
- **Example:** `resource.id.like('*01-Inlet*')`
- **Parsed:** `{"id": {"type": "like", "value": "%01-Inlet%"}}`
- **SQL:** `AND fullname like '%01-Inlet%'`

### 2. Single `id.like()` with Start Anchor (^)
**Pattern:** `resource.id.like('^pattern*')`
- **Example:** `resource.id.like('^070*')`
- **Parsed:** `{"id": {"type": "like", "value": "070%"}}`
- **SQL:** `AND fullname like '070%'`
- **Note:** The `^` anchor is removed as SQL LIKE with `%` at the end handles start matching

### 3. Single `id.like()` with Pipe-Separated Patterns
**Pattern:** `resource.id.like('pattern1|pattern2|pattern3')`
- **Example:** `resource.id.like('*01-Inlet*|*MLB_INLET_TEMP*|*INLET_TEMP_L*')`
- **Parsed:** `{"id": {"type": "like_or", "values": ["%01-Inlet%", "%MLB_INLET_TEMP%", "%INLET_TEMP_L%"]}}`
- **SQL:** `AND (fullname like '%01-Inlet%' OR fullname like '%MLB_INLET_TEMP%' OR fullname like '%INLET_TEMP_L%')`

### 4. Multiple `id.like()` in OR Conditions
**Pattern:** `(resource.id.like('pattern1') || resource.id.like('pattern2') || ...)`
- **Example:** `(resource.id.like('^070*') || resource.id.like('^071*') || resource.id.like('^072*'))`
- **Parsed:** `{"id": {"type": "like_or", "values": ["070%", "071%", "072%"]}}`
- **SQL:** `AND (fullname like '070%' OR fullname like '071%' OR fullname like '072%')`

### 5. Single `id` Equals
**Pattern:** `resource.id == 'value'`
- **Example:** `resource.id == 'ABC123'`
- **Parsed:** `{"id": {"type": "equals", "value": "ABC123"}}`
- **SQL:** `AND fullname = 'ABC123'`

### 6. Multiple `id` Equals in OR Conditions
**Pattern:** `(resource.id == 'value1' || resource.id == 'value2' || ...)`
- **Example:** `(resource.id == 'ABC123' || resource.id == 'DEF456')`
- **Parsed:** `{"id": {"type": "in", "values": ["ABC123", "DEF456"]}}`
- **SQL:** `AND fullname in ('ABC123', 'DEF456')`

## Pattern Conversion Rules

1. **Wildcard `*`** → SQL `%` (matches any sequence of characters)
2. **Start anchor `^`** → Removed (SQL LIKE with pattern at start handles this)
3. **Pipe separator `|`** → Split into multiple OR conditions
4. **Multiple `resource.id.like()` in OR** → Combined into single `like_or` type

## Examples from error.txt

### Example 1: Simple id.like()
```
Condition: "resource.type=='ThermalTemperatures' && resource.id.like('*01-Inlet*')"
Parsed id: {"type": "like", "value": "%01-Inlet%"}
SQL: AND fullname like '%01-Inlet%'
```

### Example 2: Multiple id.like() in OR
```
Condition: "resource.type=='ENB' && (resource.id.like('^070*') || resource.id.like('^071*') || resource.id.like('^072*') || resource.id.like('^073*') || resource.id.like('^074*'))"
Parsed id: {"type": "like_or", "values": ["070%", "071%", "072%", "073%", "074%"]}
SQL: AND (fullname like '070%' OR fullname like '071%' OR fullname like '072%' OR fullname like '073%' OR fullname like '074%')
```

### Example 3: Pipe-separated patterns
```
Condition: "resource.type=='ThermalTemperatures' && resource.id.like('*01-Inlet*|*MLB_INLET_TEMP*|*INLET_TEMP_L*')"
Parsed id: {"type": "like_or", "values": ["%01-Inlet%", "%MLB_INLET_TEMP%", "%INLET_TEMP_L%"]}
SQL: AND (fullname like '%01-Inlet%' OR fullname like '%MLB_INLET_TEMP%' OR fullname like '%INLET_TEMP_L%')
```

### Example 4: Complex with start anchor
```
Condition: "resource.type=='ENB' && (resource.id.like('^070*') || resource.id.like('^071*'))"
Parsed id: {"type": "like_or", "values": ["070%", "071%"]}
SQL: AND (fullname like '070%' OR fullname like '071%')
```

## Combined Conditions

The `id` condition can be combined with other conditions:
- `type` (e.g., `resource.type == 'ENB'`)
- `ranMarket` (e.g., `resource.ranMarket.like('13*')`)
- `Band` (e.g., `resource.Band == 'MMW'`)

All conditions are combined with AND in the final SQL query.

