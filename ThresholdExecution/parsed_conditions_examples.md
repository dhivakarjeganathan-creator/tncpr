# Parsed Conditions Examples

This document shows how each condition from error.txt would be parsed by the updated `_parse_condition` function.

## Example Conditions and Their Parsed Output

### 1. Simple type and ranMarket like
**Condition:** `"resource.type=='DU' && resource.ranMarket.like('13*')"`
```json
{
  "type": "du",
  "ranMarket": {"type": "like", "value": "13%"},
  "Band": null,
  "id": null
}
```

### 2. Type, Band, and multiple ranMarket OR
**Condition:** `"resource.type=='DU' && resource.Band=='MMW' && (resource.ranMarket=='131' || resource.ranMarket=='132' || ...)"`
```json
{
  "type": "du",
  "ranMarket": {"type": "in", "values": ["131", "132", "133", "134", "135", "136", "137", "138", "139", "140", "184"]},
  "Band": {"type": "equals", "value": "MMW"},
  "id": null
}
```

### 3. ENB with multiple id.like() in OR
**Condition:** `"resource.type=='ENB' && (resource.id.like('^070*') || resource.id.like('^071*') || resource.id.like('^072*') || resource.id.like('^073*') || resource.id.like('^074*'))"`
```json
{
  "type": "enb",
  "ranMarket": null,
  "Band": null,
  "id": {"type": "like_or", "values": ["070%", "071%", "072%", "073%", "074%"]}
}
```

### 4. Single id.like() with wildcard
**Condition:** `"resource.type=='ThermalTemperatures' && resource.id.like('*01-Inlet*')"`
```json
{
  "type": "thermaltemperatures",
  "ranMarket": null,
  "Band": null,
  "id": {"type": "like", "value": "%01-Inlet%"}
}
```

### 5. Single id.like() with pipe-separated patterns
**Condition:** `"resource.type=='ThermalTemperatures' && resource.id.like('*01-Inlet*|*MLB_INLET_TEMP*|*INLET_TEMP_L*')"`
```json
{
  "type": "thermaltemperatures",
  "ranMarket": null,
  "Band": null,
  "id": {"type": "like_or", "values": ["%01-Inlet%", "%MLB_INLET_TEMP%", "%INLET_TEMP_L%"]}
}
```

### 6. Device with id.like()
**Condition:** `"resource.type=='Device' && resource.id.like('*-rh-pe092*')"`
```json
{
  "type": "device",
  "ranMarket": null,
  "Band": null,
  "id": {"type": "like", "value": "%-rh-pe092%"}
}
```

### 7. AUPF with id.like()
**Condition:** `"resource.type=='AUPF' && resource.id.like('*UIC*')"`
```json
{
  "type": "aupf",
  "ranMarket": null,
  "Band": null,
  "id": {"type": "like", "value": "%UIC%"}
}
```

### 8. ThermalTemperatures with pipe-separated id patterns
**Condition:** `"resource.type=='ThermalTemperatures' && resource.id.like('*INLET_TEMP_L|*MLB_INLET_TEMP')"`
```json
{
  "type": "thermaltemperatures",
  "ranMarket": null,
  "Band": null,
  "id": {"type": "like_or", "values": ["%INLET_TEMP_L%", "%MLB_INLET_TEMP%"]}
}
```

### 9. Sector type with Band and ranMarket
**Condition:** `"resource.type=='sector' && resource.Band=='SUB6' && (resource.ranMarket=='131' || resource.ranMarket=='132' || ...)"`
```json
{
  "type": "sector",
  "ranMarket": {"type": "in", "values": ["131", "132", "133", "134", "135", "136", "137", "138", "139", "140", "184"]},
  "Band": {"type": "equals", "value": "SUB6"},
  "id": null
}
```

## SQL Query Generation Examples

### Example 1: Single id.like()
**Parsed:** `{"id": {"type": "like", "value": "%01-Inlet%"}}`
**SQL:** `AND fullname like '%01-Inlet%'`

### Example 2: Multiple id.like() in OR
**Parsed:** `{"id": {"type": "like_or", "values": ["070%", "071%", "072%"]}}`
**SQL:** `AND (fullname like '070%' OR fullname like '071%' OR fullname like '072%')`

### Example 3: Pipe-separated patterns
**Parsed:** `{"id": {"type": "like_or", "values": ["%01-Inlet%", "%MLB_INLET_TEMP%"]}}`
**SQL:** `AND (fullname like '%01-Inlet%' OR fullname like '%MLB_INLET_TEMP%')`

## Pattern Handling

1. **Wildcard `*`**: Converted to SQL `%` for LIKE patterns
2. **Start anchor `^`**: Removed (handled as start of string in SQL LIKE)
3. **Pipe separator `|`**: Split into multiple OR conditions
4. **Multiple `resource.id.like()` in OR**: Combined into `like_or` type with multiple values
5. **Single `resource.id.like()`**: Handled as `like` type with single value

