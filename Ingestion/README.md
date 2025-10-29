# ETL Processor for KPI Data Transformation

This project implements an ETL (Extract, Transform, Load) processor that transforms CSV data according to the requirements specified in `etlrequirements.txt`.

## Overview

The ETL processor takes input CSV data and transforms it based on expressions defined in KPI details configuration files. It creates output CSV files with standardized columns: Market, Region, VCPType, Technology, Datacenter, Site, Id, and DisplayName. **The processor only includes rows that satisfy the input filter conditions - rows that don't match any rule are excluded from the output.**

## Files

### Core Implementation
- `etl_processor.py` - Main ETL processor class
- `requirements.txt` - Python dependencies
- `etlrequirements.txt` - Original requirements specification

### Test Files
- `test_etl_processor.py` - Unit tests for the ETL processor
- `test_integration.py` - Integration tests with real data scenarios
- `pytest.ini` - Test configuration
- `run_tests.py` - Test runner script

### Example and Documentation
- `example_usage.py` - Example script showing how to use the ETL processor
- `README.md` - This documentation file

## Requirements

- Python 3.7+
- pandas >= 1.5.0
- numpy >= 1.21.0
- pytest >= 7.0.0 (for testing)

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```python
from etl_processor import ETLProcessor

# Create processor instance
processor = ETLProcessor()

# Run ETL process
processor.process(
    input_file="inputdata.csv",
    kpi_data_sources_file="KPIDatasources.csv",
    kpi_details_file="KPIDetails.csv",
    output_file="output_data.csv"
)
```

### Command Line Usage

Run the example script:
```bash
python example_usage.py
```

### Input Files Required

1. **inputdata.csv** - Main input data file with columns like MARKET, REGION, SITE, etc.
2. **KPIDatasources.csv** - Configuration file with KPI data source information
3. **KPIDetails.csv** - Configuration file with transformation expressions
4. **KPIResourceTypes.csv** - Configuration file with resource type information (optional)
5. **KPIResourceRules.csv** - Configuration file with filtering rules and additional expressions (optional)
6. **KpiRuleRules.csv** - Configuration file with KPI rules for dynamic columns and timestamp generation (optional)

### Output

The processor generates a CSV file with the following columns:
- Market
- Region
- VCPType
- Technology
- Datacenter
- Site
- Id (from KPIResourceRules.IdExpr)
- DisplayName (from KPIResourceRules.DisplayNameExpr)
- Timestamp (from KpiRuleRules.KpiTimestampExpr - first rule only)
- Freq (from KpiRuleRules.KpiIntervalExpr - first rule with value)
- Dynamic KPI columns (from KpiRuleRules.KpiName and KpiValueExpr)

## Expression Evaluation

The processor supports the following expression types:

1. **Column References**: `/MARKET`, `/REGION`, `/SITE`
   - References values from input data columns
   
2. **Literal Values**: `'VCP Far Edge'`, `'NR'`
   - Static values enclosed in single quotes

3. **padLeft() Function**: `padLeft(/GNB, 7, '0')`
   - Pads a string to the left with a specified character
   - Example: `padLeft(/GNB, 7, '0')` with GNB='123' returns '0000123'

4. **concat() Function**: `concat(/deviceName,'_EdgeView:<',/elements/ifIndex,'>')`
   - Concatenates multiple strings and column values
   - Example: `concat(/deviceName,'_EdgeView:<',/elements/ifIndex,'>')` returns 'router01_EdgeView:<1001>'
   - Supports nested functions: `concat(padLeft(/GNB, 7, '0'), padLeft(/DU, 4, '0'))` returns '00001230045'

5. **isEmpty() Function**: `isEmpty(/labels/kubernetes_namespace)`
   - Returns 'true' if column is empty or missing, 'false' otherwise
   - Example: `isEmpty(/labels/kubernetes_namespace)` returns 'false' for non-empty values

6. **Conditional Expressions**: `{[not(isEmpty(/labels/kubernetes_namespace))] -> /labels/kubernetes_namespace, [not(isEmpty(/labels/namespace))] -> /labels/namespace}`
   - Returns the value of the first matching condition
   - Example: Returns kubernetes_namespace if not empty, otherwise returns namespace if not empty

7. **Empty Expressions**: `''` or `NaN`
   - Results in empty string values

8. **Nested Expressions**: Functions can be nested within other functions
   - Example: `concat(padLeft(/GNB, 7, '0'), padLeft(/DU, 4, '0'))` - Equivalent to GNB[0:7]+DU[0:4]
   - Supports complex combinations of all expression types

## KPIRuleRules Functionality

The processor supports dynamic KPI column generation through the `KpiRuleRules.csv` file:

### KpiRuleRules Table Structure
- **KpiRuleRuleID**: Primary key
- **KpiResourceRuleID**: Links to KPIResourceRules table
- **KpiTimestampExpr**: Expression to create Timestamp column (first rule only)
- **KpiIntervalExpr**: Expression to create/update Freq column (first rule with value)
- **KpiName**: Name of the KPI (becomes column name)
- **KpiValueExpr**: Expression to evaluate for the KPI value
- **KpiUnit**: Unit of measurement for the KPI
- **KpiAggregator**: Aggregation function for the KPI

### Processing Logic
1. For each matching KPIResourceRule, find all associated KpiRuleRules
2. Use the first rule's `KpiTimestampExpr` to create the Timestamp column
3. Use the first rule's `KpiIntervalExpr` (if present) to create/update the Freq column
4. For each rule, create a dynamic column named after `KpiName` with the value from `KpiValueExpr`

## Filter Conditions

The processor supports various filter condition types in the `InputFilter` field:

1. **Equality Conditions**: `/MARKET = '072'`
   - Filters rows where the specified column equals the given value

2. **isBlank() Function**: `isBlank(/DU)`
   - Returns true if the column is blank (empty, null, or doesn't exist)

3. **not(isBlank()) Function**: `not(isBlank(/GNB))`
   - Returns true if the column is not blank (has a value)

4. **contains() Function**: `contains(/objectName,'bond')`
   - Returns true if the column value contains the specified substring

5. **matchesRegex() Function**: `matchesRegex(/name,'01-Inlet.*|02-CPU.*')`
   - Returns true if the column value matches the regular expression pattern

6. **Complex Conditions**: `[isBlank(/DU) and not(isBlank(/GNB))]`
   - Combines multiple conditions using 'and' logic
   - Supports brackets for grouping

7. **No Filter**: Empty or null `InputFilter`
   - All rows pass the filter condition

## Rule Selection Logic

- If multiple `KPIResourceRules` rows exist for a `KPIDetails`, only the first row that satisfies the `InputFilter` condition is considered
- The `IncludeKpiFilter` field determines whether the `InputFilter` should be applied:
  - If `IncludeKpiFilter = "1"`: Apply the `InputFilter` condition
  - If `IncludeKpiFilter ≠ "1"`: Ignore the `InputFilter` condition
- **Important**: If no condition satisfies, the row is excluded from the output (process stops for that row)
- Only rows that satisfy at least one filter condition are included in the output

## Testing

### Run All Tests
```bash
python run_tests.py
```

### Run Unit Tests Only
```bash
python run_tests.py --unit
```

### Run Integration Tests Only
```bash
python run_tests.py --integration
```

### Run Tests with Coverage
```bash
python run_tests.py --coverage
```

### Run Tests with pytest directly
```bash
pytest test_etl_processor.py test_integration.py -v
```

## Features

- **Robust Error Handling**: Comprehensive error handling and logging
- **Expression Evaluation**: Flexible expression evaluation system
- **Data Validation**: Input validation and data type handling
- **Performance**: Efficient processing of large datasets
- **Comprehensive Testing**: Unit and integration tests with high coverage
- **Logging**: Detailed logging for debugging and monitoring

## Architecture

### ETLProcessor Class

The main class that orchestrates the ETL process:

- `load_kpi_data_sources()` - Loads KPI data source configuration
- `load_kpi_details()` - Loads KPI details with transformation expressions
- `evaluate_expression()` - Evaluates expressions against row data
- `transform_data()` - Transforms input data based on expressions
- `process()` - Main method that runs the complete ETL process

### Expression Evaluation

The expression evaluator handles:
- Column references (e.g., `/MARKET`)
- Literal values (e.g., `'VCP Far Edge'`)
- Empty/null expressions
- Missing column handling
- Data type conversion

## Error Handling

The processor includes comprehensive error handling for:
- Missing input files
- Invalid CSV format
- Missing columns in input data
- Expression evaluation errors
- File I/O errors

## Logging

The processor uses Python's logging module with configurable levels:
- DEBUG: Detailed debugging information
- INFO: General information about processing steps
- WARNING: Non-critical issues
- ERROR: Critical errors that stop processing

Logs are written to both console and `etl_processor.log` file.

## Performance

The processor is designed to handle large datasets efficiently:
- Memory-efficient pandas operations
- Progress logging for long-running processes
- Optimized expression evaluation
- Duplicate removal in output

## Extensibility

The design allows for easy extension:
- New expression types can be added to `evaluate_expression()`
- Additional output columns can be added
- Custom data validation can be implemented
- Different output formats can be supported

## Example Output

Given input data with MARKET='072', REGION='Upstate New York', and expressions:
- MarketExpr: `/MARKET`
- RegionExpr: `/REGION`  
- VcpTypeExpr: `'VCP Far Edge'`
- TechnologyExpr: `'NR'`
- IdExpr: `/SITE`
- DisplayNameExpr: `'Site Identifier'`

The output would be:
```csv
Market,Region,VCPType,Technology,Datacenter,Site,Id,DisplayName
072,Upstate New York,VCP Far Edge,NR,,,ESYR,Site Identifier
```

**Important**: The processor only includes rows that satisfy the `InputFilter` conditions. Rows that don't match any rule are excluded from the output, so the output may have fewer rows than the input.

## Troubleshooting

### Common Issues

1. **File Not Found**: Ensure all required CSV files are present
2. **Column Not Found**: Check that input data contains columns referenced in expressions
3. **Empty Output**: Verify that expressions are correctly defined and input data is valid
4. **Memory Issues**: For very large files, consider processing in chunks

### Debug Mode

Run with debug logging to see detailed processing information:
```python
processor = ETLProcessor(log_level="DEBUG")
```
