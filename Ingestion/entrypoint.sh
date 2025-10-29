#!/bin/bash
set -e

# Default values
INPUT_FILE="${INPUT_FILE:-inputdata.csv}"
OUTPUT_FILE="${OUTPUT_FILE:-transformed_output.csv}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -i, --input FILE     Input CSV file (default: inputdata.csv)"
    echo "  -o, --output FILE    Output CSV file (default: transformed_output.csv)"
    echo "  -l, --log-level LEVEL Log level (DEBUG, INFO, WARNING, ERROR) (default: INFO)"
    echo "  --test               Run tests instead of ETL process"
    echo "  --help               Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  INPUT_FILE           Input CSV file path"
    echo "  OUTPUT_FILE          Output CSV file path"
    echo "  LOG_LEVEL            Logging level"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Run with defaults"
    echo "  $0 -i mydata.csv -o result.csv       # Custom input/output files"
    echo "  $0 --test                            # Run tests"
    echo "  docker run -e INPUT_FILE=mydata.csv  # Using environment variables"
}

# Parse command line arguments
RUN_TESTS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--input)
            INPUT_FILE="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        -l|--log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        --test)
            RUN_TESTS=true
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate log level
case $LOG_LEVEL in
    DEBUG|INFO|WARNING|ERROR)
        ;;
    *)
        echo "Invalid log level: $LOG_LEVEL. Must be one of: DEBUG, INFO, WARNING, ERROR"
        exit 1
        ;;
esac

echo "ETL Processor Container Starting..."
echo "=================================="
echo "Input file: $INPUT_FILE"
echo "Output file: $OUTPUT_FILE"
echo "Log level: $LOG_LEVEL"
echo "Working directory: $(pwd)"
echo "=================================="

# Check if required files exist
if [ "$RUN_TESTS" = false ]; then
    echo "Checking required files..."
    
    # Check for required configuration files
    REQUIRED_FILES=("KPIDatasources.csv" "KPIDetails.csv")
    for file in "${REQUIRED_FILES[@]}"; do
        if [ ! -f "$file" ]; then
            echo "Warning: Required configuration file '$file' not found in current directory"
            echo "Available files:"
            ls -la *.csv 2>/dev/null || echo "No CSV files found"
        fi
    done
    
    # Check for input file
    if [ ! -f "$INPUT_FILE" ]; then
        echo "Error: Input file '$INPUT_FILE' not found"
        echo "Available files:"
        ls -la *.csv 2>/dev/null || echo "No CSV files found"
        exit 1
    fi
    
    echo "All required files found. Starting ETL process..."
    
    # Run the ETL process
    python -c "
import os
import sys
from etl_processor import ETLProcessor

# Set environment variables
os.environ['LOG_LEVEL'] = '$LOG_LEVEL'

# Create processor instance
processor = ETLProcessor(log_level='$LOG_LEVEL')

# Define file paths
input_file = '$INPUT_FILE'
kpi_data_sources_file = 'KPIDatasources.csv'
kpi_details_file = 'KPIDetails.csv'
output_file = '$OUTPUT_FILE'
kpi_resource_types_file = 'KPIResourceTypes.csv'
kpi_resource_rules_file = 'KPIResourceRules.csv'
kpi_rule_rules_file = 'KpiRuleRules.csv'

try:
    print('Starting ETL process...')
    processor.process(
        input_file=input_file,
        kpi_data_sources_file=kpi_data_sources_file,
        kpi_details_file=kpi_details_file,
        output_file=output_file,
        kpi_resource_types_file=kpi_resource_types_file,
        kpi_resource_rules_file=kpi_resource_rules_file,
        kpi_rule_rules_file=kpi_rule_rules_file
    )
    print('ETL process completed successfully!')
    print(f'Output saved to: {output_file}')
    
    # Show output statistics
    import pandas as pd
    if os.path.exists(output_file):
        output_df = pd.read_csv(output_file)
        print(f'Number of output rows: {len(output_df)}')
        print(f'Output columns: {list(output_df.columns)}')
    
except Exception as e:
    print(f'Error during ETL process: {e}')
    sys.exit(1)
"
else
    echo "Running tests..."
    python -m pytest test_etl_processor.py test_integration.py -v --tb=short
fi

echo "Container execution completed."
