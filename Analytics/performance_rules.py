#!/usr/bin/env python3
"""
Performance Rules System for Analytics ETL
Handles loading and executing performance rules from UDC CSV files.
"""

import pandas as pd
import re
import os
from typing import Dict, List, Optional, Tuple
from database_config import DatabaseConfig
import logging
from datetime import datetime

# Configure logging to write to file
def setup_logging():
    """Setup logging configuration to write to file."""
    # Create logs directory if it doesn't exist
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_dir, f"performance_rules_{timestamp}.log")
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, mode='w', encoding='utf-8'),
            logging.StreamHandler()  # Also log to console
        ]
    )
    
    return log_filename

logger = logging.getLogger(__name__)

class PerformanceRules:
    """Handles performance rules loading and execution."""
    
    def __init__(self, data_directory: str = "."):
        self.data_directory = data_directory
        self.db_config = DatabaseConfig()
        self.rule_tracking_table = "rule_execution_tracking"
        
    def create_performance_rules_table(self):
        """Create the performancerules table based on UDC CSV structure."""
        try:
            # Check if old PerformanceRules table exists and drop it
            check_old_table = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'PerformanceRules'
            """
            old_table_result = self.db_config.execute_query(check_old_table)
            
            if old_table_result:
                logger.info("Dropping old PerformanceRules table...")
                drop_old_sql = 'DROP TABLE IF EXISTS "PerformanceRules" CASCADE'
                self.db_config.execute_query(drop_old_sql)
            
            # Read UDC file to get column structure
            udc_file = os.path.join(self.data_directory, "User-defined_calculations_(UDC)_CLS000.csv")
            if not os.path.exists(udc_file):
                logger.error(f"UDC file not found: {udc_file}")
                return False
                
            df = pd.read_csv(udc_file)
            
            # Create table SQL based on CSV columns
            columns = []
            for col in df.columns:
                col_name = self.clean_column_name(col)
                if col_name in ['formula', 'focal_entity', 'udc_config_name', 'description']:
                    columns.append(f'    "{col_name}" TEXT')
                elif col_name in ['modified', 'aggregation']:
                    columns.append(f'    "{col_name}" VARCHAR(50)')
                elif col_name in ['can_edit', 'data_type', 'field_type', 'owner', 'user_groups']:
                    columns.append(f'    "{col_name}" VARCHAR(100)')
                elif col_name == 'tablename':  # Handle the tablename column from CSV
                    columns.append(f'    "tablename" VARCHAR(255)')
                else:
                    columns.append(f'    "{col_name}" TEXT')
            
            # Add grouptable column as specified in requirements (only if not already present)
            if not any('grouptable' in col.lower() for col in columns):
                columns.append(f'    "grouptable" VARCHAR(255)')
            
            # Add aggregation_type column for handling different aggregation types (only if not already present)
            if not any('aggregation_type' in col.lower() for col in columns):
                columns.append(f'    "aggregation_type" VARCHAR(50)')
            
            # Add tracking columns (avoid duplicates)
            tracking_columns = [
                '    "last_executed" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP',
                '    "execution_count" INTEGER DEFAULT 0',
                '    "last_successful_execution" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP',
                '    "created_at" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP'
            ]
            
            # Only add converted_formula if it's not already in the columns
            if not any('parsed_formula' in col for col in columns):
                tracking_columns.append('    "converted_formula" TEXT')
            
            columns.extend(tracking_columns)
            
            create_sql = f"""CREATE TABLE IF NOT EXISTS "performancerules" (
{',\n'.join(columns)},
    PRIMARY KEY ("udc_config_name")
);"""
            
            self.db_config.execute_query(create_sql)
            logger.info("performancerules table created successfully")
            
            # Create rule execution tracking table
            tracking_sql = f"""CREATE TABLE IF NOT EXISTS "{self.rule_tracking_table}" (
    "rule_name" VARCHAR(255) PRIMARY KEY,
    "table_name" VARCHAR(255),
    "last_executed" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    "last_successful_execution" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    "execution_count" INTEGER DEFAULT 0,
    "last_processed_timestamp" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);"""
            
            self.db_config.execute_query(tracking_sql)
            logger.info("Rule execution tracking table created successfully")
            
            # Create rule execution results table
            self.create_rule_results_table()
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating PerformanceRules table: {e}")
            return False
    
    def create_rule_results_table(self):
        """Create the ruleexecutionresults table for storing rule execution outputs."""
        try:
            # First, check if table exists and drop it if it has the old schema
            check_sql = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'ruleexecutionresults' 
                AND column_name = 'entity'
            """
            result = self.db_config.execute_query(check_sql)
            
            if result:
                # Table exists with old schema, drop and recreate
                logger.info("Dropping existing ruleexecutionresults table with old schema...")
                drop_sql = 'DROP TABLE IF EXISTS "ruleexecutionresults" CASCADE'
                self.db_config.execute_query(drop_sql)
            
            create_sql = """CREATE TABLE IF NOT EXISTS "ruleexecutionresults" (
    "ResultsId" SERIAL PRIMARY KEY,
    "Id" TEXT NOT NULL,
    "timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
    "udc_config_name" TEXT NOT NULL,
    "udc_config_value" FLOAT,
    "created_at" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);"""
            
            self.db_config.execute_query(create_sql)
            logger.info("ruleexecutionresults table created successfully")
            
            # Create index for better query performance
            index_sql = """CREATE INDEX IF NOT EXISTS idx_rule_results_id_timestamp 
                          ON "ruleexecutionresults" ("Id", "timestamp");"""
            self.db_config.execute_query(index_sql)
            
            index_sql2 = """CREATE INDEX IF NOT EXISTS idx_rule_results_udc_config 
                           ON "ruleexecutionresults" ("udc_config_name");"""
            self.db_config.execute_query(index_sql2)
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating ruleexecutionresults table: {e}")
            return False
    
    def clean_column_name(self, col_name: str) -> str:
        """Clean column name for database compatibility."""
        col_name = col_name.strip("'\"")
        col_name = re.sub(r'[^\w\s]', '_', col_name)
        col_name = re.sub(r'_+', '_', col_name)
        col_name = col_name.strip('_')
        col_name = col_name.lower()
        return col_name
    
    def get_table_name_for_entity(self, focal_entity: str) -> str:
        """Get the appropriate table name for a focal entity."""
        if pd.isna(focal_entity) or focal_entity == 'NaN':
            return None
        
        # Map focal entities to their corresponding table names
        entity_table_mapping = {
            'ACPF': 'acpf_samsung',
            'AUPF': 'aupf_samsung', 
            'DU': 'du_samsung',
            'GNB': 'gnb_samsung',
            'SECTOR': 'sector_samsung',
            'CARRIER': 'carrier_samsung',
            'MKT': 'mkt_samsung'
        }
        
        # Try exact match first
        if focal_entity.upper() in entity_table_mapping:
            return entity_table_mapping[focal_entity.upper()]
        
        # Try partial match for entities like 'DU_Samsung', 'GNB_Samsung', etc.
        for entity, table_name in entity_table_mapping.items():
            if entity in focal_entity.upper():
                return table_name
        
        # Default fallback - try to find a table that matches the focal entity
        try:
            # Get all table names from the database
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE %s
                ORDER BY table_name
            """
            pattern = f"%{focal_entity.lower()}_%"
            result = self.db_config.execute_query(query, [pattern])
            
            if result:
                return result[0]['table_name']
        except:
            pass
        
        return None
    
    def load_rules_from_csv(self, udc_file: str = "User-defined_calculations_(UDC)_CLS000.csv"):
        """Load rules from UDC CSV file into PerformanceRules table."""
        try:
            filepath = os.path.join(self.data_directory, udc_file)
            if not os.path.exists(filepath):
                logger.error(f"UDC file not found: {filepath}")
                return False
            
            # Read UDC file
            df = pd.read_csv(filepath)
            logger.info(f"Loaded {len(df)} rules from {udc_file}")
            
            # Clean column names
            df.columns = [self.clean_column_name(col) for col in df.columns]
            
            # Convert formulas and store in parsed_formula column
            df['parsed_formula'] = df['formula'].apply(self.convert_formula_to_sql)
            
            # Map the 'tablename' column from CSV to 'tablename' for database storage
            if 'tablename' in df.columns:
                # The tablename column already exists in the CSV, use it directly
                # But handle NaN values by mapping them to appropriate table names based on focal_entity
                df['tablename'] = df.apply(lambda row: self.get_table_name_for_entity(row['focal_entity']) 
                                         if pd.isna(row['tablename']) or str(row['tablename']).upper() == 'NAN' 
                                         else row['tablename'], axis=1)
            else:
                # If no tablename column, create one based on focal_entity
                df['tablename'] = df['focal_entity'].apply(self.get_table_name_for_entity)
            
            # Add grouptable column if it doesn't exist in CSV
            if 'grouptable' not in df.columns:
                df['grouptable'] = None
            
            # Handle aggregation_type column - map the CSV column name to our expected name
            if 'aggregation_type' in df.columns:
                df['aggregation_type'] = df['aggregation_type'].fillna('NA')
                # Also handle string 'NaN' values
                df['aggregation_type'] = df['aggregation_type'].replace('NaN', 'NA')
            else:
                df['aggregation_type'] = 'NA'
            
            # Add tracking columns
            df['last_executed'] = None
            df['execution_count'] = 0
            df['last_successful_execution'] = None
            df['created_at'] = datetime.now()
            
            # Insert/update rules
            for _, rule in df.iterrows():
                self.upsert_rule(rule)
            
            logger.info(f"Successfully loaded {len(df)} rules into performancerules table")
            return True
            
        except Exception as e:
            logger.error(f"Error loading rules from CSV: {e}")
            return False
    
    def convert_formula_to_sql_for_aggregation(self, formula: str) -> str:
        """Convert Access-style formula to SQL-compatible format for aggregation queries."""
        if pd.isna(formula) or formula == '':
            return ''
        
        try:
            # Remove newlines and extra spaces
            formula = re.sub(r'\s+', ' ', str(formula).replace('\n', ' ').strip())
            
            # Convert [TABLE]![{COLUMN}] to t.COLUMN (using table alias)
            formula = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r't.\2', formula)
            
            # Convert {COLUMN} to COLUMN (standalone curly braces)
            formula = re.sub(r'\{([^}]+)\}', r'\1', formula)
            
            # Replace dots with underscores in column names, but preserve table alias
            # Convert t.COLUMN.SUBCOLUMN to t.COLUMN_SUBCOLUMN
            formula = re.sub(r'(\w+)\.(\w+)\.(\w+)', r'\1.\2_\3', formula)
            
            # Add float casting to column references for aggregation
            # Convert t.COLUMN to t.COLUMN::float
            formula = re.sub(r'\bt\.(\w+)', r't.\1::float', formula)
            
            # Convert ? : ternary operator to CASE WHEN
            def replace_ternary(match):
                condition = match.group(1)
                then_value = match.group(2).strip()
                else_value = match.group(3).strip()
                
                # Handle column references in ternary
                then_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r't.\2', then_value)
                else_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r't.\2', else_value)
                
                return f'CASE WHEN {condition} THEN {then_clause} ELSE {else_clause} END'
            
            # Use a more robust regex that handles nested parentheses
            formula = re.sub(r'\(([^()]*(?:\([^()]*\)[^()]*)*)\)\s*\?\s*([^:]+):([^)]+)', replace_ternary, formula)
            
            # Convert OR to SQL OR
            formula = re.sub(r'\bOR\b', 'OR', formula, flags=re.IGNORECASE)
            
            # Convert AND to SQL AND
            formula = re.sub(r'\bAND\b', 'AND', formula, flags=re.IGNORECASE)
            
            # Clean up extra spaces
            formula = re.sub(r'\s+', ' ', formula).strip()
            
            return formula
            
        except Exception as e:
            logger.warning(f"Error converting formula for aggregation '{formula}': {e}")
            return str(formula)

    def convert_formula_to_sql(self, formula: str) -> str:
        """Convert Access-style formula to SQL-compatible format."""
        if pd.isna(formula) or formula == '':
            return ''
        
        try:
            # Remove newlines and extra spaces
            formula = re.sub(r'\s+', ' ', str(formula).replace('\n', ' ').strip())
            
            # convert == to =
            formula = formula.replace('==', '= ')
            
            # Convert ? : ternary operator to CASE WHEN first
            # Pattern: (condition) ? value1 : value2
            # Handle both literal values and column references
            def replace_ternary(match):
                condition = match.group(1)
                then_value = match.group(2).strip()
                else_value = match.group(3).strip()
                
                # Check if then_value is a column reference (contains [TABLE]![{COLUMN}] pattern)
                if '[' in then_value and ']' in then_value and '!' in then_value:
                    # It's a column reference, convert it to TABLE.COLUMN format
                    then_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', then_value)
                elif then_value.isdigit() or (then_value.replace('.', '').replace('-', '').isdigit() and then_value.count('.') <= 1):
                    # It's a numeric literal, keep as is
                    then_clause = then_value
                else:
                    # It's a literal value, wrap in quotes
                    then_clause = f"'{then_value}'"
                
                # Check if else_value is a column reference
                if '[' in else_value and ']' in else_value and '!' in else_value:
                    # It's a column reference, convert it to TABLE.COLUMN format
                    else_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', else_value)
                elif else_value.isdigit() or (else_value.replace('.', '').replace('-', '').isdigit() and else_value.count('.') <= 1):
                    # It's a numeric literal, keep as is
                    else_clause = else_value
                else:
                    # It's a literal value, wrap in quotes
                    else_clause = f"'{else_value}'"
                
                # Ensure both clauses return the same data type
                # Check if either clause is a column reference
                # Column references can be in [TABLE]![{COLUMN}] format or just column names
                # A column reference is anything that's not a simple numeric literal or quoted string
                then_is_column = ('[' in then_value and ']' in then_value and '!' in then_value) or \
                                 (then_value and not then_value.isdigit() and not then_value.replace('.', '').replace('-', '').isdigit() and
                                  not then_value.startswith("'") and not then_value.endswith("'") and
                                  not then_value.startswith('0::') and not then_value.startswith('0.') and
                                  not then_value == '0' and not then_value == '100')
                else_is_column = ('[' in else_value and ']' in else_value and '!' in else_value) or \
                                (else_value and not else_value.isdigit() and not else_value.replace('.', '').replace('-', '').isdigit() and
                                 not else_value.startswith("'") and not else_value.endswith("'") and
                                 not else_value.startswith('0::') and not else_value.startswith('0.') and
                                 not else_value == '0' and not else_value == '100')
                
                # If one is a column reference and the other is a literal, cast the literal to match
                if then_is_column and not else_is_column:
                    # THEN is column reference, ELSE is literal - cast the literal to match column type
                    if else_value.replace('.', '').replace('-', '').isdigit():
                        # It's a numeric literal - cast the column reference to float to match
                        then_clause = f"{then_value}::float"
                        else_clause = f"{else_value}::float"
                    else:
                        # It's a string literal, cast to text
                        then_clause = f"{then_value}::text"
                        else_clause = f"'{else_value}'::text"
                elif else_is_column and not then_is_column:
                    # ELSE is column reference, THEN is literal - cast the literal to match column type
                    if then_value.replace('.', '').replace('-', '').isdigit():
                        # It's a numeric literal
                        then_clause = f"{then_value}::float"
                    else:
                        # It's a string literal, cast to text
                        then_clause = f"'{then_value}'::text"
                elif then_is_column and else_is_column:
                    # Both are column references - no casting needed
                    pass
                else:
                    # Both are literals - ensure consistent types
                    if then_value.replace('.', '').replace('-', '').isdigit() and else_value.replace('.', '').replace('-', '').isdigit():
                        # Both are numeric - no casting needed
                        pass
                    elif then_value.replace('.', '').replace('-', '').isdigit():
                        # THEN is numeric, ELSE is string - cast THEN to text
                        then_clause = f"{then_value}::text"
                    elif else_value.replace('.', '').replace('-', '').isdigit():
                        # ELSE is numeric, THEN is string - cast ELSE to text
                        else_clause = f"{else_value}::text"
                
                return f'CASE WHEN {condition} THEN {then_clause} ELSE {else_clause} END'
            
            # Use a more robust regex that handles nested parentheses
            # This pattern matches the entire condition including nested parentheses
            formula = re.sub(r'\(([^()]*(?:\([^()]*\)[^()]*)*)\)\s*\?\s*([^:]+):([^)]+)', replace_ternary, formula)
            
            # Also handle ternary operators without parentheses around the condition
            # But be more careful to avoid matching partial patterns
            # Look for patterns that end with ) or are at the end of string, followed by ? then values
            # This pattern specifically looks for ) followed by ? and values
            # First, handle the specific case where we have ) ? value1:value2
            # Use a more specific pattern that matches the entire condition ending with )
            formula = re.sub(r'([^?()]+\))\s*\?\s*([^:]+):([^)]+)', replace_ternary, formula)
            
            # Handle the specific case where we have ) ? value1:value2 (without parentheses around condition)
            # This is a more specific pattern for cases like "condition) ? 100:0"
            def replace_simple_ternary(match):
                condition = match.group(1).strip()
                then_value = match.group(2).strip()
                else_value = match.group(3).strip()
                
                # Check if then_value is a column reference
                if '[' in then_value and ']' in then_value and '!' in then_value:
                    then_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', then_value)
                elif then_value.isdigit() or (then_value.replace('.', '').replace('-', '').isdigit() and then_value.count('.') <= 1):
                    then_clause = then_value
                else:
                    then_clause = f"'{then_value}'"
                
                # Check if else_value is a column reference
                if '[' in else_value and ']' in else_value and '!' in else_value:
                    else_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', else_value)
                elif else_value.isdigit() or (else_value.replace('.', '').replace('-', '').isdigit() and else_value.count('.') <= 1):
                    else_clause = else_value
                else:
                    else_clause = f"'{else_value}'"
                
                return f'{condition}) ? {then_clause}:{else_clause}'
            
            # Apply the simple ternary conversion
            formula = re.sub(r'([^?()]+\))\s*\?\s*([^:]+):([^)]+)', replace_simple_ternary, formula)
            
            # Handle the specific case where we have ) ? value1:value2 (without parentheses around condition)
            # This is a more specific pattern for cases like "condition) ? 100:0"
            # The issue is that the previous regex might not match all cases, so let's add a more specific one
            def replace_final_ternary(match):
                condition = match.group(1).strip()
                then_value = match.group(2).strip()
                else_value = match.group(3).strip()
                
                # Check if then_value is a column reference
                if '[' in then_value and ']' in then_value and '!' in then_value:
                    then_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', then_value)
                elif then_value.isdigit() or (then_value.replace('.', '').replace('-', '').isdigit() and then_value.count('.') <= 1):
                    then_clause = then_value
                else:
                    then_clause = f"'{then_value}'"
                
                # Check if else_value is a column reference
                if '[' in else_value and ']' in else_value and '!' in else_value:
                    else_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', else_value)
                elif else_value.isdigit() or (else_value.replace('.', '').replace('-', '').isdigit() and else_value.count('.') <= 1):
                    else_clause = else_value
                else:
                    else_clause = f"'{else_value}'"
                
                return f'{condition}) ? {then_clause}:{else_clause}'
            
            # Apply the final ternary conversion with a more specific pattern
            # Debug: Log the formula before and after final ternary conversion
            logger.info(f"Before final ternary conversion: {formula}")
            
            # Handle the specific case where we have ) ? value1:value2 (without parentheses around condition)
            # This is a more specific pattern for cases like "condition) ? 100:0"
            # The issue is that the previous regex might not match all cases, so let's add a more specific one
            def replace_final_ternary_v2(match):
                condition = match.group(1).strip()
                then_value = match.group(2).strip()
                else_value = match.group(3).strip()
                
                # Check if then_value is a column reference
                if '[' in then_value and ']' in then_value and '!' in then_value:
                    then_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', then_value)
                elif then_value.isdigit() or (then_value.replace('.', '').replace('-', '').isdigit() and then_value.count('.') <= 1):
                    then_clause = then_value
                else:
                    then_clause = f"'{then_value}'"
                
                # Check if else_value is a column reference
                if '[' in else_value and ']' in else_value and '!' in else_value:
                    else_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', else_value)
                elif else_value.isdigit() or (else_value.replace('.', '').replace('-', '').isdigit() and else_value.count('.') <= 1):
                    else_clause = else_value
                else:
                    else_clause = f"'{else_value}'"
                
                return f'{condition}) ? {then_clause}:{else_clause}'
            
            # Apply the final ternary conversion with a more specific pattern
            # This pattern specifically looks for ) followed by ? and values
            # First try the original pattern
            formula = re.sub(r'([^?()]+\))\s*\?\s*([^:]+):([^)]+)', replace_final_ternary_v2, formula)
            
            # If that didn't work, try a more specific pattern for the exact case
            # Look for patterns like "condition) ? 100:0" where condition is complex
            def replace_specific_ternary(match):
                condition = match.group(1).strip()
                then_value = match.group(2).strip()
                else_value = match.group(3).strip()
                
                # Check if then_value is a column reference
                if '[' in then_value and ']' in then_value and '!' in then_value:
                    then_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', then_value)
                elif then_value.isdigit() or (then_value.replace('.', '').replace('-', '').isdigit() and then_value.count('.') <= 1):
                    then_clause = then_value
                else:
                    then_clause = f"'{then_value}'"
                
                # Check if else_value is a column reference
                if '[' in else_value and ']' in else_value and '!' in else_value:
                    else_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', else_value)
                elif else_value.isdigit() or (else_value.replace('.', '').replace('-', '').isdigit() and else_value.count('.') <= 1):
                    else_clause = else_value
                else:
                    else_clause = f"'{else_value}'"
                
                return f'{condition}) ? {then_clause}:{else_clause}'
            
            # Try a more specific pattern that matches the exact case
            # The issue is that the regex is not matching the pattern ) ? 100:0
            # Let's try a different approach - look for the specific pattern
            def replace_exact_ternary(match):
                condition = match.group(1).strip()
                then_value = match.group(2).strip()
                else_value = match.group(3).strip()
                
                # Check if then_value is a column reference
                if '[' in then_value and ']' in then_value and '!' in then_value:
                    then_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', then_value)
                elif then_value.isdigit() or (then_value.replace('.', '').replace('-', '').isdigit() and then_value.count('.') <= 1):
                    then_clause = then_value
                else:
                    then_clause = f"'{then_value}'"
                
                # Check if else_value is a column reference
                if '[' in else_value and ']' in else_value and '!' in else_value:
                    else_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', else_value)
                elif else_value.isdigit() or (else_value.replace('.', '').replace('-', '').isdigit() and else_value.count('.') <= 1):
                    else_clause = else_value
                else:
                    else_clause = f"'{else_value}'"
                
                return f'{condition}) ? {then_clause}:{else_clause}'
            
            # Try a more specific pattern that matches the exact case
            # Look for patterns like "condition) ? 100:0" where condition is complex
            # The issue is that the regex is not matching because the condition contains parentheses
            # Let's try a different approach - match the pattern ) ? value1:value2 directly
            def replace_direct_ternary(match):
                condition = match.group(1).strip()
                then_value = match.group(2).strip()
                else_value = match.group(3).strip()
                
                # Check if then_value is a column reference
                if '[' in then_value and ']' in then_value and '!' in then_value:
                    then_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', then_value)
                elif then_value.isdigit() or (then_value.replace('.', '').replace('-', '').isdigit() and then_value.count('.') <= 1):
                    then_clause = then_value
                else:
                    then_clause = f"'{then_value}'"
                
                # Check if else_value is a column reference
                if '[' in else_value and ']' in else_value and '!' in else_value:
                    else_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', else_value)
                elif else_value.isdigit() or (else_value.replace('.', '').replace('-', '').isdigit() and else_value.count('.') <= 1):
                    else_clause = else_value
                else:
                    else_clause = f"'{else_value}'"
                
                return f'{condition}) ? {then_clause}:{else_clause}'
            
            # Try a more direct approach - match ) ? value1:value2 pattern and convert to CASE
            # The issue is that I need to match the entire pattern including the condition before )
            def replace_direct_ternary_case(match):
                condition = match.group(1).strip()
                then_value = match.group(2).strip()
                else_value = match.group(3).strip()
                
                # Check if then_value is a column reference
                if '[' in then_value and ']' in then_value and '!' in then_value:
                    then_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', then_value)
                elif then_value.isdigit() or (then_value.replace('.', '').replace('-', '').isdigit() and then_value.count('.') <= 1):
                    then_clause = then_value
                else:
                    then_clause = f"'{then_value}'"
                
                # Check if else_value is a column reference
                if '[' in else_value and ']' in else_value and '!' in else_value:
                    else_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', else_value)
                elif else_value.isdigit() or (else_value.replace('.', '').replace('-', '').isdigit() and else_value.count('.') <= 1):
                    else_clause = else_value
                else:
                    else_clause = f"'{else_value}'"
                
                # Convert to CASE WHEN ... THEN ... ELSE ... END
                return f'CASE WHEN {condition} THEN {then_clause} ELSE {else_clause} END'
            
            # Try a more direct approach - match condition) ? value1:value2 pattern
            # This regex handles nested parentheses by counting them
            def match_ternary_with_nested_parens(text):
                # Find the last ? in the string
                question_pos = text.rfind('?')
                if question_pos == -1:
                    return text
                
                # Find the matching closing parenthesis before the ?
                paren_count = 0
                start_pos = -1
                for i in range(question_pos - 1, -1, -1):
                    if text[i] == ')':
                        paren_count += 1
                        if start_pos == -1:
                            start_pos = i
                    elif text[i] == '(':
                        paren_count -= 1
                        if paren_count == 0 and start_pos != -1:
                            # Found the matching opening parenthesis
                            condition = text[i+1:start_pos]
                            # Find the : after the ?
                            colon_pos = text.find(':', question_pos)
                            if colon_pos != -1:
                                then_value = text[question_pos+1:colon_pos].strip()
                                else_value = text[colon_pos+1:].strip()
                                
                                # Convert to CASE WHEN
                                then_clause = then_value
                                else_clause = else_value
                                
                                # Check if values are column references or literals
                                if '[' in then_value and ']' in then_value and '!' in then_value:
                                    then_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', then_value)
                                elif not (then_value.isdigit() or (then_value.replace('.', '').replace('-', '').isdigit() and then_value.count('.') <= 1)):
                                    then_clause = f"'{then_value}'"
                                
                                if '[' in else_value and ']' in else_value and '!' in else_value:
                                    else_clause = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', else_value)
                                elif not (else_value.isdigit() or (else_value.replace('.', '').replace('-', '').isdigit() and else_value.count('.') <= 1)):
                                    else_clause = f"'{else_value}'"
                                
                                return text[:i] + f'CASE WHEN {condition} THEN {then_clause} ELSE {else_clause} END'
                            break
                return text
            
            formula = match_ternary_with_nested_parens(formula)
            logger.info(f"After final ternary conversion: {formula}")
            
            # Handle simple ternary operators like "condition ? value1:value2" (without parentheses)
            formula = re.sub(r'([^?()]+)\s*\?\s*([^:]+):([^)]+)', replace_ternary, formula)
            
            # Add division by zero protection BEFORE column name conversion
            # Pattern: ([TABLE]![{COLUMN1}]/[TABLE]![{COLUMN2}]) -> (CASE WHEN [TABLE]![{COLUMN2}] = 0 OR [TABLE]![{COLUMN2}] IS NULL THEN 0 ELSE [TABLE]![{COLUMN1}] / [TABLE]![{COLUMN2}] END)
            def protect_division(match):
                numerator = match.group(1).strip()
                denominator = match.group(2).strip()
                return f'(CASE WHEN {denominator} = 0 OR {denominator} IS NULL THEN 0 ELSE {numerator} / {denominator} END)'
            
            # Protect division operations in the original format
            # Look for patterns like: [TABLE]![{COLUMN1}]/[TABLE]![{COLUMN2}]
            formula = re.sub(r'(\[[^\]]+\]!\[\{[^}]+\}\])\s*/\s*(\[[^\]]+\]!\[\{[^}]+\}\])', protect_division, formula)
            
            # Convert [TABLE]![{COLUMN}] to TABLE.COLUMN (for remaining references)
            formula = re.sub(r'\[([^\]]+)\]!\[\{([^}]+)\}\]', r'\1.\2', formula)
            
            # Fix duplicate table names (e.g., ACPF.ACPF.column -> ACPF.column)
            formula = re.sub(r'(\w+)\.\1\.', r'\1.', formula)
            
            # Convert {COLUMN} to COLUMN (standalone curly braces)
            formula = re.sub(r'\{([^}]+)\}', r'\1', formula)
            
            # Convert OR to SQL OR
            formula = re.sub(r'\bOR\b', 'OR', formula, flags=re.IGNORECASE)
            
            # Convert AND to SQL AND
            formula = re.sub(r'\bAND\b', 'AND', formula, flags=re.IGNORECASE)
            
            # Clean up extra spaces
            formula = re.sub(r'\s+', ' ', formula).strip()

            # replace dots with underscores and convert to lowercase
            # But preserve decimal numbers (e.g., .50 should stay as .50, not _50)
            # First, protect decimal numbers by temporarily replacing them
            # Look for decimal numbers that are standalone (not part of column names)
            # Match decimal numbers that are preceded by space, operator, or at start, and followed by space, operator, parenthesis, or at end
            decimal_pattern = r'(?<=[\s>=<()*/+-])\d*\.\d+(?=[\s>=<()*/+-]|$)'
            decimals = re.findall(decimal_pattern, formula)
            formula = re.sub(decimal_pattern, 'DECIMAL_PLACEHOLDER', formula)
            
            # Now replace remaining dots with underscores and convert to lowercase
            formula = formula.replace('.', '_').lower()
            
            # Restore decimal numbers
            for decimal in decimals:
                formula = formula.replace('decimal_placeholder', decimal, 1)
            
            # Convert SQL keywords back to uppercase
            formula = re.sub(r'\bcase\b', 'CASE', formula, flags=re.IGNORECASE)
            formula = re.sub(r'\bwhen\b', 'WHEN', formula, flags=re.IGNORECASE)
            formula = re.sub(r'\bthen\b', 'THEN', formula, flags=re.IGNORECASE)
            formula = re.sub(r'\belse\b', 'ELSE', formula, flags=re.IGNORECASE)
            formula = re.sub(r'\bend\b', 'END', formula, flags=re.IGNORECASE)
            
            # Add type casting for numeric comparisons to prevent data type errors
            # This handles cases where we compare columns with numeric values
            # Pattern: column_name operator number -> column_name::float operator number
            def add_numeric_casting(match):
                column = match.group(1)
                operator = match.group(2)
                value = match.group(3)
                return f'{column}::float {operator} {value}'
            
            # Match column names followed by comparison operators and numeric values
            # This covers patterns like: column_name != 0, column_name > 100, etc.
            formula = re.sub(r'([a-z_]+)\s*([<>=!]+)\s*(\d+(?:\.\d+)?)', add_numeric_casting, formula)
            
            return formula
            
        except Exception as e:
            logger.warning(f"Error converting formula '{formula}': {e}")
            return str(formula)
    
    def test_formula_conversion(self):
        """Test the formula conversion with sample formulas from requirements."""
        test_cases = [
            {
                'input': '([DU]![{DU.5GNR.ENDC.Drop.Pct}]>= 80 AND [DU]![{DU.S5NR.MaxRRCconnectedUsersPerDU.number}] > 10 AND [DU]![{DU.5GNR.ENDC.Drop.pct.num}] > 50) ? 100:0',
                'expected_pattern': 'CASE WHEN du_5gnr_endc_drop_pct>= 80 AND du_s5nr_maxrrcconnectedusersperdu_number > 10 AND du_5gnr_endc_drop_pct_num > 50 THEN \'100\' ELSE \'0\' END'
            },
            {
                'input': '([ACPF]![{ssum_ACPF.ENDCAddsucc}]/[ACPF]![{ssum_ACPF.ENDCAddAtt}])*100',
                'expected_pattern': '(ssum_acpf_endcaddsucc/ssum_acpf_endcaddatt)*100'
            },
            {
                'input': '([AUPF]![{AUPF.CpuUsageMax.percent}]>92.5 OR [AUPF]![{AUPF.MemUsageMax.percent}]>92.5)? 100:0',
                'expected_pattern': 'CASE WHEN aupf_cpuusagemax_percent>92.5 OR aupf_memusagemax_percent>92.5 THEN \'100\' ELSE \'0\' END'
            },
            {
                'input': '([DU]![{DU.S5NR.MaxRRCconnectedUsersPerDU.number}] > 1 AND [DU]![{DU.5GNR.ENDC.Drop.pct.num}]  > 50) ? [DU]![{DU.5GNR.ENDC.Drop.Pct}] : 0',
                'expected_pattern': 'CASE WHEN du_s5nr_maxrrcconnectedusersperdu_number > 1 AND du_5gnr_endc_drop_pct_num > 50 THEN du_5gnr_endc_drop_pct ELSE \'0\' END'
            }
        ]
        
        logger.info("Testing formula conversion...")
        for i, test_case in enumerate(test_cases, 1):
            result = self.convert_formula_to_sql(test_case['input'])
            logger.info(f"Test {i}:")
            logger.info(f"  Input: {test_case['input']}")
            logger.info(f"  Output: {result}")
            logger.info(f"  Expected pattern: {test_case['expected_pattern']}")
            logger.info("---")
        
        return True
    
    def get_last_execution_time(self, rule_name: str) -> Optional[datetime]:
        """Get the last execution time for a rule."""
        try:
            query = """
                SELECT "last_executed" 
                FROM "rule_execution_tracking" 
                WHERE "rule_name" = %s
            """
            result = self.db_config.execute_query(query, [rule_name])
            if result and result[0]['last_executed']:
                return result[0]['last_executed']
            return None
        except Exception as e:
            logger.warning(f"Error getting last execution time for rule {rule_name}: {e}")
            return None
    
    def get_rule_creation_time(self, rule_name: str) -> Optional[datetime]:
        """Get the creation time of a rule for incremental processing."""
        try:
            query = """
                SELECT "created_at" 
                FROM "performancerules" 
                WHERE "udc_config_name" = %s
            """
            result = self.db_config.execute_query(query, [rule_name])
            if result and result[0]['created_at']:
                return result[0]['created_at']
            return None
        except Exception as e:
            logger.warning(f"Error getting creation time for rule {rule_name}: {e}")
            return None
    
    def upsert_rule(self, rule: pd.Series):
        """Insert or update a rule in the PerformanceRules table."""
        try:
            # Prepare values for insertion
            values = []
            placeholders = []
            
            for col in rule.index:
                if col in ['last_executed', 'execution_count', 'last_successful_execution', 'created_at']:
                    continue  # Skip tracking columns for initial load
                values.append(rule[col])
                placeholders.append('%s')
            
            # Add default values for tracking columns
            values.extend([None, 0, None, datetime.now()])
            placeholders.extend(['%s', '%s', '%s', '%s'])   
            
            # Create upsert query
            columns = list(rule.index) 
            columns = [f'"{col}"' for col in columns]
            
            insert_sql = f"""
                INSERT INTO "performancerules" ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT ("udc_config_name") 
                DO UPDATE SET
                    "formula" = EXCLUDED."formula",
                    "parsed_formula" = EXCLUDED."parsed_formula",
                    "focal_entity" = EXCLUDED."focal_entity",
                    "description" = EXCLUDED."description",
                    "tablename" = EXCLUDED."tablename",
                    "grouptable" = EXCLUDED."grouptable",
                    "aggregation_type" = EXCLUDED."aggregation_type"
            """
            logger.info("insert_sql: " + insert_sql)
            self.db_config.execute_query(insert_sql, values)
            
        except Exception as e:
            logger.error(f"Error upserting rule {rule.get('udc_config_name', 'unknown')}: {e}")
    
    def get_rules_for_entity(self, focal_entity: str) -> List[Dict]:
        """Get all rules for a specific focal entity."""
        try:
            query = """
                SELECT * FROM "performancerules" 
                WHERE "focal_entity" = %s 
                AND "parsed_formula" IS NOT NULL 
                AND "parsed_formula" != ''
                ORDER BY "udc_config_name"
            """
            
            result = self.db_config.execute_query(query, [focal_entity])
            return result
            
        except Exception as e:
            logger.error(f"Error getting rules for entity {focal_entity}: {e}")
            return []
    
    def find_matching_tables(self, focal_entity: str, rule_name: str) -> List[str]:
        """Find tables that match the focal entity pattern using tablename from rule."""
        try:
            # Get table name from the rule's tablename column
            query = """
                SELECT "tablename" 
                FROM "performancerules" 
                WHERE "udc_config_name" = %s
            """
            result = self.db_config.execute_query(query, [rule_name])
            
            if result and result[0]['tablename']:
                table_name = result[0]['tablename']
                logger.info(f"Table name from rule tablename: {table_name}")
                return [table_name]
            else:
                logger.warning(f"No tablename found for rule: {rule_name}, falling back to pattern matching")
                
                # Fallback to pattern matching if no tablename specified
                query = """
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND UPPER(table_name) LIKE UPPER(%s)
                    ORDER BY table_name
                """
                
                pattern = f"%{focal_entity}_%"
                result = self.db_config.execute_query(query, [pattern])
                
                return [row['table_name'] for row in result]
            
        except Exception as e:
            logger.error(f"Error finding matching tables for {focal_entity}: {e}")
            return []

    
    def auto_cast_numeric_conditions(self, query: str) -> str:
        """
        Automatically adds ::float to columns in WHERE/CASE conditions 
        if they are compared against numeric constants or used in arithmetic operations.
        """
        # First, protect quoted strings and date strings from modification
        # Find and temporarily replace quoted strings and date patterns
        protected_parts = []
        
        # Protect quoted strings (single and double quotes)
        def protect_quoted(match):
            protected_parts.append(match.group(0))
            return f"__PROTECTED_QUOTED_{len(protected_parts)-1}__"
        
        # Protect quoted strings
        query = re.sub(r"'[^']*'", protect_quoted, query)
        query = re.sub(r'"[^"]*"', protect_quoted, query)
        
        # Protect date patterns (YYYY-MM-DD HH:MM:SS)
        def protect_date(match):
            protected_parts.append(match.group(0))
            return f"__PROTECTED_DATE_{len(protected_parts)-1}__"
        
        query = re.sub(r'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}', protect_date, query)
        
        # Now apply the type casting patterns
        # First, handle arithmetic operations (/, *, +, -) between columns
        # But avoid columns that already have type casting
        arithmetic_pattern = re.compile(r'(\b\w+\b)\s*([/*+\-])\s*(\b\w+\b)')
        
        def arithmetic_replacer(match):
            col1, op, col2 = match.groups()
            # Check if columns already have type casting
            if '::' in col1 or '::' in col2:
                return match.group(0)  # Return original if already has casting
            # Add ::float to both columns for arithmetic operations
            return f"{col1}::float {op} {col2}::float"
        
        query = arithmetic_pattern.sub(arithmetic_replacer, query)
        
        # Then, handle comparison operations (>=, <=, =, >, <) with numeric constants
        # But avoid columns that already have type casting
        # Use a more specific pattern that looks for the full context
        comparison_pattern = re.compile(r'(\b\w+(?:::\w+)?)\s*(>=|<=|=|>|<)\s*([0-9]*\.?[0-9]+)')
        
        def comparison_replacer(match):
            col, op, num = match.groups()
            # Check if column already has type casting
            if '::' in col:
                return match.group(0)  # Return original if already has casting
            # Add ::float to column, leave number as is
            return f"{col}::float {op} {num}"
        
        query = comparison_pattern.sub(comparison_replacer, query)
        
        # Restore protected parts
        for i, protected_part in enumerate(protected_parts):
            query = query.replace(f"__PROTECTED_QUOTED_{i}__", protected_part)
            query = query.replace(f"__PROTECTED_DATE_{i}__", protected_part)
        
        return query
    
    def execute_aggregate_rule(self, rule: Dict, table_name: str, group_table_name: str) -> Optional[pd.DataFrame]:
        """Execute a rule with aggregation logic when aggregation type is 'Aggregate'."""
        try:
            rule_name = rule['udc_config_name']
            formula = rule['formula']
            aggregation = rule.get('aggregation', 'Average')
            
            if not formula or formula.strip() == '':
                logger.warning(f"Rule {rule_name} has no valid formula")
                return None
            
            # Convert formula to SQL format for aggregation
            converted_formula = self.convert_formula_to_sql_for_aggregation(formula)
            logger.info(f"Converted formula for aggregate rule {rule_name}: {converted_formula}")
            
            # Get last execution time for incremental processing
            last_execution = self.get_last_execution_time(rule_name)
            
            # Build WHERE clause for timestamp filtering
            timestamp_filter = ""
            if last_execution:
                timestamp_filter = f' AND t."created_at" > \'{last_execution.strftime("%Y-%m-%d %H:%M:%S")}\''
                logger.info(f"Using last execution time {last_execution} for rule {rule_name}")
            else:    
                # Get rule creation time for incremental processing
                rule_creation_time = self.get_rule_creation_time(rule_name)
                if rule_creation_time:
                    timestamp_filter = f' AND t."created_at" >= \'{rule_creation_time.strftime("%Y-%m-%d %H:%M:%S")}\''
                    logger.info(f"Filtering data from rule creation time {rule_creation_time} for rule {rule_name}")
            
            # Build the aggregation query with join between tablename and grouptable
            # Join condition: grouptable.gnbid = tablename.id AND grouptable.type = tablename.focaltype
            # Group by: grouptable.typeid and tablename.timestamp
            # Select: formula, aggregation, typeid, timestamp
            
            # Determine aggregation function
            agg_function = "AVG"
            if aggregation.lower() == 'sum':
                agg_function = "SUM"
            elif aggregation.lower() == 'max':
                agg_function = "MAX"
            elif aggregation.lower() == 'min':
                agg_function = "MIN"
            elif aggregation.lower() == 'count':
                agg_function = "COUNT"
            
            # Build the main query according to the requirements:
            # 1. Inner join between tablename and grouptable on gnbid = id
            # 2. Include type in grouptable = focalentity from performancerules table
            # 3. Group by typeid and timestamp
            # 4. Use typeid, timestamp, and aggregation of formula in select
            focal_entity = rule.get('focal_entity', '')
            select_sql = f"""
                SELECT 
                    g.typeid,
                    t.timestamp,
                    {agg_function}({converted_formula}) as calculated_value,
                    COUNT(*) as record_count
                FROM "{table_name}" t
                INNER JOIN "{group_table_name}" g 
                    ON g.gnb = t.id 
                    AND g.type = '{focal_entity}'
                WHERE ({converted_formula}) IS NOT NULL{timestamp_filter}
                GROUP BY g.typeid, t.timestamp
                ORDER BY g.typeid, t.timestamp
            """
            
            # Don't apply auto casting for aggregation queries as it can cause syntax errors
            logger.info(f"Executing aggregate SQL for rule {rule_name}: {select_sql}")
            
            # Execute query
            result = self.db_config.execute_query(select_sql)
            if result:
                # Convert to DataFrame
                df = pd.DataFrame(result)
                logger.info(f"Aggregate rule {rule_name} executed successfully: {len(df)} grouped results")
                return df, True  # success
            else:
                logger.info(f"Aggregate rule {rule_name} executed: No matching rows")
                return pd.DataFrame(), True  # successful, but no results
                
        except Exception as e:
            logger.warning(f"Error executing aggregate rule {rule.get('udc_config_name', 'unknown')}: {e}")
            return None, False  # error

    def execute_aggformula_rule(self, rule: Dict, table_name: str) -> Optional[pd.DataFrame]:
        """Execute a rule with AggFormula logic when aggregation type is 'AggFormula'."""
        try:
            rule_name = rule['udc_config_name']
            formula = rule['formula']
            tablename = rule.get('tablename', 'ruleexecutionresults')
            
            if not formula or formula.strip() == '':
                logger.warning(f"Rule {rule_name} has no valid formula")
                return None
            
            # Convert the formula to SQL format first, then replace dots with underscores
            converted_formula = self.convert_formula_to_sql(formula)
            converted_formula = converted_formula.replace('.', '_')
            logger.info(f"Converted formula for AggFormula rule {rule_name}: {converted_formula}")
            
            # Get last execution time for incremental processing
            last_execution = self.get_last_execution_time(rule_name)
            
            # Build WHERE clause for timestamp filtering
            timestamp_filter = ""
            if last_execution:
                timestamp_filter = f' AND "created_at" > \'{last_execution.strftime("%Y-%m-%d %H:%M:%S")}\''
                logger.info(f"Using last execution time {last_execution} for rule {rule_name}")
            else:    
                # Get rule creation time for incremental processing
                rule_creation_time = self.get_rule_creation_time(rule_name)
                if rule_creation_time:
                    timestamp_filter = f' AND "created_at" >= \'{rule_creation_time.strftime("%Y-%m-%d %H:%M:%S")}\''
                    logger.info(f"Filtering data from rule creation time {rule_creation_time} for rule {rule_name}")
            
            # For AggFormula, we need to query the ruleexecutionresults table horizontally
            # The table stores data with udc_config_name as field names and udc_config_value as values
            # We need to pivot the data and evaluate the formula
            
            # Extract field names from the original formula (before dot replacement)
            # Look for patterns like [TABLE]![{FIELD}] and extract just the FIELD part
            import re
            field_pattern = r'\[([^\]]+)\]!\[\{([^}]+)\}\]'
            field_matches = re.findall(field_pattern, formula)
            field_names = [field for table, field in field_matches]
            
            # If no matches found, try a simpler pattern for field names
            if not field_names:
                field_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_.]*)\b'
                field_names = re.findall(field_pattern, formula)
                
                # Remove SQL keywords and common words
                sql_keywords = {'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AVG', 'SUM', 'MAX', 'MIN', 'COUNT', 'GROUP', 'BY', 'ORDER', 'HAVING', 'IS', 'NOT', 'NULL', 'TRUE', 'FALSE'}
                field_names = [f for f in field_names if f.upper() not in sql_keywords and not f.isdigit()]
            
            # Remove duplicates while preserving order
            field_names = list(dict.fromkeys(field_names))
            
            logger.info(f"Field names extracted from formula: {field_names}")
            
            if not field_names:
                logger.warning(f"No field names found in formula for rule {rule_name}")
                return None
            
            # Create CASE statements for each field to pivot the data
            # Use original field names for comparison in udc_config_name
            case_statements = []
            for field in field_names:
                # Convert field name for column alias (replace dots with underscores)
                # PostgreSQL has a 63-character limit for identifiers, so truncate if necessary
                converted_field = field.replace('.', '_')
                if len(converted_field) > 63:
                    # Truncate to 63 characters and add a hash to make it unique
                    import hashlib
                    hash_suffix = hashlib.md5(converted_field.encode()).hexdigest()[:8]
                    converted_field = converted_field[:55] + '_' + hash_suffix
                case_statements.append(f"""
                    MAX(CASE WHEN "udc_config_name" = '{field}' THEN "udc_config_value"::float ELSE NULL END) as {converted_field}
                """)
            
            # Build the query that pivots the horizontal data and evaluates the formula
            # Use a CTE (Common Table Expression) to first pivot the data, then evaluate the formula
            # In the outer SELECT, just reference the column aliases from the CTE
            column_aliases = []
            field_mapping = {}  # Map original field names to truncated aliases
            for field in field_names:
                converted_field = field.replace('.', '_')
                if len(converted_field) > 63:
                    # Truncate to 63 characters and add a hash to make it unique
                    import hashlib
                    hash_suffix = hashlib.md5(converted_field.encode()).hexdigest()[:8]
                    converted_field = converted_field[:55] + '_' + hash_suffix
                column_aliases.append(converted_field)
                field_mapping[field] = converted_field
            
            # Update the formula to use truncated column names
            updated_formula = converted_formula
            
            # Create a mapping from the converted field names in the formula to the truncated column names
            formula_field_mapping = {}
            for original_field, truncated_field in field_mapping.items():
                if original_field != truncated_field:
                    # The formula uses converted field names (lowercase with underscores)
                    # We need to map from the formula field names to the truncated column names
                    formula_field_name = original_field.replace('.', '_').lower()
                    formula_field_mapping[formula_field_name] = truncated_field
                    
                    # Also handle the case where the formula might have table prefixes
                    # The formula conversion might add table prefixes like 'gnb_' or 'aupf_'
                    for table_prefix in ['gnb_', 'aupf_', 'du_', 'corningcarrier_']:
                        prefixed_field_name = table_prefix + formula_field_name
                        formula_field_mapping[prefixed_field_name] = truncated_field
            
            # Replace the field names in the formula
            logger.info(f"Formula field mapping: {formula_field_mapping}")
            logger.info(f"Original formula: {converted_formula}")
            
            # The formula has table prefixes added by the conversion logic (gnb_, aupf_, etc.)
            # We need to replace each prefixed field name with its truncated version (without the table prefix)
            # For example: gnb_tsum_day_gnb_... -> tsum_day_GNB_..._hash
            
            updated_formula = converted_formula
            
            # Replace each field name in the formula
            for original_field in field_names:
                # Use the same conversion as the CTE column alias generation (without lowercase)
                converted_field_for_hash = original_field.replace('.', '_')
                
                if len(converted_field_for_hash) > 63:
                    # Truncate to 63 characters and add a hash to make it unique
                    import hashlib
                    hash_suffix = hashlib.md5(converted_field_for_hash.encode()).hexdigest()[:8]
                    truncated_field = converted_field_for_hash[:55] + '_' + hash_suffix
                else:
                    # Use the original field name with dots replaced by underscores
                    truncated_field = original_field.replace('.', '_')
                
                # The formula uses lowercase field names, so we need to match against that
                converted_field_lowercase = original_field.replace('.', '_').lower()
                
                # The formula conversion adds table prefixes to field names
                # We need to replace the prefixed version with the truncated version (without prefix)
                for table_prefix in ['gnb_', 'aupf_', 'du_', 'corningcarrier_']:
                    prefixed_field = table_prefix + converted_field_lowercase
                    if prefixed_field in updated_formula:
                        logger.info(f"Replacing '{prefixed_field}' with '{truncated_field}'")
                        updated_formula = updated_formula.replace(prefixed_field, truncated_field)
                        break  # Only replace the first matching prefix
                    
            logger.info(f"Updated formula: {updated_formula}")
            pivot_sql = f"""
                WITH pivoted_data AS (
                    SELECT 
                        "Id",
                        "timestamp",
                        {','.join(case_statements)}
                    FROM "{tablename}"
                    WHERE "udc_config_name" IN ({','.join([f"'{f}'" for f in field_names])}){timestamp_filter}
                    GROUP BY "Id", "timestamp"
                )
                SELECT 
                    "Id",
                    "timestamp",
                    {','.join(column_aliases)},
                    ({updated_formula}) as calculated_value
                FROM pivoted_data
                WHERE ({updated_formula}) IS NOT NULL
                ORDER BY "Id", "timestamp"
            """
            
            logger.info(f"Executing AggFormula SQL for rule {rule_name}: {pivot_sql}")
            
            # Execute query
            result = self.db_config.execute_query(pivot_sql)
            if result:
                # Convert to DataFrame
                df = pd.DataFrame(result)
                logger.info(f"AggFormula rule {rule_name} executed successfully: {len(df)} results")
                return df, True  # success
            else:
                logger.info(f"AggFormula rule {rule_name} executed: No matching rows")
                return pd.DataFrame(), True  # successful, but no results
                
        except Exception as e:
            logger.warning(f"Error executing AggFormula rule {rule.get('udc_config_name', 'unknown')}: {e}")
            return None, False  # error

    def execute_rule(self, rule: Dict, table_name: str) -> Optional[pd.DataFrame]:
        """Execute a single rule against a table with conditional logic and timestamp filtering."""
        try:
            rule_name = rule['udc_config_name']
            formula = rule['formula']
            aggregation_type = rule.get('aggregation_type', 'NA')
            grouptable = rule.get('grouptable')
            
            if not formula or formula.strip() == '':
                logger.warning(f"Rule {rule_name} has no valid formula")
                return None
            
            # Check if this is an aggregate rule
            if aggregation_type == 'Aggregate' and grouptable:
                logger.info(f"Executing aggregate rule {rule_name} with grouptable {grouptable}")
                return self.execute_aggregate_rule(rule, table_name, grouptable)
            
            # Check if this is an AggFormula rule (case-insensitive)
            if aggregation_type and aggregation_type.lower() == 'aggformula':
                logger.info(f"Executing AggFormula rule {rule_name}")
                return self.execute_aggformula_rule(rule, table_name)
            
            # Convert formula to SQL format
            converted_formula = self.convert_formula_to_sql(formula)
            logger.info(f"Converted formula for rule {rule_name}: {converted_formula}")
            
            # use last execution time 
            last_execution = self.get_last_execution_time(rule_name)
            
            # Build WHERE clause for timestamp filtering
            timestamp_filter = ""
            if last_execution:
                timestamp_filter = f' AND "created_at" > \'{last_execution.strftime("%Y-%m-%d %H:%M:%S")}\''
                logger.info(f"Using last execution time {last_execution} for rule {rule_name}")
            else:    
                # Get rule creation time for incremental processing
                rule_creation_time = self.get_rule_creation_time(rule_name)
                if rule_creation_time:
                    timestamp_filter = f' AND "created_at" >= \'{rule_creation_time.strftime("%Y-%m-%d %H:%M:%S")}\''
                    logger.info(f"Filtering data from rule creation time {rule_creation_time} for rule {rule_name}")
            
            # Check if the formula contains a condition (has ? : operator)
            has_condition = '?' in formula and ':' in formula
            
            if has_condition:
                # For conditional formulas, we need to evaluate the condition and return rows where it's true
                # Extract the condition part (before the ?)
                condition_match = re.search(r'\(([^)]+)\)\s*\?', formula)
                if condition_match:
                    condition = self.convert_formula_to_sql(condition_match.group(1))
                    # Build SELECT query that evaluates the condition
                    select_sql = f"""
                        SELECT *, 
                               ({converted_formula}) as calculated_value,
                               ({condition}) as condition_result
                        FROM "{table_name}"
                        WHERE ({condition}) = true{timestamp_filter}
                    """
                else:
                    # Try alternative pattern for conditions without parentheses
                    condition_match = re.search(r'([^?]+)\s*\?', formula)
                    if condition_match:
                        condition = self.convert_formula_to_sql(condition_match.group(1).strip())
                        select_sql = f"""
                            SELECT *, 
                                   ({converted_formula}) as calculated_value,
                                   ({condition}) as condition_result
                            FROM "{table_name}"
                            WHERE ({condition}) = true{timestamp_filter}
                        """
                    else:
                        # Fallback to original logic if condition extraction fails
                        select_sql = f"""
                            SELECT *, 
                                   ({converted_formula}) as calculated_value
                            FROM "{table_name}"
                            WHERE ({converted_formula}) IS NOT NULL{timestamp_filter}
                        """
            else:
                # For non-conditional formulas, calculate the value and filter non-null results
                select_sql = f"""
                    SELECT *, 
                               ({converted_formula}) as calculated_value
                    FROM "{table_name}"
                    WHERE ({converted_formula}) IS NOT NULL{timestamp_filter}
                """
            
            select_sql = self.auto_cast_numeric_conditions(select_sql.lower())
            logger.info(f"Executing SQL for rule {rule_name}: {select_sql.lower()}")
            
            # Execute query
            result = self.db_config.execute_query(select_sql)
            #result = []
            if result:
                # Convert to DataFrame
                df = pd.DataFrame(result)
                logger.info(f"Rule {rule_name} executed successfully on {table_name}: {len(df)} matching rows")
                return df, True # success
            else:
                logger.info(f"Rule {rule_name} executed on {table_name}: No matching rows")
                return pd.DataFrame(), True #  successful, but no results
                
        except Exception as e:
            logger.warning(f"Error executing rule {rule.get('udc_config_name', 'unknown')} on {table_name}: {e}")
            return None, False # error
    
    def save_rule_results(self, rule_name: str, table_name: str, results_df: pd.DataFrame):
        """Save rule execution results to database table."""
        try:
            if results_df.empty:
                logger.info(f"No results to save for rule {rule_name}")
                return
            
            # Insert results into ruleexecutionresults table
            insert_sql = """
                INSERT INTO "ruleexecutionresults" 
                ("Id", "timestamp", "udc_config_name", "udc_config_value")
                VALUES (%s, %s, %s, %s)
            """
            
            rows_inserted = 0
            for _, row in results_df.iterrows():
                # For aggregate results, use typeid as the ID
                row_id = None
                if 'typeid' in row and row['typeid'] is not None:
                    row_id = str(row['typeid'])
                else:
                    # Fallback to common ID column names for non-aggregate results
                    for id_col in ['id', 'Id', 'ID', 'primary_key', 'key', 'uuid']:
                        if id_col in row and row[id_col] is not None:
                            row_id = str(row[id_col])
                            break
                
                if row_id is None:
                    logger.warning(f"No ID found in row for rule {rule_name}, skipping row")
                    continue
                
                # Use the timestamp from the data row, or current time if not available
                row_timestamp = row.get('timestamp', row.get('created_date', datetime.now()))
                
                # Get the calculated value
                calculated_value = row.get('calculated_value')
                if calculated_value is not None:
                    try:
                        # Convert to float if possible
                        if isinstance(calculated_value, str):
                            # Remove quotes if present
                            calculated_value = calculated_value.strip("'\"")
                        calculated_value = float(calculated_value)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert calculated value to float: {calculated_value}")
                        calculated_value = None
                
                self.db_config.execute_query(insert_sql, [
                    row_id,
                    row_timestamp,
                    rule_name,
                    calculated_value
                ])
                rows_inserted += 1
            
            logger.info(f"Rule results saved to database: {rows_inserted} rows for rule {rule_name}")
            
        except Exception as e:
            logger.error(f"Error saving rule results for {rule_name}: {e}")
    
    def update_rule_tracking(self, rule_name: str, table_name: str, success: bool = True):
        """Update rule execution tracking."""
        try:
            now = datetime.now()
            
            if success:
                update_sql = """
                    INSERT INTO "rule_execution_tracking" 
                    ("rule_name", "table_name", "last_executed", "last_successful_execution", "execution_count")
                    VALUES (%s, %s, %s, %s, 1)
                    ON CONFLICT ("rule_name") 
                    DO UPDATE SET
                        "last_executed" = EXCLUDED."last_executed",
                        "last_successful_execution" = EXCLUDED."last_successful_execution",
                        "execution_count" = "rule_execution_tracking"."execution_count" + 1
                """
                logger.info(f"update_sql: {update_sql}")
                self.db_config.execute_query(update_sql, [rule_name, table_name, now, now])
                logger.info(f"update_sql executed successfully")
            else:
                update_sql = """
                    INSERT INTO "rule_execution_tracking" 
                    ("rule_name", "table_name", "last_executed", "execution_count")
                    VALUES (%s, %s, %s, 1)
                    ON CONFLICT ("rule_name") 
                    DO UPDATE SET
                        "last_executed" = EXCLUDED."last_executed",
                        "execution_count" = "rule_execution_tracking"."execution_count" + 1
                """
                self.db_config.execute_query(update_sql, [rule_name, table_name, now])
                
        except Exception as e:
            logger.error(f"Error updating rule tracking for {rule_name}: {e}")

    
    def execute_all_rules(self):
        """Execute all performance rules."""
        try:
            logger.info("Starting performance rules execution...")
            
            # Get all unique focal entities
            query = """
                SELECT DISTINCT "focal_entity" 
                FROM "performancerules" 
                WHERE "focal_entity" IS NOT NULL
            """
            entities = self.db_config.execute_query(query)
            
            total_rules_executed = 0
            total_results_saved = 0
            
            for entity_row in entities:
                focal_entity = entity_row['focal_entity']

                logger.info(f"Processing rules for focal entity: {focal_entity}")
                
                # Get rules for this entity
                rules = self.get_rules_for_entity(focal_entity)
                logger.info(f"Found {len(rules)} rules for focal entity: {focal_entity}")
                logger.info(f"Rules: {rules}")
                
                # Execute rules against matching tables
                for rule in rules:
                    rule_name = rule['udc_config_name']
                
                    # Find matching tables
                    matching_tables = self.find_matching_tables(focal_entity, rule_name)
                    logger.info(f"Found {len(matching_tables)} matching tables for focal entity: {focal_entity}")
                    logger.info(f"Matching tables: {matching_tables}")
                    
                    if not matching_tables:
                        logger.warning(f"No matching tables found for focal entity: {focal_entity}")
                        continue
                    
                    for table_name in matching_tables:
                        logger.info(f"Executing rule {rule_name} on table {table_name}")
                        
                        # Execute rule
                        results_df, success = self.execute_rule(rule, table_name)
                        
                        if results_df is not None and not results_df.empty:
                            # Save results
                            self.save_rule_results(rule_name, table_name, results_df)
                            total_results_saved += 1
                            logger.info(f"Success: Results saved for rule {rule_name} on table {table_name}")
                            
                        else:
                            logger.info(f"Unsuccessful: No results saved for rule {rule_name} on table {table_name}")
                            
                        # update tracking only for successful query execution
                        if success:
                            self.update_rule_tracking(rule_name, table_name, success)
                        
                        total_rules_executed += 1
            
            logger.info(f"Performance rules execution completed. Executed {total_rules_executed} rules, saved {total_results_saved} result files.")
            return True
            
        except Exception as e:
            logger.error(f"Error executing performance rules: {e}")
            return False
    
    def get_rule_execution_results(self, id_value: str = None, rule_name: str = None, 
                                 start_date: datetime = None, end_date: datetime = None) -> pd.DataFrame:
        """Retrieve rule execution results from the database."""
        try:
            query = """
                SELECT "ResultsId", "Id", "timestamp", "udc_config_name", "udc_config_value", 
                       "created_at", "updated_at"
                FROM "ruleexecutionresults"
                WHERE 1=1
            """
            params = []
            
            if id_value:
                query += ' AND "Id" = %s'
                params.append(id_value)
            
            if rule_name:
                query += ' AND "udc_config_name" = %s'
                params.append(rule_name)
            
            if start_date:
                query += ' AND "timestamp" >= %s'
                params.append(start_date)
            
            if end_date:
                query += ' AND "timestamp" <= %s'
                params.append(end_date)
            
            query += ' ORDER BY "timestamp" DESC'
            
            result = self.db_config.execute_query(query, params)
            
            if result:
                df = pd.DataFrame(result)
                logger.info(f"Retrieved {len(df)} rule execution results")
                return df
            else:
                logger.info("No rule execution results found")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error retrieving rule execution results: {e}")
            return pd.DataFrame()
    
    def close_connection(self):
        """Close database connection."""
        self.db_config.close_connection()

if __name__ == "__main__":
    # Setup logging
    log_file = setup_logging()
    logger.info(f"Logging initialized. Log file: {log_file}")
    
    # Example usage
    rules = PerformanceRules()
    
    try:
        # Test formula conversion
        #rules.test_formula_conversion()
        
        # Create tables
        rules.create_performance_rules_table()
        
        # Load rules from CSV
        rules.load_rules_from_csv()
        
        # Execute all rules
        rules.execute_all_rules()
        
    finally:
        rules.close_connection()
        logger.info(f"Performance rules execution completed. Log saved to: {log_file}")
