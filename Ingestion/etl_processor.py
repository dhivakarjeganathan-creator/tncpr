"""
ETL Processor for KPI Data Transformation

This module processes CSV files according to the requirements in etlrequirements.txt.
It transforms input data based on expressions defined in KPIDetails table.
"""

import pandas as pd
import numpy as np
import logging
import re
from typing import Dict, List, Any, Optional
from pathlib import Path
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os

class ETLProcessor:
    """
    Main ETL processor class that handles CSV transformation based on KPI details.
    """
    
    def __init__(self, log_level: str = "INFO", db_config: Dict[str, str] = None, env_file: str = "database.env"):
        """
        Initialize the ETL processor.
        
        Args:
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
            db_config: Database configuration dictionary with keys: host, port, database, user, password
            env_file: Path to the .env file containing database configuration
        """
        self.setup_logging(log_level)
        self.kpi_data_sources: Dict[int, str] = {}
        self.kpi_details: List[Dict[str, Any]] = []
        self.kpi_resource_types: Dict[int, str] = {}
        self.kpi_resource_rules: List[Dict[str, Any]] = []
        self.kpi_rule_rules: List[Dict[str, Any]] = []
        self.logger = logging.getLogger(__name__)
        self.db_config = db_config or self.load_db_config_from_env(env_file)
        self.db_connection = None
        self.kpi_data_source_props: List[Dict[str, Any]] = []  # Store KPI data source properties
        
    def load_db_config_from_env(self, env_file: str = "database.env") -> Dict[str, str]:
        """
        Load database configuration from .env file.
        
        Args:
            env_file: Path to the .env file
            
        Returns:
            Dictionary containing database configuration
        """
        try:
            # Load environment variables from .env file
            load_dotenv(env_file)
            
            db_config = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': os.getenv('DB_PORT', '5432'),
                'database': os.getenv('DB_NAME', ''),
                'user': os.getenv('DB_USER', ''),
                'password': os.getenv('DB_PASSWORD', ''),
                'timeout': int(os.getenv('DB_TIMEOUT', '30')),
                'pool_size': int(os.getenv('DB_POOL_SIZE', '5')),
                'max_overflow': int(os.getenv('DB_MAX_OVERFLOW', '10'))
            }
            
            # Check if required fields are present
            required_fields = ['database', 'user', 'password']
            missing_fields = [field for field in required_fields if not db_config.get(field)]
            
            if missing_fields:
                self.logger.warning(f"Missing required database configuration fields: {missing_fields}")
                self.logger.warning(f"Please check your {env_file} file")
                return {}
            
            self.logger.info(f"Database configuration loaded from {env_file}")
            return db_config
            
        except Exception as e:
            self.logger.error(f"Error loading database configuration from {env_file}: {str(e)}")
            return {}
    
    def get_db_connection(self):
        """
        Get database connection. Creates new connection if none exists.
        
        Returns:
            Database connection object
        """
        if self.db_connection is None or self.db_connection.closed:
            if not self.db_config:
                raise ValueError("Database configuration not provided")
            
            try:
                self.db_connection = psycopg2.connect(
                    host=self.db_config.get('host'),
                    port=self.db_config.get('port'),
                    database=self.db_config.get('database'),
                    user=self.db_config.get('user'),
                    password=self.db_config.get('password')
                )
                self.logger.info("Database connection established")
            except Exception as e:
                self.logger.error(f"Failed to connect to database: {str(e)}")
                raise
        
        return self.db_connection
    
    def close_db_connection(self):
        """Close database connection if it exists."""
        if self.db_connection and not self.db_connection.closed:
            self.db_connection.close()
            self.logger.info("Database connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure database connection is closed."""
        self.close_db_connection()
    
    def get_type_id_from_hierarchy(self, gnb_value: str, type_name: str) -> Optional[str]:
        """
        Query hierarchy_gnb table to get type_id based on GNB value and type.
        
        Args:
            gnb_value: The GNB value from the row data
            type_name: Either 'AUPF' or 'ACPF'
            
        Returns:
            The type_id if found, None otherwise
        """
        if not self.db_config:
            self.logger.warning("Database configuration not provided, cannot query hierarchy_gnb table")
            return None
        
        try:
            conn = self.get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT typeid 
                    FROM public.hierarchy_gnb 
                    WHERE gnb = %s AND type = %s
                """
                cursor.execute(query, (gnb_value, type_name))
                result = cursor.fetchone()
                
                if result:
                    type_id = result['typeid']
                    self.logger.debug(f"Found type_id '{type_id}' for GNB '{gnb_value}' and type '{type_name}'")
                    return str(type_id)
                else:
                    self.logger.warning(f"No type_id found for GNB '{gnb_value}' and type '{type_name}'")
                    return None
                    
        except Exception as e:
            self.logger.error(f"Error querying hierarchy_gnb table: {str(e)}")
            return None
    
    def setup_logging(self, log_level: str) -> None:
        """Setup logging configuration."""
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('etl_processor.log'),
                logging.StreamHandler()
            ]
        )
    
    def load_kpi_data_sources(self, file_path: str) -> None:
        """
        Load KPI data sources from CSV file.
        
        Args:
            file_path: Path to KPIDatasources.csv file
        """
        try:
            #self.logger.info(f"Loading KPI data sources from {file_path}")
            df = pd.read_csv(file_path)
            
            # Create mapping of KpiDataSourceID to KpiDataSourceName
            self.kpi_data_sources = dict(zip(df['KpiDataSourceID'], df['KpiDataSourceName']))
            #self.logger.info(f"Loaded {len(self.kpi_data_sources)} KPI data sources")
            
        except Exception as e:
            self.logger.error(f"Error loading KPI data sources: {str(e)}")
            raise
    
    def load_kpi_details(self, file_path: str) -> None:
        """
        Load KPI details from CSV file.
        
        Args:
            file_path: Path to KPIDetails.csv file
        """
        try:
            #self.logger.info(f"Loading KPI details from {file_path}")
            df = pd.read_csv(file_path)
            
            # Convert to list of dictionaries for easier processing
            self.kpi_details = df.to_dict('records')
            #self.logger.info(f"Loaded {len(self.kpi_details)} KPI details")
            
        except Exception as e:
            self.logger.error(f"Error loading KPI details: {str(e)}")
            raise
    
    def load_kpi_resource_types(self, file_path: str) -> None:
        """
        Load KPI resource types from CSV file.
        
        Args:
            file_path: Path to KPIResourceTypes.csv file
        """
        try:
            #self.logger.info(f"Loading KPI resource types from {file_path}")
            df = pd.read_csv(file_path)
            
            # Create mapping of KpiResourceTypeID to KpiResourceTypeName
            self.kpi_resource_types = dict(zip(df['KpiResourceTypeID'], df['KpiResourceTypeName']))
            #self.logger.info(f"Loaded {len(self.kpi_resource_types)} KPI resource types")
            
        except Exception as e:
            self.logger.error(f"Error loading KPI resource types: {str(e)}")
            raise
    
    def load_kpi_resource_rules(self, file_path: str) -> None:
        """
        Load KPI resource rules from CSV file.
        
        Args:
            file_path: Path to KPIResourceRules.csv file
        """
        try:
            #self.logger.info(f"Loading KPI resource rules from {file_path}")
            df = pd.read_csv(file_path)
            
            # Convert to list of dictionaries for easier processing
            self.kpi_resource_rules = df.to_dict('records')
            #self.logger.info(f"Loaded {len(self.kpi_resource_rules)} KPI resource rules")
            
        except Exception as e:
            self.logger.error(f"Error loading KPI resource rules: {str(e)}")
            raise
    
    def load_kpi_rule_rules(self, file_path: str) -> None:
        """
        Load KPI rule rules from CSV file.
        
        Args:
            file_path: Path to KpiRuleRules.csv file
        """
        try:
            #self.logger.info(f"Loading KPI rule rules from {file_path}")
            df = pd.read_csv(file_path)
            
            # Convert to list of dictionaries for easier processing
            self.kpi_rule_rules = df.to_dict('records')
            #self.logger.info(f"Loaded {len(self.kpi_rule_rules)} KPI rule rules")
            
        except Exception as e:
            self.logger.error(f"Error loading KPI rule rules: {str(e)}")
            raise
    
    def load_kpi_data_source_props(self, file_path: str) -> None:
        """
        Load KPI data source properties from CSV file.
        
        Args:
            file_path: Path to KpiDataSourceProps.csv file
        """
        try:
            #self.logger.info(f"Loading KPI data source properties from {file_path}")
            df = pd.read_csv(file_path)
            
            # Convert to list of dictionaries for easier processing
            self.kpi_data_source_props = df.to_dict('records')
            #self.logger.info(f"Loaded {len(self.kpi_data_source_props)} KPI data source properties")
            
        except Exception as e:
            self.logger.error(f"Error loading KPI data source properties: {str(e)}")
            raise
    
    def evaluate_expression(self, expression: str, row_data: Dict[str, Any]) -> str:
        """
        Evaluate expression against row data.
        
        Args:
            expression: Expression to evaluate (e.g., "/MARKET", "/REGION", 'padLeft(/GNB, 7, '0')')
            row_data: Dictionary containing row data
            
        Returns:
            Evaluated expression result as string
        """
        if pd.isna(expression) or expression == '':
            return ''
        
        # Convert expression to string if it's not already
        expression = str(expression)
        
        try:
            # Handle simple column references like "/MARKET", "/REGION"
            if expression.startswith('/'):
                column_name = expression[1:]  # Remove leading slash
                
                # Special handling for /AUPF and /ACPF expressions
                if column_name in ['AUPF', 'ACPF']:
                    # Get GNB value from row_data
                    gnb_value = row_data.get('GNB', '')
                    if not gnb_value or pd.isna(gnb_value):
                        self.logger.warning(f"GNB column not found or empty, cannot resolve {column_name}")
                        return ''
                    
                    # Query hierarchy_gnb table to get type_id
                    type_id = self.get_type_id_from_hierarchy(str(gnb_value), column_name)
                    if type_id:
                        return type_id
                    else:
                        self.logger.warning(f"Could not find type_id for GNB '{gnb_value}' and type '{column_name}'")
                        return ''
                
                # Column name mapping - map ranMarket to MARKET
                if column_name == 'ranMarket':
                    column_name = 'MARKET'
                
                if column_name in row_data:
                    value = row_data[column_name]
                    return str(value) if not pd.isna(value) else ''
                else:
                    self.logger.warning(f"Column '{column_name}' not found in data")
                    return ''
                        
            # Handle literal values (enclosed in quotes)
            elif expression.startswith("'") and expression.endswith("'"):
                return expression[1:-1]  # Remove quotes
            
            # Handle literal numeric values (not expressions)
            elif isinstance(expression, (int, float)):
                return str(expression)
            
            # Handle padLeft() function (e.g., 'padLeft(/GNB, 7, '0')')
            elif expression.startswith('padLeft(') and expression.endswith(')'):
                return self._evaluate_padleft(expression, row_data)
            
            # Handle concat() function (e.g., 'concat(/deviceName,'_EdgeView:<',/elements/ifIndex,'>')')
            elif expression.startswith('concat(') and expression.endswith(')'):
                return self._evaluate_concat(expression, row_data)
            
            # Handle isEmpty() function (e.g., 'isEmpty(/labels/kubernetes_namespace)')
            elif expression.startswith('isEmpty(') and expression.endswith(')'):
                return self._evaluate_isempty(expression, row_data)
            
            # Handle conditional expressions (e.g., '{[not(isEmpty(/labels/kubernetes_namespace))] -> /labels/kubernetes_namespace, [not(isEmpty(/labels/namespace))] -> /labels/namespace}')
            elif expression.startswith('{') and expression.endswith('}'):
                return self._evaluate_conditional(expression, row_data)
            
            # Handle toDate() function
            elif expression.startswith('toDate(') and expression.endswith(')'):
                #self.logger.info(f"Evaluating toDate function: {expression}")
                result = self._evaluate_todate(expression, row_data)
                #self.logger.info(f"toDate result: {result}")
                return result
            
            # Handle substringBefore() function
            elif expression.startswith('substringBefore(') and expression.endswith(')'):
                return self._evaluate_substringbefore(expression, row_data)
            
            # Handle matchesRegex() function (for expressions)
            elif expression.startswith('matchesRegex(') and expression.endswith(')'):
                return self._evaluate_matchesregex(expression, row_data)
            
            # Handle substring() function (e.g., 'substring(/timestamp,0,13)')
            elif expression.startswith('substring(') and expression.endswith(')'):
                return self._evaluate_substring(expression, row_data)
            
            # Handle padRight() function (e.g., 'padRight(/timestamp, 13, '0')')
            elif expression.startswith('padRight(') and expression.endswith(')'):
                return self._evaluate_padright(expression, row_data)
            
            # Handle replace() function (e.g., 'replace(/text, 'old', 'new')')
            elif expression.startswith('replace(') and expression.endswith(')'):
                return self._evaluate_replace(expression, row_data)
            
            # Handle more complex expressions (basic implementation)
            else:
                # Replace column references with actual values
                eval_expression = expression
                for column, value in row_data.items():
                    if not pd.isna(value):
                        eval_expression = eval_expression.replace(f"/{column}", str(value))
                
                # For now, return the expression as-is if it contains complex logic
                # In a production system, you might want to use a proper expression evaluator
                return eval_expression
            
        except Exception as e:
            self.logger.warning(f"Error evaluating expression '{expression}': {str(e)}")
            return expression
    
    def _evaluate_padleft(self, expression: str, row_data: Dict[str, Any]) -> str:
        """
        Evaluate padLeft() function.
        Example: padLeft(/GNB, 7, '0') - pad GNB column to 7 characters with '0' on the left
        
        Args:
            expression: padLeft expression to evaluate
            row_data: Dictionary containing row data
            
        Returns:
            Padded string result
        """
        try:
            # Parse padLeft(/column, length, 'pad_char')
            inner = expression[8:-1]  # Remove 'padLeft(' and ')'
            
            # Split by comma, but be careful with nested quotes
            parts = []
            current_part = ""
            in_quotes = False
            quote_char = None
            
            for char in inner:
                if char in ["'", '"'] and not in_quotes:
                    in_quotes = True
                    quote_char = char
                    current_part += char
                elif char == quote_char and in_quotes:
                    in_quotes = False
                    quote_char = None
                    current_part += char
                elif char == ',' and not in_quotes:
                    parts.append(current_part.strip())
                    current_part = ""
                else:
                    current_part += char
            
            if current_part.strip():
                parts.append(current_part.strip())
            
            if len(parts) >= 2:
                column_ref = parts[0].strip()
                length = int(parts[1].strip())
                pad_char = parts[2].strip().strip("'\"") if len(parts) > 2 else ' '
                
                if column_ref.startswith('/'):
                    column_name = column_ref[1:]
                    if column_name in row_data:
                        value = str(row_data[column_name]) if not pd.isna(row_data[column_name]) else ''
                        return value.rjust(length, pad_char)
                    else:
                        self.logger.warning(f"Column '{column_name}' not found for padLeft")
                        return ''
            
            self.logger.warning(f"Invalid padLeft expression: {expression}")
            return ''
            
        except Exception as e:
            self.logger.warning(f"Error evaluating padLeft expression '{expression}': {str(e)}")
            return ''
    
    def _evaluate_concat(self, expression: str, row_data: Dict[str, Any]) -> str:
        """
        Evaluate concat() function.
        Example: concat(/deviceName,'_EdgeView:<',/elements/ifIndex,'>')
        
        Args:
            expression: concat expression to evaluate
            row_data: Dictionary containing row data
            
        Returns:
            Concatenated string result
        """
        try:
            # Parse concat(part1, part2, part3, ...)
            inner = expression[7:-1]  # Remove 'concat(' and ')'
            
            # Split by comma, but be careful with nested quotes and parentheses
            parts = []
            current_part = ""
            in_quotes = False
            quote_char = None
            paren_count = 0
            
            for char in inner:
                if char in ["'", '"'] and not in_quotes:
                    in_quotes = True
                    quote_char = char
                    current_part += char
                elif char == quote_char and in_quotes:
                    in_quotes = False
                    quote_char = None
                    current_part += char
                elif char == '(' and not in_quotes:
                    paren_count += 1
                    current_part += char
                elif char == ')' and not in_quotes:
                    paren_count -= 1
                    current_part += char
                elif char == ',' and not in_quotes and paren_count == 0:
                    parts.append(current_part.strip())
                    current_part = ""
                else:
                    current_part += char
            
            if current_part.strip():
                parts.append(current_part.strip())
            
            # Evaluate each part and concatenate
            result_parts = []
            for part in parts:
                part = part.strip()
                if part.startswith('/'):
                    # Column reference
                    column_name = part[1:]
                            
                    # Special handling for /AUPF and /ACPF expressions
                    if column_name in ['AUPF', 'ACPF']:
                        # Get GNB value from row_data
                        gnb_value = row_data.get('GNB', '')
                        if not gnb_value or pd.isna(gnb_value):
                            self.logger.warning(f"GNB column not found or empty, cannot resolve {column_name}")
                            result_parts.append('')
                        
                        # Query hierarchy_gnb table to get type_id
                        type_id = self.get_type_id_from_hierarchy(str(gnb_value), column_name)
                        if type_id:
                            result_parts.append(type_id)
                        else:
                            self.logger.warning(f"Could not find type_id for GNB '{gnb_value}' and type '{column_name}'")
                            result_parts.append('')
        
                    elif column_name == 'ranMarket':
                        column_name = 'MARKET'
                    
                    elif column_name in row_data:
                        value = str(row_data[column_name]) if not pd.isna(row_data[column_name]) else ''
                        result_parts.append(value)
                    else:
                        # Provide default values for common missing columns in timestamp expressions
                        if column_name == 'MI':
                            # Default to '00' for minutes if not provided
                            result_parts.append('00')
                            #self.logger.info(f"Column '{column_name}' not found, using default '00' for minutes")
                        elif column_name == 'SS':
                            # Default to '00' for seconds if not provided
                            result_parts.append('00')
                            #self.logger.info(f"Column '{column_name}' not found, using default '00' for seconds")
                        else:
                            self.logger.warning(f"Column '{column_name}' not found for concat")
                            result_parts.append('')
                elif part.startswith("'") and part.endswith("'"):
                    # Literal string
                    result_parts.append(part[1:-1])
                else:
                    # Other expression - evaluate recursively
                    evaluated_part = self.evaluate_expression(part, row_data)
                    result_parts.append(evaluated_part)
            
            return ''.join(result_parts)
            
        except Exception as e:
            self.logger.warning(f"Error evaluating concat expression '{expression}': {str(e)}")
            return ''
    
    def _evaluate_isempty(self, expression: str, row_data: Dict[str, Any]) -> str:
        """
        Evaluate isEmpty() function.
        Example: isEmpty(/labels/kubernetes_namespace)
        
        Args:
            expression: isEmpty expression to evaluate
            row_data: Dictionary containing row data
            
        Returns:
            'true' if empty, 'false' if not empty
        """
        try:
            # Parse isEmpty(/column)
            inner = expression[8:-1].strip()  # Remove 'isEmpty(' and ')'
            
            if inner.startswith('/'):
                column_name = inner[1:]
                if column_name in row_data:
                    value = row_data[column_name]
                    is_empty = pd.isna(value) or str(value).strip() == ''
                    return 'true' if is_empty else 'false'
                else:
                    # Column doesn't exist, consider it empty
                    return 'true'
            
            self.logger.warning(f"Invalid isEmpty expression: {expression}")
            return 'false'
            
        except Exception as e:
            self.logger.warning(f"Error evaluating isEmpty expression '{expression}': {str(e)}")
            return 'false'
    
    def _evaluate_conditional(self, expression: str, row_data: Dict[str, Any]) -> str:
        """
        Evaluate conditional expressions.
        Example: {[not(isEmpty(/labels/kubernetes_namespace))] -> /labels/kubernetes_namespace, [not(isEmpty(/labels/namespace))] -> /labels/namespace}
        
        Args:
            expression: Conditional expression to evaluate
            row_data: Dictionary containing row data
            
        Returns:
            Result of the first matching condition
        """
        try:
            # Remove outer braces
            inner = expression[1:-1].strip()
            #self.logger.info(f"Printing inner: '{inner}'")
            
            # Split by comma, but be careful with nested brackets and quotes
            conditions = []
            current_condition = ""
            bracket_count = 0
            in_quotes = False
            quote_char = None
            
            for char in inner:
                ##self.logger.info(f"current_condition: '{current_condition}'")
                if char in ["'", '"'] and not in_quotes:
                    in_quotes = True
                    quote_char = char
                    current_condition += char
                elif char == quote_char and in_quotes:
                    in_quotes = False
                    quote_char = None
                    current_condition += char
                elif char in ["[", '('] and not in_quotes:
                    bracket_count += 1
                    current_condition += char
                elif char in ["]", ')'] and not in_quotes:
                    bracket_count -= 1
                    current_condition += char
                elif char == ',' and not in_quotes and bracket_count == 0:
                    conditions.append(current_condition.strip())
                    current_condition = ""
                else:
                    current_condition += char
                ##self.logger.info(f"Printing chars: '{char}'--;'{in_quotes}';'{quote_char}';'{bracket_count}';'")
                
            if current_condition.strip():
                conditions.append(current_condition.strip())
            
            
            #self.logger.info(f"Printing all condition: '{conditions}'")
            
            # Evaluate each condition
            for condition in conditions:
                #self.logger.info(f"Printing condition: '{condition}'")
                if ' -> ' in condition:
                    
                    condition_part, value_part = condition.split(' -> ', 1)
                    condition_part = condition_part.strip()
                    value_part = value_part.strip()
                    
                    # Remove brackets from condition
                    if condition_part.startswith('[') and condition_part.endswith(']'):
                        condition_part = condition_part[1:-1]
                    
                    #self.logger.info(f"Evaluating condition: '{condition_part}'")
                    # Evaluate the condition
                    condition_result = self._evaluate_condition_expression(condition_part, row_data)
                    #self.logger.info(f"Condition result: {condition_result}")
                    
                    if condition_result:
                        # Condition is true, return the value
                        #self.logger.info(f"Condition matched, evaluating value: '{value_part}'")
                        result = self.evaluate_expression(value_part, row_data)
                        #self.logger.info(f"Value evaluation result: '{result}'")
                        # If the result is still a toDate function, evaluate it further
                        if isinstance(result, str) and result.startswith('toDate('):
                            #self.logger.info(f"Result is toDate function, evaluating further: '{result}'")
                            final_result = self.evaluate_expression(result, row_data)
                            #self.logger.info(f"Final toDate result: '{final_result}'")
                            return final_result
                        return result
            
            # No condition matched, return empty string
            return ''
            
        except Exception as e:
            self.logger.warning(f"Error evaluating conditional expression '{expression}': {str(e)}")
            return ''
    
    def _evaluate_todate(self, expression: str, row_data: Dict[str, Any]) -> str:
        """
        Evaluate toDate() function for date/time conversion.
        Examples:
        - toDate(concat(/DAY,' ', /HR, ':', /MI),'MM/dd/yyyy HH:mm','America/Indiana/Indianapolis')
        - toDate(/@timestamp, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')
        
        Args:
            expression: toDate expression to evaluate
            row_data: Dictionary containing row data
            
        Returns:
            Formatted date/time string
        """
        try:
            # Parse toDate(input, format, timezone)
            inner = expression[7:-1]  # Remove 'toDate(' and ')'
            
            # Split by comma, but be careful with nested quotes and parentheses
            parts = []
            current_part = ""
            in_quotes = False
            quote_char = None
            paren_count = 0
            
            for char in inner:
                if char in ["'", '"'] and not in_quotes:
                    in_quotes = True
                    quote_char = char
                    current_part += char
                elif char == quote_char and in_quotes:
                    in_quotes = False
                    quote_char = None
                    current_part += char
                elif char == '(' and not in_quotes:
                    paren_count += 1
                    current_part += char
                elif char == ')' and not in_quotes:
                    paren_count -= 1
                    current_part += char
                elif char == ',' and not in_quotes and paren_count == 0:
                    parts.append(current_part.strip())
                    current_part = ""
                else:
                    current_part += char
            
            if current_part.strip():
                parts.append(current_part.strip())
            
            if len(parts) < 2:
                self.logger.warning(f"toDate() requires at least 2 parameters: input and format")
                return ''
            
            # Evaluate input expression
            input_expr = parts[0].strip()
            input_value = self.evaluate_expression(input_expr, row_data)
            
            # Get format
            format_expr = parts[1].strip()
            if format_expr.startswith("'") and format_expr.endswith("'"):
                date_format = format_expr[1:-1]
                # Unescape quotes in format string
                date_format = date_format.replace("\\'", "'")
            else:
                date_format = self.evaluate_expression(format_expr, row_data)
            
            # Get timezone (optional)
            timezone = None
            if len(parts) >= 3:
                timezone_expr = parts[2].strip()
                if timezone_expr.startswith("'") and timezone_expr.endswith("'"):
                    timezone = timezone_expr[1:-1]
                else:
                    timezone = self.evaluate_expression(timezone_expr, row_data)
            
            # Convert date/time
            from datetime import datetime
            import pytz
            
            try:
                # Convert format from requirements format to Python datetime format
                python_format = self._convert_date_format(date_format)
                # Debug: print the values being parsed
                #self.logger.info(f"Parsing date '{input_value}' with format '{date_format}' -> '{python_format}'")
                # Parse the input date string
                parsed_date = datetime.strptime(input_value, python_format)
                
                # Apply timezone if provided
                if timezone:
                    try:
                        tz = pytz.timezone(timezone)
                        parsed_date = tz.localize(parsed_date)
                    except pytz.exceptions.UnknownTimeZoneError:
                        self.logger.warning(f"Unknown timezone: {timezone}")
                
                # Return ISO format string
                return parsed_date.isoformat()
                
            except ValueError as e:
                self.logger.warning(f"Error parsing date '{input_value}' with format '{date_format}': {str(e)}")
                return input_value  # Return original value if parsing fails
                
        except Exception as e:
            self.logger.warning(f"Error evaluating toDate expression '{expression}': {str(e)}")
            return ''
    
    def _convert_date_format(self, format_string: str) -> str:
        """
        Convert date format from requirements format to Python datetime format.
        
        Args:
            format_string: Format string in requirements format (e.g., 'MM/dd/yyyy HH:mm')
            
        Returns:
            Python datetime format string (e.g., '%m/%d/%Y %H:%M')
        """
        # If the format already contains % characters, assume it's already in Python format
        if '%' in format_string:
            return format_string.replace("\\'", "'")
        
        # Common format conversions
        format_mappings = {
            'yyyy': '%Y',  # 4-digit year
            'yy': '%y',    # 2-digit year
            'MM': '%m',    # Month (01-12)
            'M': '%m',     # Month (1-12)
            'dd': '%d',    # Day (01-31)
            'd': '%d',     # Day (1-31)
            'HH': '%H',    # Hour (00-23)
            'H': '%H',     # Hour (0-23)
            'mm': '%M',    # Minute (00-59)
            'm': '%M',     # Minute (0-59)
            'ss': '%S',    # Second (00-59)
            's': '%S',     # Second (0-59)
            'SSS': '%f',   # Microsecond (000000-999999)
            'SS': '%f',    # Microsecond (00000-99999)
            'S': '%f',     # Microsecond (0000-9999)
        }
        
        python_format = format_string
        
        # Use a simple approach: replace each format specifier with a unique placeholder first
        # to avoid conflicts, then replace placeholders with actual format codes
        placeholders = {}
        placeholder_count = 0
        
        # Sort by length (longer first) to avoid conflicts
        sorted_mappings = sorted(format_mappings.items(), key=lambda x: len(x[0]), reverse=True)
        
        # First pass: replace with unique placeholders
        for req_format, py_format in sorted_mappings:
            if req_format in python_format:
                placeholder = f"__X{placeholder_count}X__"
                placeholders[placeholder] = py_format
                python_format = python_format.replace(req_format, placeholder)
                placeholder_count += 1
        
        # Second pass: replace placeholders with actual format codes
        for placeholder, py_format in placeholders.items():
            python_format = python_format.replace(placeholder, py_format)
        
        # Handle escaped quotes
        python_format = python_format.replace("\\'", "'")
        
        return python_format
    
    def _evaluate_substringbefore(self, expression: str, row_data: Dict[str, Any]) -> str:
        """
        Evaluate substringBefore() function.
        Example: substringBefore(/timestamp,'Z')
        
        Args:
            expression: substringBefore expression to evaluate
            row_data: Dictionary containing row data
            
        Returns:
            Substring before the specified character
        """
        try:
            # Parse substringBefore(input, delimiter)
            inner = expression[16:-1]  # Remove 'substringBefore(' and ')'
            
            # Split by comma, but be careful with nested quotes and parentheses
            parts = []
            current_part = ""
            in_quotes = False
            quote_char = None
            paren_count = 0
            
            for char in inner:
                if char in ["'", '"'] and not in_quotes:
                    in_quotes = True
                    quote_char = char
                    current_part += char
                elif char == quote_char and in_quotes:
                    in_quotes = False
                    quote_char = None
                    current_part += char
                elif char == '(' and not in_quotes:
                    paren_count += 1
                    current_part += char
                elif char == ')' and not in_quotes:
                    paren_count -= 1
                    current_part += char
                elif char == ',' and not in_quotes and paren_count == 0:
                    parts.append(current_part.strip())
                    current_part = ""
                else:
                    current_part += char
            
            if current_part.strip():
                parts.append(current_part.strip())
            
            if len(parts) != 2:
                self.logger.warning(f"substringBefore() requires exactly 2 parameters: input and delimiter")
                return ''
            
            # Evaluate input expression
            input_expr = parts[0].strip()
            input_value = self.evaluate_expression(input_expr, row_data)
            
            # Get delimiter
            delimiter_expr = parts[1].strip()
            if delimiter_expr.startswith("'") and delimiter_expr.endswith("'"):
                delimiter = delimiter_expr[1:-1]
            else:
                delimiter = self.evaluate_expression(delimiter_expr, row_data)
            
            # Find substring before delimiter
            if delimiter in input_value:
                return input_value.split(delimiter)[0]
            else:
                return input_value  # Return original if delimiter not found
                
        except Exception as e:
            self.logger.warning(f"Error evaluating substringBefore expression '{expression}': {str(e)}")
            return ''
    
    def _evaluate_matchesregex(self, expression: str, row_data: Dict[str, Any]) -> str:
        """
        Evaluate matchesRegex() function for use in expressions.
        Example: matchesRegex(/ranMarket,'^031$|^036$')
        
        Args:
            expression: matchesRegex expression to evaluate
            row_data: Dictionary containing row data
            
        Returns:
            'true' if matches, 'false' if not
        """
        try:
            # Parse matchesRegex(input, pattern)
            inner = expression[13:-1]  # Remove 'matchesRegex(' and ')'
            
            # Split by comma, but be careful with nested quotes and parentheses
            parts = []
            current_part = ""
            in_quotes = False
            quote_char = None
            paren_count = 0
            
            for char in inner:
                if char in ["'", '"'] and not in_quotes:
                    in_quotes = True
                    quote_char = char
                    current_part += char
                elif char == quote_char and in_quotes:
                    in_quotes = False
                    quote_char = None
                    current_part += char
                elif char == '(' and not in_quotes:
                    paren_count += 1
                    current_part += char
                elif char == ')' and not in_quotes:
                    paren_count -= 1
                    current_part += char
                elif char == ',' and not in_quotes and paren_count == 0:
                    parts.append(current_part.strip())
                    current_part = ""
                else:
                    current_part += char
            
            if current_part.strip():
                parts.append(current_part.strip())
            
            if len(parts) != 2:
                self.logger.warning(f"matchesRegex() requires exactly 2 parameters: input and pattern")
                return 'false'
            
            # Evaluate input expression
            input_expr = parts[0].strip()
            input_value = self.evaluate_expression(input_expr, row_data)
            
            # Get pattern
            pattern_expr = parts[1].strip()
            if pattern_expr.startswith("'") and pattern_expr.endswith("'"):
                pattern = pattern_expr[1:-1]
            else:
                pattern = self.evaluate_expression(pattern_expr, row_data)
            
            #self.logger.info(f"matchesRegex: input_expr = '{input_expr}', input_value='{input_value}', pattern='{pattern}'")
            
            # Check regex match
            import re
            if re.search(pattern, str(input_value)):
                #self.logger.info(f"matchesRegex: MATCH found")
                return 'true'
            else:
                #self.logger.info(f"matchesRegex: NO MATCH")
                return 'false'
                
        except Exception as e:
            self.logger.warning(f"Error evaluating matchesRegex expression '{expression}': {str(e)}")
            return 'false'
    
    def _evaluate_substring(self, expression: str, row_data: Dict[str, Any]) -> str:
        """
        Evaluate substring() function.
        Format: substring(/column, start, length)
        Example: substring(/timestamp,0,13)
        
        Args:
            expression: Expression to evaluate (e.g., 'substring(/timestamp,0,13)')
            row_data: Dictionary containing row data
            
        Returns:
            Substring result as string
        """
        try:
            # Extract the inner expression: substring(/timestamp,0,13) -> /timestamp,0,13
            inner = expression[10:-1]  # Remove 'substring(' and ')'
            
            # Parse arguments: /timestamp,0,13
            args = []
            current_arg = ""
            paren_count = 0
            in_quotes = False
            quote_char = None
            
            for char in inner:
                if char in ['"', "'"] and not in_quotes:
                    in_quotes = True
                    quote_char = char
                    current_arg += char
                elif char == quote_char and in_quotes:
                    in_quotes = False
                    quote_char = None
                    current_arg += char
                elif char == ',' and not in_quotes and paren_count == 0:
                    args.append(current_arg.strip())
                    current_arg = ""
                else:
                    if char == '(':
                        paren_count += 1
                    elif char == ')':
                        paren_count -= 1
                    current_arg += char
            
            # Add the last argument
            if current_arg.strip():
                args.append(current_arg.strip())
            
            if len(args) != 3:
                self.logger.warning(f"substring() expects 3 arguments (column, start, length), got {len(args)}")
                return ''
            
            # Parse arguments
            column_expr = args[0].strip()
            start_expr = args[1].strip()
            length_expr = args[2].strip()
            
            # Evaluate column reference
            if column_expr.startswith('/'):
                column_name = column_expr[1:]
                if column_name in row_data:
                    input_value = row_data[column_name]
                    if pd.isna(input_value):
                        return ''
                    input_str = str(input_value)
                else:
                    self.logger.warning(f"Column '{column_name}' not found in data")
                    return ''
            else:
                self.logger.warning(f"substring() first argument must be a column reference starting with '/', got: {column_expr}")
                return ''
            
            # Parse start position
            try:
                start_pos = int(start_expr)
            except ValueError:
                self.logger.warning(f"substring() start position must be an integer, got: {start_expr}")
                return ''
            
            # Parse length
            try:
                length = int(length_expr)
            except ValueError:
                self.logger.warning(f"substring() length must be an integer, got: {length_expr}")
                return ''
            
            # Extract substring
            if start_pos < 0:
                start_pos = 0
            if start_pos >= len(input_str):
                return ''
            
            end_pos = start_pos + length
            if end_pos > len(input_str):
                end_pos = len(input_str)
            
            result = input_str[start_pos:end_pos]
            self.logger.debug(f"substring({input_str}, {start_pos}, {length}) = '{result}'")
            return result
            
        except Exception as e:
            self.logger.warning(f"Error evaluating substring expression '{expression}': {str(e)}")
            return ''
    
    def _evaluate_padright(self, expression: str, row_data: Dict[str, Any]) -> str:
        """
        Evaluate padRight() function.
        Format: padRight(/column, length, pad_char)
        Example: padRight(/timestamp, 13, '0')
        
        Args:
            expression: Expression to evaluate (e.g., 'padRight(/timestamp, 13, '0')')
            row_data: Dictionary containing row data
            
        Returns:
            Right-padded string result
        """
        try:
            # Extract the inner expression: padRight(/timestamp, 13, '0') -> /timestamp, 13, '0'
            inner = expression[9:-1]  # Remove 'padRight(' and ')'
            
            # Parse arguments: /timestamp, 13, '0'
            args = []
            current_arg = ""
            paren_count = 0
            in_quotes = False
            quote_char = None
            
            for char in inner:
                if char in ['"', "'"] and not in_quotes:
                    in_quotes = True
                    quote_char = char
                    current_arg += char
                elif char == quote_char and in_quotes:
                    in_quotes = False
                    quote_char = None
                    current_arg += char
                elif char == ',' and not in_quotes and paren_count == 0:
                    args.append(current_arg.strip())
                    current_arg = ""
                else:
                    if char == '(':
                        paren_count += 1
                    elif char == ')':
                        paren_count -= 1
                    current_arg += char
            
            # Add the last argument
            if current_arg.strip():
                args.append(current_arg.strip())
            
            if len(args) != 3:
                self.logger.warning(f"padRight() expects 3 arguments (column, length, pad_char), got {len(args)}")
                return ''
            
            # Parse arguments
            column_expr = args[0].strip()
            length_expr = args[1].strip()
            pad_char_expr = args[2].strip()
            
            # Evaluate column reference
            if column_expr.startswith('/'):
                column_name = column_expr[1:]
                if column_name in row_data:
                    input_value = row_data[column_name]
                    if pd.isna(input_value):
                        input_str = ''
                    else:
                        input_str = str(input_value)
                else:
                    self.logger.warning(f"Column '{column_name}' not found in data")
                    return ''
            else:
                self.logger.warning(f"padRight() first argument must be a column reference starting with '/', got: {column_expr}")
                return ''
            
            # Parse target length
            try:
                target_length = int(length_expr)
            except ValueError:
                self.logger.warning(f"padRight() length must be an integer, got: {length_expr}")
                return ''
            
            # Parse pad character
            if pad_char_expr.startswith("'") and pad_char_expr.endswith("'"):
                pad_char = pad_char_expr[1:-1]  # Remove quotes
            elif pad_char_expr.startswith('"') and pad_char_expr.endswith('"'):
                pad_char = pad_char_expr[1:-1]  # Remove quotes
            else:
                self.logger.warning(f"padRight() pad character must be quoted, got: {pad_char_expr}")
                return ''
            
            if len(pad_char) != 1:
                self.logger.warning(f"padRight() pad character must be a single character, got: '{pad_char}'")
                return ''
            
            # Right-pad the string
            if len(input_str) >= target_length:
                result = input_str
            else:
                padding_needed = target_length - len(input_str)
                result = input_str + (pad_char * padding_needed)
            
            self.logger.debug(f"padRight('{input_str}', {target_length}, '{pad_char}') = '{result}'")
            return result
            
        except Exception as e:
            self.logger.warning(f"Error evaluating padRight expression '{expression}': {str(e)}")
            return ''
    
    def _evaluate_replace(self, expression: str, row_data: Dict[str, Any]) -> str:
        """
        Evaluate replace() function.
        Format: replace(text, old_text, new_text)
        Example: replace('type', 'y', 'abc') -> 'tabcpe'
        
        Args:
            expression: Expression to evaluate (e.g., 'replace(/text, 'old', 'new')')
            row_data: Dictionary containing row data
            
        Returns:
            String with replacements made
        """
        try:
            # Extract the inner expression: replace('type', 'y', 'abc') -> 'type', 'y', 'abc'
            inner = expression[8:-1]  # Remove 'replace(' and ')'
            
            # Parse arguments: 'type', 'y', 'abc'
            args = []
            current_arg = ""
            paren_count = 0
            in_quotes = False
            quote_char = None
            
            for char in inner:
                if char in ['"', "'"] and not in_quotes:
                    in_quotes = True
                    quote_char = char
                    current_arg += char
                elif char == quote_char and in_quotes:
                    in_quotes = False
                    quote_char = None
                    current_arg += char
                elif char == ',' and not in_quotes and paren_count == 0:
                    args.append(current_arg.strip())
                    current_arg = ""
                else:
                    if char == '(':
                        paren_count += 1
                    elif char == ')':
                        paren_count -= 1
                    current_arg += char
            
            # Add the last argument
            if current_arg.strip():
                args.append(current_arg.strip())
            
            if len(args) != 3:
                self.logger.warning(f"replace() expects 3 arguments (text, old_text, new_text), got {len(args)}")
                return ''
            
            # Parse arguments
            text_expr = args[0].strip()
            old_text_expr = args[1].strip()
            new_text_expr = args[2].strip()
            
            # Evaluate text expression
            if text_expr.startswith('/'):
                column_name = text_expr[1:]
                if column_name in row_data:
                    text_value = row_data[column_name]
                    if pd.isna(text_value):
                        text_str = ''
                    else:
                        text_str = str(text_value)
                else:
                    self.logger.warning(f"Column '{column_name}' not found in data")
                    return ''
            elif text_expr.startswith("'") and text_expr.endswith("'"):
                text_str = text_expr[1:-1]  # Remove quotes
            elif text_expr.startswith('"') and text_expr.endswith('"'):
                text_str = text_expr[1:-1]  # Remove quotes
            else:
                # Evaluate as expression
                text_str = self.evaluate_expression(text_expr, row_data)
            
            # Get old_text
            if old_text_expr.startswith("'") and old_text_expr.endswith("'"):
                old_text = old_text_expr[1:-1]  # Remove quotes
            elif old_text_expr.startswith('"') and old_text_expr.endswith('"'):
                old_text = old_text_expr[1:-1]  # Remove quotes
            else:
                old_text = self.evaluate_expression(old_text_expr, row_data)
            
            # Get new_text
            if new_text_expr.startswith("'") and new_text_expr.endswith("'"):
                new_text = new_text_expr[1:-1]  # Remove quotes
            elif new_text_expr.startswith('"') and new_text_expr.endswith('"'):
                new_text = new_text_expr[1:-1]  # Remove quotes
            else:
                new_text = self.evaluate_expression(new_text_expr, row_data)
            
            # Perform the replacement
            if old_text == '':
                # Special case: replacing empty string should prepend new_text
                result = new_text + text_str
            else:
                result = text_str.replace(old_text, new_text)
            
            self.logger.debug(f"replace('{text_str}', '{old_text}', '{new_text}') = '{result}'")
            return result
            
        except Exception as e:
            self.logger.warning(f"Error evaluating replace expression '{expression}': {str(e)}")
            return ''
    
    def _evaluate_condition_expression(self, condition: str, row_data: Dict[str, Any]) -> bool:
        """
        Evaluate a condition expression (used in conditional expressions).
        
        Args:
            condition: Condition to evaluate (e.g., 'not(isEmpty(/labels/kubernetes_namespace))')
            row_data: Dictionary containing row data
            
        Returns:
            True if condition is satisfied, False otherwise
        """
        try:
            # Debug: print the condition being evaluated
            self.logger.debug(f"Evaluating condition: '{condition}'")
            
            # Handle not() function
            if condition.startswith('not(') and condition.endswith(')'):
                inner_condition = condition[4:-1]
                return not self._evaluate_condition_expression(inner_condition, row_data)
            
            # Handle isEmpty() function
            elif condition.startswith('isEmpty(') and condition.endswith(')'):
                inner = condition[8:-1].strip()
                if inner.startswith('/'):
                    column_name = inner[1:]
                    if column_name in row_data:
                        value = row_data[column_name]
                        return pd.isna(value) or str(value).strip() == ''
                    else:
                        return True  # Column doesn't exist, consider it empty
            
            # Handle isBlank() function
            elif condition.startswith('isBlank(') and condition.endswith(')'):
                column_ref = condition[8:-1].strip()  # Remove 'isBlank(' and ')'
                if column_ref.startswith('/'):
                    column_name = column_ref[1:]
                    if column_name in row_data:
                        value = row_data[column_name]
                        return pd.isna(value) or str(value).strip() == ''
                    else:
                        # Column doesn't exist, consider it blank
                        return True
                return False
            
            # Handle not(isBlank()) function
            elif condition.startswith('not(isBlank(') and condition.endswith('))'):
                column_ref = condition[12:-2].strip()  # Remove 'not(isBlank(' and '))'
                if column_ref.startswith('/'):
                    column_name = column_ref[1:]
                    if column_name in row_data:
                        value = row_data[column_name]
                        is_blank = pd.isna(value) or str(value).strip() == ''
                        return not is_blank
                    else:
                        # Column doesn't exist, not(isBlank()) is false
                        return False
                return False
            
            # Handle contains() function
            elif condition.startswith('contains(') and condition.endswith(')'):
                # Parse contains(/column,'substring')
                inner = condition[9:-1]  # Remove 'contains(' and ')'
                if ',' in inner:
                    parts = inner.split(',', 1)
                    if len(parts) == 2:
                        column_ref = parts[0].strip()
                        substring = parts[1].strip().strip("'\"")
                        
                        if column_ref.startswith('/'):
                            column_name = column_ref[1:]
                            if column_name in row_data:
                                value = row_data[column_name]
                                if not pd.isna(value):
                                    return substring in str(value)
                            return False
                return False
            
            # Handle matchesRegex() function
            elif condition.startswith('matchesRegex(') and condition.endswith(')'):
                # Use the existing matchesRegex evaluation method
                result = self._evaluate_matchesregex(condition, row_data)
                return result == 'true'
            
            # Handle simple equality conditions like "/MARKET = '072'"
            elif '=' in condition:
                parts = condition.split('=')
                if len(parts) == 2:
                    left_part = parts[0].strip()
                    right_part = parts[1].strip()
                    
                    # Evaluate left part (column reference)
                    left_value = self.evaluate_expression(left_part, row_data)
                    
                    # Evaluate right part (literal or expression)
                    right_value = self.evaluate_expression(right_part, row_data)
                    
                    return left_value == right_value
                return False
            
            # Handle other conditions (could be extended)
            else:
                self.logger.debug(f"Condition not recognized: '{condition}'")
                self.logger.warning(f"Unsupported condition expression: {condition}")
                return False
                
        except Exception as e:
            self.logger.warning(f"Error evaluating condition expression '{condition}': {str(e)}")
            return False
    
    def find_kpi_rule_rules(self, kpi_resource_rule_id: int) -> List[Dict[str, Any]]:
        """
        Find all KPI rule rules for a given KPI resource rule ID.
        
        Args:
            kpi_resource_rule_id: The KPI resource rule ID to find rules for
            
        Returns:
            List of matching KPI rule rules
        """
        matching_rules = []
        
        for rule in self.kpi_rule_rules:
            rule_kpi_resource_rule_id = rule.get('KpiResourceRuleID')
            # Convert to int if it's a string
            if isinstance(rule_kpi_resource_rule_id, str):
                try:
                    rule_kpi_resource_rule_id = int(rule_kpi_resource_rule_id)
                except ValueError:
                    continue
            if rule_kpi_resource_rule_id == kpi_resource_rule_id:
                matching_rules.append(rule)
        
        return matching_rules
    
    def evaluate_filter_condition(self, filter_condition: str, row_data: Dict[str, Any]) -> bool:
        """
        Evaluate filter condition against row data.
        Supports various filter condition types:
        - Simple equality: /MARKET = '072'
        - isBlank(): isBlank(/DU)
        - not(isBlank()): not(isBlank(/GNB))
        - contains(): contains(/objectName,'bond')
        - matchesRegex(): matchesRegex(/name,'01-Inlet.*|02-CPU.*')
        - Complex conditions with 'and': [condition1 and condition2]
        
        Args:
            filter_condition: Filter condition to evaluate
            row_data: Dictionary containing row data
            
        Returns:
            True if condition is satisfied, False otherwise
        """
        if pd.isna(filter_condition) or filter_condition == '':
            return True  # No filter means all rows pass
        
        try:
            # Remove outer brackets if present
            condition = filter_condition.strip()
            if condition.startswith('[') and condition.endswith(']'):
                condition = condition[1:-1]
            
            # Handle complex conditions with 'and'
            if ' and ' in condition:
                parts = condition.split(' and ')
                # Evaluate each part and combine with AND logic
                for part in parts:
                    if not self.evaluate_single_condition(part.strip(), row_data):
                        return False
                return True
            
            # Handle single condition
            return self.evaluate_single_condition(condition, row_data)
            
        except Exception as e:
            self.logger.warning(f"Error evaluating filter condition '{filter_condition}': {str(e)}")
            return False  # If error, assume condition fails
    
    def evaluate_single_condition(self, condition: str, row_data: Dict[str, Any]) -> bool:
        """
        Evaluate a single filter condition.
        
        Args:
            condition: Single condition to evaluate
            row_data: Dictionary containing row data
            
        Returns:
            True if condition is satisfied, False otherwise
        """
        try:
            # Handle isBlank() function
            if condition.startswith('isBlank(') and condition.endswith(')'):
                column_ref = condition[8:-1].strip()  # Remove 'isBlank(' and ')'
                if column_ref.startswith('/'):
                    column_name = column_ref[1:]
                    if column_name in row_data:
                        value = row_data[column_name]
                        return pd.isna(value) or str(value).strip() == ''
                    else:
                        # Column doesn't exist, consider it blank
                        return True
                return False
            
            # Handle not(isBlank()) function
            elif condition.startswith('not(isBlank(') and condition.endswith('))'):
                column_ref = condition[12:-2].strip()  # Remove 'not(isBlank(' and '))'
                if column_ref.startswith('/'):
                    column_name = column_ref[1:]
                    if column_name in row_data:
                        value = row_data[column_name]
                        is_blank = pd.isna(value) or str(value).strip() == ''
                        return not is_blank
                    else:
                        # Column doesn't exist, not(isBlank()) is false
                        return False
                return False
            
            # Handle contains() function
            elif condition.startswith('contains(') and condition.endswith(')'):
                # Parse contains(/column,'substring')
                inner = condition[9:-1]  # Remove 'contains(' and ')'
                if ',' in inner:
                    parts = inner.split(',', 1)
                    if len(parts) == 2:
                        column_ref = parts[0].strip()
                        substring = parts[1].strip().strip("'\"")
                        
                        if column_ref.startswith('/'):
                            column_name = column_ref[1:]
                            if column_name in row_data:
                                value = row_data[column_name]
                                if not pd.isna(value):
                                    return substring in str(value)
                            return False
                return False
            
            # Handle matchesRegex() function
            elif condition.startswith('matchesRegex(') and condition.endswith(')'):
                # Use the existing matchesRegex evaluation method
                result = self._evaluate_matchesregex(condition, row_data)
                return result == 'true'
            
            # Handle simple equality conditions like "/MARKET = '072'"
            if '=' in condition:
                parts = condition.split('=')
                if len(parts) == 2:
                    left_part = parts[0].strip()
                    right_part = parts[1].strip().strip("'\"")
                    
                    # Check if left part is a column reference
                    if left_part.startswith('/'):
                        column_name = left_part[1:]  # Remove leading slash
                        
                        # Get the actual value from row data
                        if column_name in row_data:
                            actual_value = row_data[column_name]
                            if not pd.isna(actual_value):
                                actual_value_str = str(actual_value)
                                # Compare with the expected value
                                return actual_value_str == right_part
                            return False
                        else:
                            self.logger.warning(f"Column '{column_name}' not found in data for filter: {condition}")
                            return False
            
            # If we can't evaluate, assume it fails
            self.logger.warning(f"Could not evaluate condition: {condition}")
            return False
            
        except Exception as e:
            self.logger.warning(f"Error evaluating single condition '{condition}': {str(e)}")
            return False
    
    def find_matching_rule(self, kpi_detail_id: int, row_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Find the first matching rule for a given KPI detail that satisfies the input filter.
        If no condition satisfies, returns None to indicate the process should stop for this row.
        
        Args:
            kpi_detail_id: The KPI detail ID to find rules for
            row_data: Dictionary containing row data for filter evaluation
            
        Returns:
            The first matching rule or None if no rules match (process should stop)
        """
        matching_rules = []
        
        # Find all rules for this KPI detail
        for rule in self.kpi_resource_rules:
            rule_kpi_detail_id = rule.get('KpiDetailID')
            # Convert to int if it's a string
            if isinstance(rule_kpi_detail_id, str):
                try:
                    rule_kpi_detail_id = int(rule_kpi_detail_id)
                except ValueError:
                    continue
            if rule_kpi_detail_id == kpi_detail_id:
                # Check if IncludeKpiFilter is "1" and evaluate the filter
                include_filter = rule.get('IncludeKpiFilter', '')
                if include_filter == '1':
                    input_filter = rule.get('InputFilter', '')
                    if self.evaluate_rule_expression(input_filter, row_data):
                        matching_rules.append(rule)
                else:
                    # If IncludeKpiFilter is not "1", include the rule
                    input_filter = rule.get('InputFilter', '')
                    self.logger.info(f"Input filter: {input_filter}")
                    #self.logger.info(f"Input filter: {input_filter}")
                    if self.evaluate_rule_expression(input_filter, row_data):
                        matching_rules.append(rule)
        #self.logger.info(f"Matching rules: {matching_rules}")                    
        # Return the first matching rule (as per requirements)
        if matching_rules:
            return matching_rules[0]
        
        # If no condition satisfies, return None to stop the process for this row
        return None
    
    def _evaluate_displaynameconcat(self, expression: str, row_data: Dict[str, Any]) -> str:
        # 1. Remove "concat(" at the start and ")" at the end
        inner = expression.strip()
        if inner.startswith("concat(") and inner.endswith(")"):
            inner = inner[len("concat("):-1]

        # 2. Split by commas, strip spaces and quotes
        parts = [p.strip().strip('"').strip("'") for p in inner.split(",")]

        # 3. Remove leading "/" from each part
        parts = [p.lstrip("/") for p in parts]

        # 4. Concatenate into one string
        return "".join(parts)


    def evaluate_displaynameexpression(self, expression: str, row_data: Dict[str, Any]) -> str:
        if expression.startswith('/'):
            column_name = expression[1:]  # Remove leading slash
        elif expression.startswith('concat(') and expression.endswith(')'):
            column_name = self._evaluate_displaynameconcat(expression, row_data)
        else:
            column_name = ""
        return column_name
        
    def transform_data(self, input_file: str, output_file: str) -> None:
        """
        Transform input CSV data based on KPI details and resource rules.
        Creates output rows only for input rows that satisfy filter conditions.
        If no condition satisfies, the row is excluded from output.
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output CSV file
        """
        try:
            #self.logger.info(f"Starting data transformation from {input_file} to {output_file}")
            
            # Load input data - force all columns to be strings to preserve leading zeros
            input_df = pd.read_csv(input_file, dtype=str, na_filter=False)
            #self.logger.info(f"Loaded {len(input_df)} rows from input file")
            
            datasourceid = self.get_datasource_from_filename(input_file)
            self.logger.info(f"DatasourceID: {datasourceid}")
            matching_kpi_detail, kpidetailid = self.get_kpidetails_fordatasource(datasourceid)
            #self.logger.info(f"KpiDetailID: {kpidetailid}")

            # Create output dataframe with required columns (including new ones)
            base_columns = ['Market', 'Region', 'VCPType', 'Technology', 'Datacenter', 'Site', 'Id', 'DisplayName']
            output_data = []
            processed_rows = 0
            excluded_rows = 0
            
            # Process each row - only include rows that satisfy filter conditions
            for index, row in input_df.iterrows():
                if index % 1000 == 0:
                    self.logger.info(f"Processing row {index + 1}/{len(input_df)}")
                
                # Convert row to dict, ensuring all values are strings to preserve leading zeros
                row_dict = {col: str(val) if not pd.isna(val) else '' for col, val in row.to_dict().items()}
                #self.logger.info(f"Row dict: {row_dict}")
                
                # Find the appropriate KPI detail and matching rule for this row
                matching_rule = None
                
                # Check each KPI detail to find one with a matching rule
                kpi_detail_id = kpidetailid
                if kpi_detail_id:
                    #self.logger.info(f"Going inside find_matching_rule function")
                    rule = self.find_matching_rule(kpi_detail_id, row_dict)
                    #self.logger.info(f"Finished inside find_matching_rule function")
                    if rule:
                        matching_rule = rule
                        #self.logger.info(f"Matching rule inside: {matching_rule}")
                        # break
                self.logger.info(f"Matching rule ouside: {matching_rule}")

                # If no matching rule found and we have resource rules, exclude this row
                if not matching_rule:
                    excluded_rows += 1
                    self.logger.debug(f"Row {index + 1} excluded - no matching rule found")
                    continue
                
                #self.logger.info(f"Error here")
                #self.logger.info(f"Matching kpi detail: {matching_kpi_detail}")               
                if matching_kpi_detail:
                    # Evaluate each expression from KPI detail
                    market = self.evaluate_expression(matching_kpi_detail.get('MarketExpr', ''), row_dict)
                    region = self.evaluate_expression(matching_kpi_detail.get('RegionExpr', ''), row_dict)
                    vcp_type = self.evaluate_expression(matching_kpi_detail.get('VcpTypeExpr', ''), row_dict)
                    technology = self.evaluate_expression(matching_kpi_detail.get('TechnologyExpr', ''), row_dict)
                    datacenter = self.evaluate_expression(matching_kpi_detail.get('DataCenterExpr', ''), row_dict)
                    site = self.evaluate_expression(matching_kpi_detail.get('SiteExpr', ''), row_dict)
                    
                    # Evaluate expressions from matching rule (if available)
                    id_value = ''
                    display_name = ''
                    if matching_rule:
                        
                        #self.logger.info(f"Line number 1427")
                        id_expr = matching_rule.get('IdExpr', '')
                        display_name_expr = matching_rule.get('DisplayNameExpr', '')
                        id_value = self.evaluate_expression(id_expr, row_dict)
                        #self.logger.info(f"Id value: {id_value}")
                        display_name = self.evaluate_displaynameexpression(display_name_expr, row_dict)
                        #self.logger.info(f"Display name: {display_name}")   

                        # Handle KPIRuleRules if available
                        kpi_resource_rule_id = matching_rule.get('KpiResourceRuleID')
                        #self.logger.info(f"Kpi resource rule ID: {kpi_resource_rule_id}")

                        if kpi_resource_rule_id and self.kpi_rule_rules:
                            # Convert to int if it's a string
                            if isinstance(kpi_resource_rule_id, str):
                                try:
                                    kpi_resource_rule_id = int(kpi_resource_rule_id)
                                except ValueError:
                                    kpi_resource_rule_id = None
                            
                            if kpi_resource_rule_id:
                                kpi_rule_rules = self.find_kpi_rule_rules(kpi_resource_rule_id)
                                if kpi_rule_rules:
                                    # Take the first row for Timestamp column
                                    first_rule = kpi_rule_rules[0]
                                    timestamp_expr = first_rule.get('KpiTimestampExpr', '')
                                    #self.logger.info(f"Found timestamp expression: {timestamp_expr}")
                                    #self.logger.info(f"Timestamp expression type: {type(timestamp_expr)}")
                                    #self.logger.info(f"Timestamp expression starts with toDate: {timestamp_expr.startswith('toDate(') if timestamp_expr else False}")
                                    if timestamp_expr:
                                        timestamp_value = self.evaluate_expression(timestamp_expr, row_dict)
                                        #self.logger.info(f"Evaluated timestamp value: {timestamp_value}")
                                        row_dict['_timestamp'] = timestamp_value
                                    
                                    # Process all rules for dynamic KPI columns
                                    for rule in kpi_rule_rules:
                                        kpi_name = rule.get('KpiName', '')
                                        kpi_value_expr = rule.get('KpiValueExpr', '')
                                        kpi_interval_expr = rule.get('KpiIntervalExpr', '')
                                        
                                        if kpi_name and kpi_value_expr:
                                            kpi_value = self.evaluate_expression(kpi_value_expr, row_dict)
                                            row_dict[f'_kpi_{kpi_name}'] = kpi_value
                                        
                                        # Only set freq from the first rule that has it (as per requirements)
                                        if kpi_interval_expr and '_freq' not in row_dict:
                                            freq_value = self.evaluate_expression(kpi_interval_expr, row_dict)
                                            row_dict['_freq'] = freq_value
                    
                    # Process NFName requirement using pre-evaluated values
                    if hasattr(self, 'kpi_data_source_props') and self.kpi_data_source_props:
                        try:
                            # Get NFName expression for this datasource
                            nfname_expr = self.get_nfname_expr_from_datasource(int(datasourceid))
                            if nfname_expr:
                                # Get timestamp value (already evaluated above)
                                timestamp_value = row_dict.get('_timestamp', '')
                                if not timestamp_value:
                                    from datetime import datetime
                                    timestamp_value = datetime.now().isoformat()
                                
                                # Process NFName requirement with pre-evaluated values
                                self.process_nfname_requirement_with_values(
                                    id_expr=id_value,
                                    timestamp=timestamp_value,
                                    region=region,
                                    nfname_expr=nfname_expr,
                                    row_data=row_dict
                                )
                        except Exception as e:
                            self.logger.warning(f"Error processing NFName requirement: {str(e)}")
                    
                    # Create output row
                    output_row = {
                        'Market': market,
                        'Region': region,
                        'VCPType': vcp_type,
                        'Technology': technology,
                        'Datacenter': datacenter,
                        'Site': site,
                        'Id': id_value
                    }

                    # if display_name and id_value:
                    #    output_row[display_name] = id_value
                    
                    # Add Timestamp column if available
                    if '_timestamp' in row_dict:
                        output_row['Timestamp'] = row_dict['_timestamp']
                    
                    # Add Freq column if available
                    if '_freq' in row_dict:
                        output_row['Freq'] = row_dict['_freq']
                    
                    # Add dynamic KPI columns
                    for key, value in row_dict.items():
                        if key.startswith('_kpi_'):
                            kpi_name = key[5:]  # Remove '_kpi_' prefix
                            output_row[kpi_name] = value
                    
                    output_data.append(output_row)
                    processed_rows += 1
                else:
                    # If no KPI details and no matching rules, exclude this row
                    excluded_rows += 1
                    self.logger.debug(f"Row {index + 1} excluded - no matching rule found")
            
            # Create output dataframe
            output_df = pd.DataFrame(output_data)
            
            # Ensure Id column is treated as string to preserve leading zeros
            if 'Id' in output_df.columns:
                output_df['Id'] = output_df['Id'].astype(str)
            
            # Save to output file
            output_df.to_csv(output_file, index=False)
            #self.logger.info(f"Transformation completed. Output saved to {output_file}")
            #self.logger.info(f"Output contains {len(output_df)} rows (processed: {processed_rows}, excluded: {excluded_rows})")
            
        except Exception as e:
            self.logger.error(f"Error during data transformation: {str(e)}")
            raise
    
    def process(self, 
                input_file: str, 
                kpi_data_sources_file: str, 
                kpi_details_file: str, 
                output_file: str,
                kpi_resource_types_file: str = None,
                kpi_resource_rules_file: str = None,
                kpi_rule_rules_file: str = None,
                kpi_data_source_props_file: str = None) -> None:
        """
        Main processing method that orchestrates the entire ETL process.
        
        Args:
            input_file: Path to input CSV file
            kpi_data_sources_file: Path to KPIDatasources.csv file
            kpi_details_file: Path to KPIDetails.csv file
            output_file: Path to output CSV file
            kpi_resource_types_file: Path to KPIResourceTypes.csv file (optional)
            kpi_resource_rules_file: Path to KPIResourceRules.csv file (optional)
            kpi_rule_rules_file: Path to KpiRuleRules.csv file (optional)
            kpi_data_source_props_file: Path to KpiDataSourceProps.csv file (optional)
        """
        try:
            self.logger.info("Starting ETL process")
            
            # Load configuration data
            self.load_kpi_data_sources(kpi_data_sources_file)
            self.load_kpi_details(kpi_details_file)
            
            # Load optional resource tables if provided
            if kpi_resource_types_file:
                self.load_kpi_resource_types(kpi_resource_types_file)
            
            if kpi_resource_rules_file:
                self.load_kpi_resource_rules(kpi_resource_rules_file)
            
            if kpi_rule_rules_file:
                self.load_kpi_rule_rules(kpi_rule_rules_file)
            
            # Load KpiDataSourceProps for NFName functionality
            if kpi_data_source_props_file:
                self.load_kpi_data_source_props(kpi_data_source_props_file)
            
            # Transform data
            self.transform_data(input_file, output_file)
            
            self.logger.info("ETL process completed successfully")
            
        except Exception as e:
            self.logger.error(f"ETL process failed: {str(e)}")
            raise

    def get_datasource_from_filename(self, filename: str) -> str:
        """
        Returns the datasource from the filename.
        """
        import os
        return os.path.basename(filename).split("-", 1)[0]

    def get_kpidetails_fordatasource(self, datasourceid: int):
        """
        Returns the latest dictionary from self.kpi_details 
        where datasourceid, based on update_date.
        """
        
        #self.logger.info(f"KPI DETAILS: {self.kpi_details}")
        filtered = [item for item in self.kpi_details if int(item.get("KpiDataSourceID")) == int(datasourceid)]
        
        #self.logger.info(f"Filtered: {filtered}")
        if not filtered:
            return None  # return None if no match found
        
        latest = max(filtered, key=lambda x: x.get("updated"))
        return latest, latest.get("KpiDetailID")


    def evaluate_rule_expression(self, expr: str, data: Dict[str, Any]) -> bool:
        """
        Evaluate a DSL expression against a dictionary.
        Supports:
        - [ ... ] AND blocks
        - { [ ... ], [ ... ] } OR blocks
        - Functions: isBlank, isEmpty, contains, matchesRegex
        - Nested keys like /labels/group
        - Missing keys handled correctly
        """
        #self.logger.info(f"Evaluating rule expression: {expr}")
        #self.logger.info(f"Data: {data}")
        # --- Helper functions ---
        def get_value(path: str):
            path = path.strip().lstrip("/")
            parts = path.split("/")
            #self.logger.info(f"Parts: {parts}")
            value = data
            for p in parts:
                if isinstance(value, dict) and p in value:
                    value = value[p]
                else:
                    return None
            return value

        def isBlank(path: str):
            #self.logger.info(f"Evaluating isBlank: {path}")
            #val = get_value(path)
            #self.logger.info(f"Path: {path}")
            # Treat None, empty string, or "NaN" as blank
            #self.logger.info(f"Evaluating isBlank: {path}")
            if path is None:
                return True
            val_str = str(path).strip()
            #self.logger.info(f"Val str: {val_str}")
            if val_str == "" or val_str.lower() == "nan":
                return True
            return False


        def isEmpty(path: str):
            val = get_value(path)
            return val is None or val == "" or (isinstance(val, (list, dict)) and len(val) == 0)

        def contains(path: str, substr: str):
            val = get_value(path)
            return val is not None and substr in str(val)

        def matchesRegex(path: str, pattern: str):
            self.logger.info(f"Evaluating matchesRegex: {path}, {pattern}")
            #val = get_value(path)
            #self.logger.info(f"Val: {val}")
            if path is None:
                return False
            val_str = str(path).strip()
            self.logger.info(f"Val_str after strip: '{val_str}'")
            match_result = re.search(pattern, val_str)
            self.logger.info(f"Re.search(pattern, val_str): {match_result}")
            return match_result is not None

        def eval_block(block: str) -> bool:
            block = block.strip()
            block = re.sub(r"(?<![=!<>])=(?!=)", "==", block)
            # Replace /keys with get_value
            def replace_key(match):
                key = match.group(0)
                return f"get_value('{key}')"
            block = re.sub(r"/[\w\-/]+", replace_key, block)
            self.logger.info(f"Block after replace: {block}")

            try:
                return eval(block, {"__builtins__": {}}, {
                    "isBlank": isBlank,
                    "isEmpty": isEmpty,
                    "contains": contains,
                    "matchesRegex": matchesRegex,
                    "get_value": get_value
                })
            except Exception as e:
                print(f"Error evaluating block '{block}': {e}")
                return False

        expr = expr.strip()

        # --- OR groups { ... } ---
        if expr.startswith("{") and expr.endswith("}"):
            inner = expr[1:-1]
            blocks = re.findall(r'\[([^\[\]]+)\]', inner)
            return any(eval_block(b) for b in blocks)

        # --- AND blocks [ ... ][ ... ] ---
        blocks = re.findall(r'\[([^\[\]]+)\]', expr)
        self.logger.info(f"Blocks: {blocks}")
        results = []
        for b in blocks:
            self.logger.info(f"Evaluating block: {b}")
            res = eval_block(b)
            self.logger.info(f"Block: {b}; Result: {res}")
            results.append(res)
        return all(results)

        # Not recognized
        return False

    def get_nfname_expr_from_datasource(self, datasource_id: int) -> str:
        """
        Get NFName expression from KpiDataSourceProps based on datasource ID.
        
        Args:
            datasource_id: The KPI data source ID
            
        Returns:
            NFName expression string or empty string if not found
        """
        for prop in self.kpi_data_source_props:
            if int(prop.get('KpiDataSourceID', 0)) == datasource_id:
                return prop.get('NfNameExpr', '')
        return ''

    def create_nfname_results_table(self) -> None:
        """
        Create the NFName results table in the database.
        """
        if not self.db_config:
            self.logger.warning("Database configuration not provided, cannot create table")
            return
        
        try:
            conn = self.get_db_connection()
            with conn.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS nfname_results (
                    id SERIAL PRIMARY KEY,
                    id_expr VARCHAR(255),
                    timestamp TIMESTAMP WITH TIME ZONE,
                    region VARCHAR(255),
                    nfname_expr_value VARCHAR(255),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                """
                cursor.execute(create_table_sql)
                conn.commit()
                self.logger.info("NFName results table created successfully")
                
        except Exception as e:
            self.logger.error(f"Error creating NFName results table: {str(e)}")
            raise

    def store_nfname_data(self, id_expr: str, timestamp: str, region: str, nfname_expr_value: str) -> None:
        """
        Store NFName data in the database.
        This is the new function as required in nfnamereq.txt.
        
        Args:
            id_expr: The pre-evaluated ID expression value
            timestamp: The pre-evaluated timestamp value
            region: The pre-evaluated region value
            nfname_expr_value: The evaluated NFName expression value
        """
        if not self.db_config:
            self.logger.warning("Database configuration not provided, cannot store result")
            return
        
        try:
            conn = self.get_db_connection()
            with conn.cursor() as cursor:
                insert_sql = """
                INSERT INTO nfname_results (id_expr, timestamp, region, nfname_expr_value)
                VALUES (%s, %s, %s, %s)
                """
                cursor.execute(insert_sql, (id_expr, timestamp, region, nfname_expr_value))
                conn.commit()
                self.logger.debug(f"Stored NFName data: id_expr={id_expr}, timestamp={timestamp}, region={region}, nfname_expr_value={nfname_expr_value}")
                
        except Exception as e:
            self.logger.error(f"Error storing NFName data: {str(e)}")
            raise

    def process_nfname_requirement(self, input_file: str, kpi_data_sources_file: str, kpi_details_file: str, kpi_data_source_props_file: str) -> None:
        """
        Process the NFName requirement as specified in nfnamereq.txt:
        1. Use KpiDataSourceProps.csv to get the NFName
        2. Use the NfNameExpr to calculate the expression value
        3. Store Id_expr, timestamp, region and NFNameExpr value in a new table
        
        Args:
            input_file: Path to input CSV file
            kpi_data_sources_file: Path to KPIDatasources.csv file
            kpi_details_file: Path to KPIDetails.csv file
            kpi_data_source_props_file: Path to KpiDataSourceProps.csv file
        """
        try:
            self.logger.info("Starting NFName requirement processing")
            
            # Load configuration data
            self.load_kpi_data_sources(kpi_data_sources_file)
            self.load_kpi_details(kpi_details_file)
            self.load_kpi_data_source_props(kpi_data_source_props_file)
            
            # Create the NFName results table
            self.create_nfname_results_table()
            
            # Get datasource ID from filename
            datasource_id = int(self.get_datasource_from_filename(input_file))
            self.logger.info(f"Processing datasource ID: {datasource_id}")
            
            # Get NFName expression for this datasource from KpiDataSourceProps
            nfname_expr = self.get_nfname_expr_from_datasource(datasource_id)
            self.logger.info(f"NFName expression for datasource {datasource_id}: {nfname_expr}")
            
            if not nfname_expr:
                self.logger.warning(f"No NFName expression found for datasource {datasource_id}")
                return
            
            # Load input data
            input_df = pd.read_csv(input_file, dtype=str, na_filter=False)
            self.logger.info(f"Loaded {len(input_df)} rows from input file")
            
            # Find matching KPI details for this datasource
            matching_kpi_detail, kpi_detail_id = self.get_kpidetails_fordatasource(datasource_id)
            
            if not matching_kpi_detail:
                self.logger.warning(f"No matching KPI details found for datasource {datasource_id}")
                return
            
            # Process each row
            for index, row in input_df.iterrows():
                if index % 1000 == 0:
                    self.logger.info(f"Processing row {index + 1}/{len(input_df)}")
                
                # Convert row to dict
                row_dict = {col: str(val) if not pd.isna(val) else '' for col, val in row.to_dict().items()}
                
                # Find matching rule
                matching_rule = self.find_matching_rule(kpi_detail_id, row_dict)
                
                if not matching_rule:
                    continue
                
                # Get pre-evaluated values from the existing process
                # These should be passed in from the calling process that has already evaluated them
                id_expr = row_dict.get('_id_expr', '')  # Pre-evaluated ID expression
                region = row_dict.get('_region', '')    # Pre-evaluated region
                timestamp = row_dict.get('_timestamp', '')  # Pre-evaluated timestamp
                
                # If pre-evaluated values are not available, fall back to evaluation
                if not id_expr:
                    id_expr = self.evaluate_expression(matching_rule.get('IdExpr', ''), row_dict)
                if not region:
                    region = self.evaluate_expression(matching_kpi_detail.get('RegionExpr', ''), row_dict)
                if not timestamp:
                    from datetime import datetime
                    timestamp = datetime.now().isoformat()
                
                # Use the NfNameExpr to calculate the expression value
                nfname_expr_value = self.evaluate_expression(nfname_expr, row_dict)
                
                # Store the result using the new function
                if id_expr and nfname_expr_value:
                    self.store_nfname_data(id_expr, timestamp, region, nfname_expr_value)
            
            self.logger.info("NFName requirement processing completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error processing NFName requirement: {str(e)}")
            raise

    def process_nfname_requirement_with_values(self, id_expr: str, timestamp: str, region: str, nfname_expr: str, row_data: Dict[str, Any]) -> None:
        """
        Process NFName requirement using pre-evaluated values.
        This method is designed to be called from the existing process that has already
        evaluated id_expr, timestamp, and region.
        
        Args:
            id_expr: Pre-evaluated ID expression value
            timestamp: Pre-evaluated timestamp value
            region: Pre-evaluated region value
            nfname_expr: The NFName expression to evaluate
            row_data: Row data for evaluating the NFName expression
        """
        try:
            # Create the NFName results table if it doesn't exist
            self.create_nfname_results_table()
            
            # Evaluate the NFName expression
            nfname_expr_value = self.evaluate_expression(nfname_expr, row_data)
            
            # Store the result using the new function
            if id_expr and nfname_expr_value:
                self.store_nfname_data(id_expr, timestamp, region, nfname_expr_value)
                self.logger.debug(f"Stored NFName data with pre-evaluated values: id_expr={id_expr}, timestamp={timestamp}, region={region}, nfname_expr_value={nfname_expr_value}")
            
        except Exception as e:
            self.logger.error(f"Error processing NFName requirement with pre-evaluated values: {str(e)}")
            raise


def main():
    """Main function to run the ETL process."""
    # Database configuration will be loaded from database.env file
    # Make sure to update database.env with your actual database credentials
    
    # Use context manager to ensure database connections are properly closed
    with ETLProcessor() as processor:
        import os

        folder_path = "./csvfiles"

        for file in os.listdir(folder_path):
            if file.endswith(".csv") and os.path.isfile(os.path.join(folder_path, file)):
                filename = os.path.splitext(file)[0]
                print(f"filename to be processed: {filename}")

                # File paths 
                input_file = f"{folder_path}/{filename}.csv"
                kpi_data_sources_file = "./configfiles/KPIDatasources.csv"
                kpi_details_file = "./configfiles/KPIDetails.csv"
                output_file = f"{folder_path}/{filename}_output.csv"
                kpi_resource_types_file = "./configfiles/KPIResourceTypes.csv"  # Optional
                kpi_resource_rules_file = "./configfiles/KPIResourceRules.csv"   # Optional
                kpi_rule_rules_file = "./configfiles/KpiRuleRules.csv"           # Optional
                kpi_data_source_props_file = "./configfiles/KpiDataSourceProps.csv"  # For NFName functionality
        
                try:
                    processor.process(
                        input_file=input_file,
                        kpi_data_sources_file=kpi_data_sources_file,
                        kpi_details_file=kpi_details_file,
                        output_file=output_file,
                        kpi_resource_types_file=kpi_resource_types_file,
                        kpi_resource_rules_file=kpi_resource_rules_file,
                        kpi_rule_rules_file=kpi_rule_rules_file,
                        kpi_data_source_props_file=kpi_data_source_props_file
                    )
                    print("ETL process completed successfully!")
                    
                except Exception as e:
                    print(f"ETL process failed: {str(e)}")
                        
    return 0


if __name__ == "__main__":
    exit(main())
