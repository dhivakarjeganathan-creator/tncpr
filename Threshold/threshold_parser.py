"""
JSON parser for threshold definitions and rule extraction.
"""

import json
from typing import List, Dict, Any, Optional, Tuple

class ThresholdParser:
    """Parser for threshold definition JSON files."""
    
    def __init__(self, json_file_path: str):
        """Initialize parser with JSON file path."""
        self.json_file_path = json_file_path
        self.threshold_data = None
    
    def load_json(self):
        """Load and parse the JSON file."""
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as file:
                self.threshold_data = json.load(file)
            print(f"Successfully loaded JSON from {self.json_file_path}")
        except FileNotFoundError:
            raise Exception(f"JSON file not found: {self.json_file_path}")
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON format: {e}")
    
    def detect_mode(self, evaluation: Dict[str, Any]) -> Optional[str]:
        """
        Detect the mode (burst or period) based on enabled flags.
        Returns 'burst' if any burst_*_enabled is True, 'period' if any period_*_enabled is True.
        """
        # Check for burst mode
        burst_enabled = any(
            evaluation.get(f"burst_{category}_enabled", False) 
            for category in ['critical', 'major', 'minor', 'warning']
        )
        
        # Check for period mode
        period_enabled = any(
            evaluation.get(f"period_{category}_enabled", False) 
            for category in ['critical', 'major', 'minor', 'warning']
        )
        
        if burst_enabled and period_enabled:
            # If both are enabled, prioritize based on which has more enabled rules
            burst_count = sum(
                1 for category in ['critical', 'major', 'minor', 'warning']
                if evaluation.get(f"burst_{category}_enabled", False)
            )
            period_count = sum(
                1 for category in ['critical', 'major', 'minor', 'warning']
                if evaluation.get(f"period_{category}_enabled", False)
            )
            return 'burst' if burst_count >= period_count else 'period'
        elif burst_enabled:
            return 'burst'
        elif period_enabled:
            return 'period'
        else:
            return None
    
    def detect_categories(self, evaluation: Dict[str, Any]) -> List[str]:
        """
        Detect all enabled categories (critical, major, minor, warning).
        Returns a list of categories that are enabled.
        """
        categories = []
        
        for category in ['critical', 'major', 'minor', 'warning']:
            # Check both burst and period modes for this category
            burst_enabled = evaluation.get(f"burst_{category}_enabled", False)
            period_enabled = evaluation.get(f"period_{category}_enabled", False)
            
            if burst_enabled or period_enabled:
                categories.append(category)
        
        return categories
    
    def extract_rule_data(self, evaluation: Dict[str, Any], mode: str, category: str, threshold_def: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Extract rule data for a specific mode and category combination.
        """
        # Use provided threshold_def or fall back to self.threshold_data
        if threshold_def is None:
            threshold_def = self.threshold_data
        
        # Get the base key prefix - matches updated requirements: {mode}_{category}
        prefix = f"{mode}_{category}"
        
        # Extract values using the concatenated keys
        rule_data = {
            'name': threshold_def.get('name', ''),
            'metric': threshold_def.get('metric', ''),
            'mode': mode,
            'category': category,
            'lowerlimit': self._safe_float(evaluation.get(f"{prefix}_lower_limit", "0.0")),
            'upperlimit': self._safe_float(evaluation.get(f"{prefix}_upper_limit", "0.0")),
            'occurrence': evaluation.get(f"{prefix}_occurrence", 0),
            'clearoccurrence': evaluation.get(f"{prefix}_clear_occurrence", 0),
            'cleartime': evaluation.get(f"{prefix}_clear_time", 0),
            'time': evaluation.get(f"{prefix}_time", 0),
            'activeuntil': evaluation.get('active_until', ''),
            'periodgranularity': evaluation.get('period_granularity', 0),
            'schedule': evaluation.get('schedule', ''),
            'tag': threshold_def.get('tag', ''),
            'user_groups': threshold_def.get('user_groups', ''),
            'resource': self._safe_json_string(threshold_def.get('resource', [])),
            'threshold_group': self._safe_json_string(threshold_def.get('threshold_group', [])),
            'target_rule': threshold_def.get('target_rule', ''),
            'can_edit': threshold_def.get('can_edit', False),
            'owner': threshold_def.get('owner', ''),
            'update_time': threshold_def.get('update_time', 0)
        }
        
        return rule_data
    
    def _safe_float(self, value: Any) -> float:
        """Safely convert value to float."""
        try:
            return float(value) if value is not None else 0.0
        except (ValueError, TypeError):
            return 0.0
    
    def _safe_json_string(self, value: Any) -> str:
        """Safely convert value to JSON string."""
        try:
            if value is None:
                return ''
            if isinstance(value, (list, dict)):
                return json.dumps(value)
            return str(value)
        except (ValueError, TypeError):
            return ''
    
    def process_evaluations(self) -> List[Dict[str, Any]]:
        """
        Process all evaluations and extract threshold rules.
        Returns a list of rule dictionaries.
        """
        if not self.threshold_data:
            raise Exception("JSON data not loaded. Call load_json() first.")
        
        rules = []
        
        # Handle both single object and array of objects
        if isinstance(self.threshold_data, list):
            # JSON contains an array of threshold definitions
            threshold_definitions = self.threshold_data
        else:
            # JSON contains a single threshold definition
            threshold_definitions = [self.threshold_data]
        
        for threshold_def in threshold_definitions:
            evaluations = threshold_def.get('evaluations', [])
            
            for evaluation in evaluations:
                # Detect mode for this evaluation
                mode = self.detect_mode(evaluation)
                if not mode:
                    print("Warning: No valid mode detected for evaluation, skipping...")
                    continue
                
                # Detect categories for this evaluation
                categories = self.detect_categories(evaluation)
                if not categories:
                    print("Warning: No valid categories detected for evaluation, skipping...")
                    continue
                
                # Create a rule for each category
                for category in categories:
                    rule_data = self.extract_rule_data(evaluation, mode, category, threshold_def)
                    rules.append(rule_data)
                    print(f"Extracted rule: {category} {mode} - {rule_data['name']}")
        
        return rules
    
    def get_threshold_info(self) -> Dict[str, Any]:
        """Get basic threshold information."""
        if not self.threshold_data:
            raise Exception("JSON data not loaded. Call load_json() first.")
        
        # Handle both single object and array of objects
        if isinstance(self.threshold_data, list):
            # For array, get info from first item and total count
            first_item = self.threshold_data[0] if self.threshold_data else {}
            total_evaluations = sum(len(item.get('evaluations', [])) for item in self.threshold_data)
            return {
                'name': f"Multiple thresholds ({len(self.threshold_data)} items)",
                'metric': first_item.get('metric', ''),
                'creation_time': first_item.get('creation_time', 0),
                'update_time': first_item.get('update_time', 0),
                'owner': first_item.get('owner', ''),
                'evaluation_count': total_evaluations,
                'threshold_count': len(self.threshold_data)
            }
        else:
            # Single object
            return {
                'name': self.threshold_data.get('name', ''),
                'metric': self.threshold_data.get('metric', ''),
                'creation_time': self.threshold_data.get('creation_time', 0),
                'update_time': self.threshold_data.get('update_time', 0),
                'owner': self.threshold_data.get('owner', ''),
                'evaluation_count': len(self.threshold_data.get('evaluations', [])),
                'threshold_count': 1
            }
