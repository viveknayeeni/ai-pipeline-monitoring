# src/log_processor.py
import re
import json
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path

class LogProcessor:
    def __init__(self):
        self.error_patterns = {
            'connection_timeout': r'connection.*timeout|timed out|Connection timed out',
            'memory_error': r'OutOfMemoryError|Java heap space|memory.*exceeded',
            'permission_denied': r'AccessDenied|permission.*denied|not authorized',
            'file_not_found': r'FileNotFoundException|No such file|does not exist',
            'sql_error': r'SQLException|syntax error|PARSE_SYNTAX_ERROR',
            'network_error': r'NetworkException|connection refused|Connection refused'
        }
        
        self.service_patterns = {
            'airflow': r'airflow|taskinstance\.py',
            'emr': r'org\.apache\.spark|SparkException',
            'lambda': r'ERROR.*Z.*lambda_handler|botocore\.exceptions',
            'databricks': r'SparkSQLException|databricks|notebook'
        }
    
    def process_log_file(self, file_path: str) -> Dict:
        """Process a single log file and extract error information"""
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            return self._extract_error_info(content, file_path)
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            return {}
    
    def _extract_error_info(self, content: str, file_path: str) -> Dict:
        """Extract structured error information from log content"""
        lines = content.split('\n')
        
        # Extract key information
        error_info = {
            'file_path': file_path,
            'timestamp': self._extract_timestamp(content),
            'service': self._identify_service(content),
            'error_type': self._classify_error(content),
            'pipeline_name': self._extract_pipeline_name(content),
            'step_name': self._extract_step_name(content),
            'error_message': self._extract_main_error(content),
            'stack_trace': self._extract_stack_trace(content),
            'full_content': content
        }
        
        return error_info
    
    def _extract_timestamp(self, content: str) -> Optional[str]:
        """Extract timestamp from log content"""
        patterns = [
            r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\]',  # Airflow format
            r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+',      # EMR format
            r'\[ERROR\] (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)',  # Lambda format
            r'(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})'           # Databricks format
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                return match.group(1)
        
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def _identify_service(self, content: str) -> str:
        """Identify which service generated the log"""
        for service, pattern in self.service_patterns.items():
            if re.search(pattern, content, re.IGNORECASE):
                return service
        return 'unknown'
    
    def _classify_error(self, content: str) -> str:
        """Classify the type of error"""
        for error_type, pattern in self.error_patterns.items():
            if re.search(pattern, content, re.IGNORECASE):
                return error_type
        return 'unknown_error'
    
    def _extract_pipeline_name(self, content: str) -> Optional[str]:
        """Extract pipeline name from log content"""
        patterns = [
            r'Pipeline:\s*([^\s,]+)',
            r'Task\s+([^.]+\.[^.]+)',
            r'Function:\s*([^\s,]+)',
            r'Notebook:\s*/[^/]+/([^/]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                return match.group(1)
        
        return 'unknown_pipeline'
    
    def _extract_step_name(self, content: str) -> Optional[str]:
        """Extract step/task name from log content"""
        patterns = [
            r'Step:\s*([^\s,\n]+)',
            r'Task\s+[^.]+\.([^.\s]+)',
            r'Stage\s+(\d+)',
            r'Application:\s*([^\s,]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                return match.group(1)
        
        return 'unknown_step'
    
    def _extract_main_error(self, content: str) -> str:
        """Extract the main error message"""
        error_lines = []
        for line in content.split('\n'):
            if any(keyword in line.upper() for keyword in ['ERROR', 'EXCEPTION', 'FAILED']):
                error_lines.append(line.strip())
        
        return '\n'.join(error_lines[:3])  # First 3 error lines
    
    def _extract_stack_trace(self, content: str) -> str:
        """Extract stack trace information"""
        lines = content.split('\n')
        stack_trace = []
        in_trace = False
        
        for line in lines:
            if 'Traceback' in line or 'Exception' in line:
                in_trace = True
            elif in_trace and (line.strip().startswith('at ') or line.strip().startswith('File ') or '\t' in line):
                stack_trace.append(line.strip())
            elif in_trace and line.strip() and not any(char in line for char in ['\t', '  at', 'File']):
                break
        
        return '\n'.join(stack_trace[:10])  # Limit to 10 lines

# Test the processor
if __name__ == "__main__":
    processor = LogProcessor()
    
    # Test with sample logs
    log_files = [
        'data/sample_logs/airflow_failure.log',
        'data/sample_logs/emr_memory_error.log',
        'data/sample_logs/lambda_permission_error.log',
        'data/sample_logs/databricks_sql_error.log'
    ]
    
    for log_file in log_files:
        try:
            result = processor.process_log_file(log_file)
            print(f"\n=== Processing {log_file} ===")
            print(json.dumps(result, indent=2))
        except FileNotFoundError:
            print(f"File not found: {log_file}")