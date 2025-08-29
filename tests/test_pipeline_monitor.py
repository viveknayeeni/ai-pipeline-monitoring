# test_pipeline_monitor.py
#!/usr/bin/env python3

import sys
import os
from pathlib import Path

# Add src to path
sys.path.append('src')

from log_processor import LogProcessor
from llm_analyzer import LLMAnalyzer

def main():
    print("🚀 Testing AI Pipeline Monitor")
    print("=" * 50)
    
    # Initialize components
    processor = LogProcessor()
    analyzer = LLMAnalyzer()
    
    # Test log files
    log_files = [
        'data/sample_logs/airflow_failure.log',
        'data/sample_logs/emr_memory_error.log',
        'data/sample_logs/lambda_permission_error.log',
        'data/sample_logs/databricks_sql_error.log'
    ]
    
    for log_file in log_files:
        if not os.path.exists(log_file):
            print(f"❌ Missing: {log_file}")
            continue
            
        print(f"\n📝 Processing: {log_file}")
        print("-" * 30)
        
        # Process log
        error_info = processor.process_log_file(log_file)
        
        print(f"Service: {error_info.get('service')}")
        print(f"Pipeline: {error_info.get('pipeline_name')}")
        print(f"Error Type: {error_info.get('error_type')}")
        
        # Analyze with LLM
        print("\n🤖 AI Analysis:")
        analysis = analyzer.analyze_failure(error_info)
        
        if 'error' in analysis:
            print(f"❌ Analysis Error: {analysis['error']}")
        else:
            print(f"Root Cause: {analysis.get('root_cause', 'Unknown')}")
            print(f"Severity: {analysis.get('severity', 'Unknown')}")
            print(f"Fix Time: {analysis.get('estimated_fix_time', 'Unknown')}")
            print("Suggested Fixes:")
            for i, fix in enumerate(analysis.get('suggested_fixes', []), 1):
                print(f"  {i}. {fix}")

if __name__ == "__main__":
    main()