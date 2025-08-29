# test_multiple_llms.py
#!/usr/bin/env python3
import sys
import os
sys.path.append('src')

from log_processor import LogProcessor
from llm_analyzer_ollama import OllamaAnalyzer
# from llm_analyzer_groq import GroqAnalyzer  # Uncomment if you have Groq API key

def main():
    print("🚀 Testing AI Pipeline Monitor with Open Source LLMs")
    print("=" * 60)
    
    # Initialize components
    processor = LogProcessor()
    
    # Test different LLM options
    analyzers = {
        "Ollama (Local)": OllamaAnalyzer(),
        # "Groq (API)": GroqAnalyzer(),  # Uncomment if you have API key
    }
    
    # Sample error
    sample_error = {
        'service': 'airflow',
        'pipeline_name': 'user_analytics',
        'step_name': 'data_extraction',
        'error_type': 'connection_timeout',
        'timestamp': '2025-01-15 14:23:45',
        'error_message': 'Connection timeout to database server',
        'stack_trace': 'psycopg2.OperationalError: timeout'
    }
    
    for analyzer_name, analyzer in analyzers.items():
        print(f"\n🤖 Testing {analyzer_name}")
        print("-" * 40)
        
        if hasattr(analyzer, 'test_connection') and not analyzer.test_connection():
            print(f"❌ {analyzer_name} not available")
            continue
        
        result = analyzer.analyze_failure(sample_error)
        
        if 'error' in result:
            print(f"❌ Error: {result['error']}")
        else:
            print(f"✅ Root Cause: {result.get('root_cause', 'Unknown')}")
            print(f"📊 Severity: {result.get('severity', 'Unknown')}")
            print(f"⏱️  Fix Time: {result.get('estimated_fix_time', 'Unknown')}")
            print("🔧 Suggested Fixes:")
            for i, fix in enumerate(result.get('suggested_fixes', []), 1):
                print(f"   {i}. {fix}")

if __name__ == "__main__":
    main()