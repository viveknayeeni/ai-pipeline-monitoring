# src/llm_analyzer.py
import json
import os
from typing import Dict
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config/.env')

class LLMAnalyzer:
    def __init__(self):
        self.client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        self.model = "gpt-4o-mini"  # Cost-effective for development
    
    def analyze_failure(self, error_info: Dict) -> Dict:
        """Analyze pipeline failure using OpenAI"""
        
        if not error_info:
            return {"error": "No error information provided"}
        
        prompt = self._build_analysis_prompt(error_info)
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a senior data engineer expert in diagnosing pipeline failures. Always respond with valid JSON only."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1000,
                temperature=0.1
            )
            
            result = response.choices[0].message.content
            return self._parse_analysis_response(result)
            
        except Exception as e:
            return {"error": f"LLM analysis failed: {str(e)}"}
    
    def _build_analysis_prompt(self, error_info: Dict) -> str:
        return f"""
        Analyze this data pipeline failure and provide actionable insights:

        **Service:** {error_info.get('service', 'unknown')}
        **Pipeline:** {error_info.get('pipeline_name', 'unknown')}
        **Step:** {error_info.get('step_name', 'unknown')}
        **Error Type:** {error_info.get('error_type', 'unknown')}
        **Timestamp:** {error_info.get('timestamp', 'unknown')}

        **Error Message:**
        {error_info.get('error_message', 'No error message')}

        **Stack Trace:**
        {error_info.get('stack_trace', 'No stack trace')}

        Provide a JSON response with exactly these fields:
        {{
            "root_cause": "Brief explanation of what caused the failure",
            "severity": "HIGH|MEDIUM|LOW based on business impact",
            "suggested_fixes": ["specific action 1", "specific action 2", "specific action 3"],
            "prevention_tips": ["prevention tip 1", "prevention tip 2"],
            "estimated_fix_time": "time estimate like '30 minutes' or '2 hours'",
            "dependencies": ["system1", "system2"],
            "confidence": "HIGH|MEDIUM|LOW - how confident you are in this analysis"
        }}

        Respond with valid JSON only, no markdown or explanations.
        """
    
    def _parse_analysis_response(self, response: str) -> Dict:
        """Parse and validate the LLM response"""
        try:
            # Clean up the response (remove markdown if present)
            cleaned = response.strip()
            if cleaned.startswith('```json'):
                cleaned = cleaned[7:]
            if cleaned.endswith('```'):
                cleaned = cleaned[:-3]
            
            parsed = json.loads(cleaned)
            
            # Validate required fields
            required_fields = ['root_cause', 'severity', 'suggested_fixes', 'prevention_tips', 'estimated_fix_time']
            for field in required_fields:
                if field not in parsed:
                    parsed[field] = f"Missing {field}"
            
            return parsed
            
        except json.JSONDecodeError as e:
            return {
                "error": f"Failed to parse LLM response: {str(e)}",
                "raw_response": response
            }

# Test the analyzer
if __name__ == "__main__":
    # Test with a sample error
    sample_error = {
        'service': 'airflow',
        'pipeline_name': 'data_ingestion_dag',
        'step_name': 'extract_user_data',
        'error_type': 'connection_timeout',
        'timestamp': '2025-01-15 14:23:45',
        'error_message': 'psycopg2.OperationalError: connection to server at "prod-db.company.com" (10.0.1.45), port 5432 failed: Connection timed out',
        'stack_trace': 'File "/opt/airflow/dags/extract_user_data.py", line 45, in execute\n    conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_pass)'
    }
    
    analyzer = LLMAnalyzer()
    result = analyzer.analyze_failure(sample_error)
    print(json.dumps(result, indent=2))