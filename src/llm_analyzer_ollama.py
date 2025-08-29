# src/llm_analyzer_ollama.py
import json
import requests
from typing import Dict

class OllamaAnalyzer:
    def __init__(self, model="llama3.1:8b", base_url="http://localhost:11434"):
        self.model = model
        self.base_url = base_url
        self.api_url = f"{base_url}/api/generate"
    
    def analyze_failure(self, error_info: Dict) -> Dict:
        """Analyze pipeline failure using Ollama"""
        
        if not error_info:
            return {"error": "No error information provided"}
        
        prompt = self._build_analysis_prompt(error_info)
        
        try:
            response = requests.post(
                self.api_url,
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.1,
                        "top_p": 0.9,
                        "max_tokens": 1000
                    }
                },
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()['response']
                return self._parse_analysis_response(result)
            else:
                return {"error": f"Ollama API error: {response.status_code}"}
                
        except requests.exceptions.RequestException as e:
            return {"error": f"Connection to Ollama failed: {str(e)}"}
        except Exception as e:
            return {"error": f"LLM analysis failed: {str(e)}"}
    
    def _build_analysis_prompt(self, error_info: Dict) -> str:
        return f"""You are a senior data engineer analyzing pipeline failures. Analyze this error and respond with ONLY a valid JSON object.

ERROR DETAILS:
- Service: {error_info.get('service', 'unknown')}
- Pipeline: {error_info.get('pipeline_name', 'unknown')}
- Step: {error_info.get('step_name', 'unknown')}
- Error Type: {error_info.get('error_type', 'unknown')}
- Timestamp: {error_info.get('timestamp', 'unknown')}

ERROR MESSAGE:
{error_info.get('error_message', 'No error message')}

STACK TRACE:
{error_info.get('stack_trace', 'No stack trace')}

Respond with ONLY this JSON format:
{{
    "root_cause": "Brief explanation of what caused the failure",
    "severity": "HIGH|MEDIUM|LOW",
    "suggested_fixes": ["action 1", "action 2", "action 3"],
    "prevention_tips": ["tip 1", "tip 2"],
    "estimated_fix_time": "30 minutes|2 hours|etc",
    "dependencies": ["system1", "system2"],
    "confidence": "HIGH|MEDIUM|LOW"
}}

JSON only, no explanations:"""
    
    def _parse_analysis_response(self, response: str) -> Dict:
        """Parse and validate the LLM response"""
        try:
            # Clean up the response
            cleaned = response.strip()
            
            # Remove any markdown formatting
            if '```json' in cleaned:
                start = cleaned.find('```json') + 7
                end = cleaned.find('```', start)
                cleaned = cleaned[start:end] if end != -1 else cleaned[start:]
            elif '```' in cleaned:
                start = cleaned.find('```') + 3
                end = cleaned.find('```', start)
                cleaned = cleaned[start:end] if end != -1 else cleaned[start:]
            
            # Find JSON object
            start_brace = cleaned.find('{')
            end_brace = cleaned.rfind('}')
            
            if start_brace != -1 and end_brace != -1:
                json_str = cleaned[start_brace:end_brace + 1]
                parsed = json.loads(json_str)
                
                # Validate required fields
                required_fields = ['root_cause', 'severity', 'suggested_fixes']
                for field in required_fields:
                    if field not in parsed:
                        parsed[field] = f"Missing {field}"
                
                return parsed
            else:
                raise ValueError("No JSON object found in response")
                
        except (json.JSONDecodeError, ValueError) as e:
            return {
                "error": f"Failed to parse response: {str(e)}",
                "raw_response": response[:500],  # First 500 chars for debugging
                "root_cause": "Unable to analyze - parsing error",
                "severity": "MEDIUM",
                "suggested_fixes": ["Check logs manually", "Retry the pipeline"],
                "prevention_tips": ["Monitor system health"],
                "estimated_fix_time": "Unknown",
                "confidence": "LOW"
            }

    def test_connection(self) -> bool:
        """Test if Ollama is running and model is available"""
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=5)
            if response.status_code == 200:
                models = [model['name'] for model in response.json()['models']]
                return self.model in models
            return False
        except:
            return False

# Test script
if __name__ == "__main__":
    analyzer = OllamaAnalyzer()
    
    # Test connection
    if not analyzer.test_connection():
        print("❌ Ollama not running or model not found")
        print("Run: ollama serve")
        print("Then: ollama pull llama3.1:8b")
        exit(1)
    
    print("✅ Ollama connection successful")
    
    # Test analysis
    sample_error = {
        'service': 'airflow',
        'pipeline_name': 'user_data_pipeline',
        'step_name': 'extract_users',
        'error_type': 'connection_timeout',
        'timestamp': '2025-01-15 14:23:45',
        'error_message': 'psycopg2.OperationalError: connection timeout',
        'stack_trace': 'File "extract_users.py", line 45, in execute'
    }
    
    result = analyzer.analyze_failure(sample_error)
    print(json.dumps(result, indent=2))