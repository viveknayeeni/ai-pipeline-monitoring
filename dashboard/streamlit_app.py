# dashboard/streamlit_app.py
import streamlit as st
import sys
import os
import json
from datetime import datetime

# Add src to path
sys.path.append('./src')

from log_processor import LogProcessor
# from llm_analyzer import LLMAnalyzer
from llm_analyzer_ollama import OllamaAnalyzer

st.set_page_config(
    page_title="AI Pipeline Monitor",
    page_icon="🤖",
    layout="wide"
)

def main():
    st.title("🤖 AI-Powered Pipeline Monitoring")
    st.markdown("Local development version with sample data")
    
    # Initialize components
    if 'processor' not in st.session_state:
        st.session_state.processor = LogProcessor()
        # st.session_state.analyzer = LLMAnalyzer()
        st.session_state.analyzer = OllamaAnalyzer()
    
    # Sidebar for log selection
    st.sidebar.header("Log Selection")
    
    sample_logs = {
        "Airflow Connection Timeout": "./data/sample_logs/airflow_failure.log",
        "EMR Memory Error": "./data/sample_logs/emr_memory_error.log",
        "Lambda Permission Error": "./data/sample_logs/lambda_permission_error.log",
        "Databricks SQL Error": "./data/sample_logs/databricks_sql_error.log"
    }
    
    selected_log = st.sidebar.selectbox("Choose a sample log:", list(sample_logs.keys()))
    
    if st.sidebar.button("Analyze Log"):
        log_path = sample_logs[selected_log]
        
        with st.spinner("Processing log..."):
            # Process log
            error_info = st.session_state.processor.process_log_file(log_path)
            
        with st.spinner("Analyzing with AI..."):
            # Analyze with LLM
            analysis = st.session_state.analyzer.analyze_failure(error_info)
        
        # Display results
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📋 Error Information")
            st.json({
                "Service": error_info.get('service'),
                "Pipeline": error_info.get('pipeline_name'),
                "Step": error_info.get('step_name'),
                "Error Type": error_info.get('error_type'),
                "Timestamp": error_info.get('timestamp')
            })
            
            st.subheader("📝 Error Message")
            st.code(error_info.get('error_message', 'No error message'), language='text')
        
        with col2:
            st.subheader("🤖 AI Analysis")
            
            if 'error' in analysis:
                st.error(f"Analysis failed: {analysis['error']}")
            else:
                # Severity indicator
                severity = analysis.get('severity', 'UNKNOWN')
                severity_color = {'HIGH': '🔴', 'MEDIUM': '🟡', 'LOW': '🟢'}.get(severity, '⚪')
                st.markdown(f"**Severity:** {severity_color} {severity}")
                
                st.markdown(f"**Root Cause:** {analysis.get('root_cause', 'Unknown')}")
                st.markdown(f"**Estimated Fix Time:** {analysis.get('estimated_fix_time', 'Unknown')}")
                
                st.markdown("**Suggested Fixes:**")
                for i, fix in enumerate(analysis.get('suggested_fixes', []), 1):
                    st.markdown(f"{i}. {fix}")
                
                st.markdown("**Prevention Tips:**")
                for i, tip in enumerate(analysis.get('prevention_tips', []), 1):
                    st.markdown(f"{i}. {tip}")
    
    # Sample metrics (mock data)
    st.subheader("📊 Sample Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("MTTR", "2.3 hours", "-1.2 hours")
    with col2:
        st.metric("Auto-Resolved", "45%", "+15%")
    with col3:
        st.metric("Active Incidents", "3", "-2")
    with col4:
        st.metric("AI Accuracy", "87%", "+5%")

if __name__ == "__main__":
    main()