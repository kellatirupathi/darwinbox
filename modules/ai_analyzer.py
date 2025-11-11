import streamlit as st
import requests
import json
import logging
import time
from random import uniform

logger = logging.getLogger(__name__)

class AIAnalyzer:
    def __init__(self):
        # REMOVED: Key pool, lock, and cooldown logic.
        # Now it just holds a simple list of keys like in Application 2.
        self.api_keys_list = [key for key in [
            st.secrets.get("MISTRAL_API_KEY_1"),
            st.secrets.get("MISTRAL_API_KEY_2"),
            st.secrets.get("MISTRAL_API_KEY_3")
        ] if key]
        if not self.api_keys_list:
            raise ValueError("No Mistral API keys found in secrets.toml")
        
        self.endpoint = "https://api.mistral.ai/v1/chat/completions"
        self.model = "mistral-medium-latest"

    # REMOVED: The get_available_key() and set_key_cooldown() methods.

    # MODIFIED: This function now accepts an `api_key` directly and contains the retry logic.
    def analyze_resume(self, resume_text, job_description, api_key):
        """
        Analyzes a resume using a specific API key with retry logic inspired by Application 2.
        """
        
        prompt = f"""
        You are a world-class, meticulous HR recruitment analyst. Your task is to perform a detailed, critical analysis of the provided RESUME against the JOB DESCRIPTION.

        Follow these steps in your reasoning:
        1. **Deconstruct the Job Description:** Identify the 3-5 most critical mandatory requirements (e.g., years of experience, specific technologies like 'React' or 'SQL', required licenses, educational degrees).
        2. **Scan the Resume:** Systematically search the resume for explicit evidence matching each of these critical requirements. Do not make assumptions.
        3. **Evaluate the Match:** For each requirement, determine if it is a strong match, a partial match, or a clear gap.
        4. **Calculate a Score:** Assign a quantitative score that reflects this evaluation. A perfect match on all critical requirements is 100. **Significantly penalize the score (e.g., below 50) if a mandatory requirement is clearly missing.**
        5. **Generate Remarks:** Formulate a concise summary based on your findings, broken down into specific strengths and weaknesses.

        You MUST return your response as a single, valid JSON object and nothing else. Do not include any introductory text like "Here is the JSON object:".

        <JSON_STRUCTURE>
        {{
            "overall_score": <integer from 0 to 100>,
            "key_strengths": [
                "<A specific skill or experience that is a strong match. (e.g., '5+ years of experience in Python matches requirement')>",
                "<Another strong match>"
            ],
            "key_weaknesses": [
                "<A specific skill or requirement that is missing or weak. (e.g., 'Lacks the required PMP certification')>",
                "<Another gap>"
            ],
            "summary": "<A 1-2 sentence conclusion that justifies the score based on the strengths and weaknesses.>"
        }}
        </JSON_STRUCTURE>

        <JOB_DESCRIPTION>
        {job_description}
        </JOB_DESCRIPTION>

        <RESUME_TEXT>
        {resume_text}
        </RESUME_TEXT>
        """
        
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "response_format": {"type": "json_object"},
            "temperature": 0.1,
        }
        
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        # NEW: Retry logic copied from Application 2. It will retry with the SAME key.
        attempts = 3
        initial_backoff = 2.0
        for attempt in range(attempts):
            try:
                response = requests.post(self.endpoint, headers=headers, json=payload, timeout=120)

                if response.status_code == 200:
                    data = response.json()
                    content = data['choices'][0]['message']['content']
                    return json.loads(content)
                
                elif response.status_code == 429: # Rate limit error
                    wait_s = (initial_backoff * (2 ** attempt)) + uniform(0, 1)
                    logger.warning(f"Rate limit hit on key ...{api_key[-4:]}. Attempt {attempt + 1}/{attempts}. Retrying in {wait_s:.2f}s...")
                    time.sleep(wait_s)
                    continue # Try again with the SAME key

                else:
                    logger.error(f"Mistral API Client Error ({response.status_code}): {response.text}")
                    return {"overall_score": 0, "key_strengths": [], "key_weaknesses": [], "summary": f"API Client Error: {response.status_code} - {response.text}"}

            except requests.exceptions.RequestException as e:
                wait_s = (initial_backoff * (2 ** attempt)) + uniform(0, 1)
                logger.warning(f"Request exception on key ...{api_key[-4:]}: {e}. Retrying in {wait_s:.2f}s.")
                time.sleep(wait_s)
                continue

        return {"overall_score": 0, "key_strengths": [], "key_weaknesses": [], "summary": "AI analysis failed after multiple retries."}