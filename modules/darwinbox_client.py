import streamlit as st
import requests
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class DarwinboxClient:
    def __init__(self):
        # Common credentials
        self.subdomain = st.secrets["DARWINBOX_SUBDOMAIN"]
        self.base_url = f"https://{self.subdomain}.darwinbox.in"

        # --- Specific credentials for GET JOBS ---
        self.username_get_jobs = st.secrets["DARWINBOX_USERNAME_GET_JOBS"]
        self.password_get_jobs = st.secrets["DARWINBOX_PASSWORD_GET_JOBS"]
        self.api_key_get_jobs = st.secrets["DARWINBOX_API_KEY_GET_JOBS"]

        # --- Specific credentials for GET CANDIDATES ---
        self.username_get_candidates = st.secrets["DARWINBOX_USERNAME_GET_CANDIDATES"]
        self.password_get_candidates = st.secrets["DARWINBOX_PASSWORD_GET_CANDIDATES"]
        self.api_key_get_candidates = st.secrets["DARWINBOX_API_KEY_GET_CANDIDATES"]
        
        # --- Specific credentials for UPDATE actions (Shortlist/Reject) ---
        self.username_update_actions = st.secrets["DARWINBOX_USERNAME_UPDATE_SCORE"]
        self.password_update_actions = st.secrets["DARWINBOX_PASSWORD_UPDATE_SCORE"]
        
        # Load BOTH new API keys from secrets.toml
        self.api_key_shortlist = st.secrets["DARWINBOX_API_KEY_SHORTLIST"]
        self.api_key_reject = st.secrets["DARWINBOX_API_KEY_REJECT"]

    def get_jobs(self):
        url = f"{self.base_url}/JobsApiv3/Joblist"
        payload = {"api_key": self.api_key_get_jobs}
        
        try:
            response = requests.post(url, auth=(self.username_get_jobs, self.password_get_jobs), json=payload, timeout=20)
            response.raise_for_status()
            data = response.json()

            if data.get("status") == 1:
                return data.get("data", [])
            else:
                st.error(f"Darwinbox API Error (get_jobs): {data.get('message')}")
                return []
        except requests.exceptions.RequestException as e:
            st.error(f"Connection Error (get_jobs): Could not connect. Details: {e}")
            return []

    def get_candidates_for_job(self, job_id):
        url = f"{self.base_url}/JobsApiv3/BulkCandidatesData"
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180) 
        date_format = "%d-%m-%Y %H:%M:%S"
        payload = {
            "api_key": self.api_key_get_candidates,
            "job_id": job_id,
            "created_from": start_date.strftime(date_format),
            "created_to": end_date.strftime(date_format)
        }
        try:
            response = requests.post(url, auth=(self.username_get_candidates, self.password_get_candidates), json=payload, timeout=60)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == 1:
                candidates_raw = data.get("data", [])
                processed_candidates = [] # Create a new list for valid candidates

                for cand in candidates_raw:
                    if not isinstance(cand, dict):
                        continue

                    cand['name'] = f"{cand.get('firstname', '')} {cand.get('lastname', '')}".strip()
                    resume_url = ""
                    app_data = cand.get('application_data', {})
                    if 'Resume' in app_data and isinstance(app_data['Resume'], dict):
                        resume_url = app_data['Resume'].get('Resume', '')
                    cand['darwinbox_resume_url'] = resume_url
                    
                    processed_candidates.append(cand)
                
                return processed_candidates
            else:
                st.error(f"Darwinbox API Error (get_candidates_for_job): {data.get('message')}")
                return []
        # --- FIX: ADDED THE MISSING EXCEPT BLOCK ---
        except requests.exceptions.RequestException as e:
            st.error(f"Connection Error (get_candidates_for_job): Could not connect. Details: {e}")
            return []

    def shortlist_candidate(self, candidate_id: str, job_id: str):
        url = f"{self.base_url}/JobsApiv3/candidatetag"
        payload = {
            "api_key": self.api_key_shortlist, # Use the specific key for shortlisting
            "job_id": job_id,
            "candidate_id": candidate_id,
            "status_tag": "Shortlisted",
            "remarks": "Shortlisted via AI Screening Tool"
        }
        try:
            response = requests.post(url, auth=(self.username_update_actions, self.password_update_actions), json=payload, timeout=20)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == 1:
                return True, "Successfully shortlisted."
            else:
                return False, data.get('message', 'API returned an error for shortlisting.')
        except requests.exceptions.RequestException as e:
            return False, f"Network Error: {str(e)}"

    def reject_candidate(self, candidate_id: str, job_id: str, reason_tag: str):
        url = f"{self.base_url}/JobsApiv3/RejectCandidate"
        payload = {
            "api_key": self.api_key_reject, # Use the specific key for rejecting
            "job_id": job_id,
            "candidate_id": candidate_id,
            "rejection_reason": reason_tag
        }
        try:
            response = requests.post(url, auth=(self.username_update_actions, self.password_update_actions), json=payload, timeout=20)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == 1:
                return True, "Successfully rejected."
            else:
                return False, data.get('message', 'Failed to reject from API.')
        except requests.exceptions.RequestException as e:
            return False, f"Network Error: {str(e)}"
