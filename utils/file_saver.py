import os
import json
import pandas as pd
from datetime import datetime

# Define the base directory for all output
BASE_OUTPUT_DIR = "run_archive"

def get_timestamp_str():
    """Generates a string from the current date and time."""
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def save_data(data, folder_name: str, file_prefix: str, file_type: str = 'json'):
    """
    Saves data (list of dicts or DataFrame) to a specified folder with a timestamp.
    
    Args:
        data: The data to save (list, dictionary, or pandas DataFrame).
        folder_name: The subfolder within BASE_OUTPUT_DIR (e.g., 'job_list').
        file_prefix: A prefix for the filename (e.g., 'all_jobs').
        file_type: 'json' or 'csv'.
    """
    try:
        # Create the base and sub-directories if they don't exist
        folder_path = os.path.join(BASE_OUTPUT_DIR, folder_name)
        os.makedirs(folder_path, exist_ok=True)

        timestamp = get_timestamp_str()
        filename = f"{file_prefix}_{timestamp}.{file_type}"
        full_path = os.path.join(folder_path, filename)

        if file_type == 'json':
            # If it's a DataFrame, convert it to a list of dicts first
            if isinstance(data, pd.DataFrame):
                data_to_save = data.to_dict(orient='records')
            else:
                data_to_save = data
            
            with open(full_path, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, indent=4)

        elif file_type == 'csv':
            if isinstance(data, pd.DataFrame):
                data.to_csv(full_path, index=False)
            else: # Convert list of dicts to DataFrame
                pd.DataFrame(data).to_csv(full_path, index=False)

        print(f"Successfully saved data to {full_path}")
        return full_path

    except Exception as e:
        print(f"Error saving data to {folder_name}: {e}")
        return None

def create_resume_folder(job_code: str):
    """Creates a dedicated folder for resumes of a specific job."""
    folder_path = os.path.join(BASE_OUTPUT_DIR, "Candidates_resumes", job_code)
    os.makedirs(folder_path, exist_ok=True)
    return folder_path