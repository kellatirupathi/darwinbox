import streamlit as st
import gspread
import pandas as pd
from datetime import datetime
import logging
import json # <-- NEW IMPORT

logger = logging.getLogger(__name__)

try:
    SPREADSHEET_KEY = st.secrets["GOOGLE_SHEET_KEY"]
except KeyError:
    SPREADSHEET_KEY = None
    logger.error("GOOGLE_SHEET_KEY not found in secrets.toml. Google Sheets integration will be disabled.")


class GSheetsClient:
    def __init__(self):
        self.spreadsheet = None
        self.connected = False
        
        if not SPREADSHEET_KEY:
            return

        try:
            # --- FIX STARTS HERE ---
            # Get the credentials string from secrets
            creds_str = st.secrets["gcp_service_account"]
            # Convert the string into a Python dictionary
            creds_dict = json.loads(creds_str) 
            # --- FIX ENDS HERE ---

            self.gc = gspread.service_account_from_dict(creds_dict) # Use the dictionary
            self.spreadsheet = self.gc.open_by_key(SPREADSHEET_KEY)
            self.connected = True
            logger.info("Successfully connected to Google Sheets.")
        except Exception as e:
            st.error(f"Failed to connect to Google Sheets. Check API permissions and sharing settings. Error: {e}")
            logger.error(f"GSheets connection failed: {e}")
    
    def _prepare_data_for_sheets(self, df):
        """Converts all data types to string to prevent gspread errors."""
        # Create a copy to avoid modifying the original DataFrame in session_state
        df_copy = df.copy()
        for col in df_copy.columns:
            # Convert lists/dicts to JSON strings, otherwise convert to simple string
            if df_copy[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df_copy[col] = df_copy[col].apply(json.dumps)
            else:
                df_copy[col] = df_copy[col].astype(str)
        return df_copy

    def append_data_to_sheet(self, worksheet_name: str, data: list):
        if not self.connected or not data:
            if not self.connected:
                logger.warning("Google Sheets client not connected. Skipping data append.")
            return

        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            try:
                worksheet = self.spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="50")
                logger.info(f"Created new worksheet: '{worksheet_name}'")
            except Exception as e:
                logger.error(f"Could not create worksheet '{worksheet_name}': {e}")
                return
        
        try:
            df_to_append = pd.DataFrame(data)

            df_to_append.insert(0, "run_timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            df_prepared = self._prepare_data_for_sheets(df_to_append)
            
            existing_headers = worksheet.row_values(1)

            if not existing_headers:
                worksheet.update([df_prepared.columns.values.tolist()] + df_prepared.values.tolist(), value_input_option='USER_ENTERED')
                logger.info(f"Wrote initial data with headers to '{worksheet_name}'.")
            else:
                new_cols_to_add = [col for col in df_prepared.columns if col not in existing_headers]

                if new_cols_to_add:
                    start_col_index = len(existing_headers) + 1
                    range_to_update = f"{gspread.utils.rowcol_to_a1(1, start_col_index)}:{gspread.utils.rowcol_to_a1(1, start_col_index + len(new_cols_to_add) - 1)}"
                    worksheet.update(range_to_update, [new_cols_to_add])
                    logger.info(f"Added new columns to '{worksheet_name}': {new_cols_to_add}")
                    existing_headers.extend(new_cols_to_add)
                
                final_df = df_prepared.reindex(columns=existing_headers, fill_value="")

                worksheet.append_rows(final_df.values.tolist(), value_input_option='USER_ENTERED')
                logger.info(f"Appended {len(final_df)} rows to '{worksheet_name}'.")

        except Exception as e:
            st.warning(f"Could not write data to sheet '{worksheet_name}'. Error: {e}")
            logger.error(f"Failed to append data to '{worksheet_name}': {e}")