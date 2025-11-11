import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import logging

logger = logging.getLogger(__name__)

class GDriveClient:
    def __init__(self):
        try:
            self.creds = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"],
                scopes=['https://www.googleapis.com/auth/drive']
            )
            self.service = build('drive', 'v3', credentials=self.creds)
            self.folder_id = st.secrets["GDRIVE_FOLDER_ID"]
            logger.info("Google Drive Client initialized successfully.")
        except Exception as e:
            st.error(f"Failed to initialize Google Drive Client. Check your `secrets.toml` configuration. Error: {e}")
            self.service = None
            self.folder_id = None
            
    def upload_resume(self, local_file_path, remote_file_name):
        if not self.service:
            return None, "GDrive service not available."
            
        try:
            file_metadata = {
                'name': remote_file_name,
                'parents': [self.folder_id]
            }
            media = MediaFileUpload(local_file_path)
            
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink'
            ).execute()
            
            file_id = file.get('id')
            
            # Set permissions to "anyone with the link can edit"
            permission = {'type': 'anyone', 'role': 'writer'}
            self.service.permissions().create(fileId=file_id, body=permission).execute()

            # We need to fetch the link again AFTER setting permissions for it to be public
            public_file = self.service.files().get(fileId=file_id, fields='webViewLink').execute()

            logger.info(f"Successfully uploaded {remote_file_name} to Google Drive.")
            return public_file.get('webViewLink'), None
            
        except Exception as e:
            logger.error(f"Error uploading {remote_file_name} to Google Drive: {e}")
            return None, str(e)