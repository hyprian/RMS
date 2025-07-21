# RMS/utils/gspread_client.py
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import logging

logger = logging.getLogger(__name__)

@st.cache_resource
def get_gspread_client():
    """Connects to Google Sheets API using Streamlit Secrets."""
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds_dict = st.secrets["google_credentials"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        logger.info("Successfully connected to Google Sheets API.")
        return client
    except Exception as e:
        # We will return the error instead of calling st.error() here
        logger.error(f"Failed to connect to Google Sheets API: {e}", exc_info=True)
        return f"Failed to connect to Google Sheets. Check secrets. Error: {e}"

def get_settings_from_gsheet(client, spreadsheet_id, worksheet_name):
    """Fetches settings from the Google Sheet using its ID and returns them as a dictionary."""
    if not client or not spreadsheet_id:
        return {}
    try:
        spreadsheet = client.open_by_key(spreadsheet_id) # Use open_by_key for ID
        worksheet = spreadsheet.worksheet(worksheet_name)
        records = worksheet.get_all_records()
        
        settings_dict = {}
        for row in records:
            key = row.get('Setting_Key')
            value = row.get('Setting_Value')
            if key:
                try:
                    settings_dict[key] = int(value)
                except (ValueError, TypeError):
                    settings_dict[key] = value
        
        logger.info(f"Successfully fetched {len(settings_dict)} settings from Google Sheet.")
        return settings_dict
    except Exception as e:
        # Return the error message to be displayed on the page after set_page_config
        error_message = f"Failed to fetch settings from Google Sheet (ID: {spreadsheet_id}). Check ID, sheet name, and sharing permissions. Error: {e}"
        logger.error(error_message, exc_info=True)
        return {"error": error_message} # Return a dict with an error key

def update_settings_in_gsheet(client, spreadsheet_id, worksheet_name, settings_to_update: dict):
    """Updates specific settings in the Google Sheet using its ID."""
    if not client:
        return False
    try:
        spreadsheet = client.open_by_key(spreadsheet_id) # Use open_by_key for ID
        worksheet = spreadsheet.worksheet(worksheet_name)
        
        df = pd.DataFrame(worksheet.get_all_records())
        if 'Setting_Key' not in df.columns:
            logger.error("'Setting_Key' column not found in the Google Sheet.")
            return False

        cells_to_update = []
        for key, value in settings_to_update.items():
            match = df.index[df['Setting_Key'] == key].tolist()
            if match:
                row_index = match[0] + 2
                cell = gspread.Cell(row_index, 2, str(value))
                cells_to_update.append(cell)
        
        if cells_to_update:
            worksheet.update_cells(cells_to_update)
            logger.info(f"Successfully updated {len(cells_to_update)} settings in Google Sheet.")
        
        return True
    except Exception as e:
        logger.error(f"Failed to update settings in Google Sheet (ID: {spreadsheet_id}): {e}", exc_info=True)
        st.error(f"Failed to update settings in Google Sheet (ID: {spreadsheet_id}).")
        return False