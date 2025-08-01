# # RMS/utils/config_loader.py
# import yaml
# import os
# import logging

# # --- load_app_config and setup_logging remain the same ---
# def load_app_config(config_path="settings.yaml"):
#     """Loads application configuration from a YAML file."""
#     if not os.path.exists(config_path):
#         logging.error(f"Configuration file not found: {config_path}")
#         raise FileNotFoundError(f"Configuration file not found: {config_path}")
#     try:
#         with open(config_path, 'r') as f:
#             config = yaml.safe_load(f)
#         logging.info(f"Configuration loaded successfully from {config_path}")
#         return config
#     except Exception as e:
#         logging.error(f"Error loading configuration from {config_path}: {e}")
#         raise

# def setup_logging(config):
#     # ... (existing setup_logging code) ...
#     log_config = config.get('logging', {})
#     level = getattr(logging, log_config.get('level', 'INFO').upper(), logging.INFO)
#     log_format = log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s')
#     log_file = log_config.get('file_name', 'rms_app.log')
#     if not os.path.isabs(log_file):
#         project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#         log_file_path = os.path.join(project_root, log_file)
#     else: log_file_path = log_file
#     logging.basicConfig(level=level, format=log_format, handlers=[logging.FileHandler(log_file_path), logging.StreamHandler()])
#     logging.info(f"Logging configured. Level: {logging.getLevelName(level)}. Log file: {log_file_path}")


# # --- NEW FUNCTION ---
# def save_app_config(config_data, config_path="settings.yaml"):
#     """Saves the configuration dictionary to a YAML file."""
#     try:
#         with open(config_path, 'w') as f:
#             # Use yaml.dump for better formatting (e.g., indentation)
#             yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)
#         logging.info(f"Configuration saved successfully to {config_path}")
#         return True
#     except Exception as e:
#         logging.error(f"Error saving configuration to {config_path}: {e}", exc_info=True)
#         return False

# # --- Existing config loading at module import ---
# try:
#     APP_CONFIG = load_app_config()
#     setup_logging(APP_CONFIG)
# except Exception as e:
#     logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
#     logging.error(f"Failed to initialize application configuration or logging: {e}. Using fallback logging.")
#     APP_CONFIG = {}

# RMS/utils/config_loader.py
import yaml
import os
import logging
import streamlit as st
from .gspread_client import get_gspread_client, get_settings_from_gsheet , get_parameters_from_gsheet


def _load_static_config(config_path="settings.yaml"):
    """Loads only the static configuration from the YAML file."""
    if not os.path.exists(config_path):
        logging.error(f"Configuration file not found: {config_path}")
        return {"error": f"Configuration file not found: {config_path}"}
    try:
        with open(config_path, 'r') as f:
            static_config = yaml.safe_load(f)
        logging.info(f"Static configuration loaded from {config_path}")
        return static_config
    except Exception as e:
        logging.error(f"Error loading configuration from {config_path}: {e}")
        return {"error": f"Error loading configuration from {config_path}: {e}"}

# --- Helper function to load the merged config, cached for the session ---
@st.cache_data(show_spinner=False)
def load_and_merge_configs():
    """
    Loads configuration from YAML and merges dynamic settings from Google Sheets.
    Returns the merged config dict, or a dict with an 'error' key on failure.
    """
    static_config = _load_static_config()
    if "error" in static_config:
        return static_config # Pass the error up

    # Get GSheet details from the static config
    gsheet_settings = static_config.get("google_sheet_settings", {})
    spreadsheet_id = gsheet_settings.get("spreadsheet_id")
    # worksheet_name = gsheet_settings.get("worksheet_name")

    if not spreadsheet_id:
        error_msg = "google_sheet_settings (spreadsheet_id) not found in settings.yaml"
        logging.error(error_msg)
        return {"error": error_msg}
    
    gsheet_client = get_gspread_client()
    if isinstance(gsheet_client, str): # Check if get_gspread_client returned an error string
        return {"error": gsheet_client}
    
    # 1. Fetch Table IDs (as before)
    table_id_worksheet = gsheet_settings.get("worksheet_name", "Config")
    dynamic_settings = get_settings_from_gsheet(gsheet_client, spreadsheet_id, table_id_worksheet)
    if "error" in dynamic_settings:
        return dynamic_settings
    
    # --- NEW: Fetch Replenishment Parameters ---
    params_worksheet = "Parameters" # As per your new tab name
    replenishment_params = get_parameters_from_gsheet(gsheet_client, spreadsheet_id, params_worksheet)
    if "error" in replenishment_params:
        return replenishment_params # Pass the error up
    # --- END NEW ---
        
    # 3. Merge settings
    if dynamic_settings:
        if 'baserow' not in static_config: static_config['baserow'] = {}
        for key, value in dynamic_settings.items():
            static_config['baserow'][key] = value
        logging.info("Successfully merged dynamic table IDs from Google Sheets.")

    # --- NEW: Add parameters to the config ---
    static_config['replenishment_parameters'] = replenishment_params
    logging.info("Successfully merged replenishment parameters from Google Sheets.")
    # --- END NEW ---

    return static_config

def setup_logging(config):
    """
    Configures logging based on the loaded configuration.
    This version is more robust and will override any pre-existing logging configurations.
    """
    log_config = config.get('logging', {})
    if not log_config:
        print("WARNING: 'logging' section not found in config. Using basic logging.")
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        return

    level_str = log_config.get('level', 'INFO').upper()
    level = getattr(logging, level_str, logging.INFO)
    log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_file = log_config.get('file_name', 'rms_app.log')
    
    if not os.path.isabs(log_file):
        # Correctly find the project root from this file's location
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_file_path = os.path.join(project_root, log_file)
    else: 
        log_file_path = log_file

    # --- THIS IS THE CRUCIAL FIX ---
    # Get the root logger
    root_logger = logging.getLogger()
    
    # Set the level on the root logger
    root_logger.setLevel(level)
    
    # Remove any existing handlers to prevent duplicate logs or conflicts
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    # --- END FIX ---

    # Create your own handlers
    # File handler to write to rms_app.log
    try:
        file_handler = logging.FileHandler(log_file_path, mode='a') # 'a' for append
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter(log_format))
        root_logger.addHandler(file_handler)
    except Exception as e:
        # If file handler fails, print an error to the console
        print(f"ERROR: Could not create log file handler for {log_file_path}. Error: {e}")

    # Stream handler to print to the terminal
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(logging.Formatter(log_format))
    root_logger.addHandler(stream_handler)
    
    logging.info(f"Logging configured. Level: {level_str}. Log file: {log_file_path}")
    
def save_app_config(config_data, config_path="settings.yaml"):
    """Saves the static configuration dictionary to a YAML file."""
    try:
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)
        logging.info(f"Configuration saved successfully to {config_path}")
        return True
    except Exception as e:
        logging.error(f"Error saving configuration to {config_path}: {e}", exc_info=True)
        return False

# --- Main config loading logic ---
# This block runs when the module is first imported.
APP_CONFIG = load_and_merge_configs()
if "error" not in APP_CONFIG:
    setup_logging(APP_CONFIG)
else:
    # Fallback basic logging if config fails to load
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.warning(f"Using fallback logging due to config error: {APP_CONFIG.get('error')}")