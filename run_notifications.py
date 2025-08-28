# RMS/run_notifications.py
import os
import sys
import logging

# Add the project root to the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from notifications.notification_engine import run_all_checks
from utils.config_loader import APP_CONFIG

# --- Setup Logging with UTF-8 Encoding ---
log_config = APP_CONFIG.get('logging', {})
log_file = log_config.get('file_name', 'rms_app.log')
log_level = getattr(logging, log_config.get('level', 'INFO').upper(), logging.INFO)
log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# --- THIS IS THE FIX ---
# Create handlers with explicit encoding
file_handler = logging.FileHandler(os.path.join(project_root, log_file), encoding='utf-8')
stream_handler = logging.StreamHandler() # Console

# Set formatters
formatter = logging.Formatter(log_format)
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# Get the root logger and add handlers
logger_root = logging.getLogger()
logger_root.setLevel(log_level)
# Clear existing handlers to avoid duplicate logs
if logger_root.hasHandlers():
    logger_root.handlers.clear()
logger_root.addHandler(file_handler)
logger_root.addHandler(stream_handler)
# --- END FIX ---

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.info("=============================================")
    logger.info("STARTING SCHEDULED RMS NOTIFICATION SCRIPT")
    logger.info("=============================================")
    
    if "error" in APP_CONFIG:
        logger.critical(f"Failed to load application configuration. Error: {APP_CONFIG['error']}")
        logger.critical("Aborting notification script.")
    else:
        run_all_checks()
        logger.info("---------------------------------------------")
        logger.info("FINISHED SCHEDULED RMS NOTIFICATION SCRIPT")
        logger.info("---------------------------------------------")