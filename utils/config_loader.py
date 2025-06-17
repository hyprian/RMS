# RMS/utils/config_loader.py
import yaml
import os
import logging

def load_app_config(config_path="settings.yaml"):
    """Loads application configuration from a YAML file."""
    if not os.path.exists(config_path):
        logging.error(f"Configuration file not found: {config_path}")
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logging.info(f"Configuration loaded successfully from {config_path}")
        return config
    except Exception as e:
        logging.error(f"Error loading configuration from {config_path}: {e}")
        raise

def setup_logging(config):
    """Configures logging based on the loaded configuration."""
    log_config = config.get('logging', {})
    level = getattr(logging, log_config.get('level', 'INFO').upper(), logging.INFO)
    log_format = log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s')
    log_file = log_config.get('file_name', 'rms_app.log') # Default log file name

    # Ensure log file path is relative to project root if not absolute
    if not os.path.isabs(log_file):
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # RMS directory
        log_file_path = os.path.join(project_root, log_file)
    else:
        log_file_path = log_file

    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler()
        ]
    )
    logging.info(f"Logging configured. Level: {logging.getLevelName(level)}. Log file: {log_file_path}")

# Load config and setup logging when module is imported,
# so other modules can just import logging and use it.
try:
    APP_CONFIG = load_app_config()
    setup_logging(APP_CONFIG)
except Exception as e:
    # Fallback basic logging if config loading fails
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.error(f"Failed to initialize application configuration or logging: {e}. Using fallback logging.")
    APP_CONFIG = {} # Provide an empty config to prevent further errors if APP_CONFIG is accessed