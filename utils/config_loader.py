# RMS/utils/config_loader.py
import yaml
import os
import logging

# --- load_app_config and setup_logging remain the same ---
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
    # ... (existing setup_logging code) ...
    log_config = config.get('logging', {})
    level = getattr(logging, log_config.get('level', 'INFO').upper(), logging.INFO)
    log_format = log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s')
    log_file = log_config.get('file_name', 'rms_app.log')
    if not os.path.isabs(log_file):
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_file_path = os.path.join(project_root, log_file)
    else: log_file_path = log_file
    logging.basicConfig(level=level, format=log_format, handlers=[logging.FileHandler(log_file_path), logging.StreamHandler()])
    logging.info(f"Logging configured. Level: {logging.getLevelName(level)}. Log file: {log_file_path}")


# --- NEW FUNCTION ---
def save_app_config(config_data, config_path="settings.yaml"):
    """Saves the configuration dictionary to a YAML file."""
    try:
        with open(config_path, 'w') as f:
            # Use yaml.dump for better formatting (e.g., indentation)
            yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)
        logging.info(f"Configuration saved successfully to {config_path}")
        return True
    except Exception as e:
        logging.error(f"Error saving configuration to {config_path}: {e}", exc_info=True)
        return False

# --- Existing config loading at module import ---
try:
    APP_CONFIG = load_app_config()
    setup_logging(APP_CONFIG)
except Exception as e:
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.error(f"Failed to initialize application configuration or logging: {e}. Using fallback logging.")
    APP_CONFIG = {}