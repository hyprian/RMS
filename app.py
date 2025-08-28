# RMS/app.py
import streamlit as st
import os
import sys

# Ensure project root is in sys.path for utils and data_processing imports
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


# Import necessary modules
from utils.config_loader import APP_CONFIG
from notifications.notification_engine import run_all_checks
import logging

logger = logging.getLogger(__name__)

# --- NEW: WEBHOOK HANDLING LOGIC ---
def handle_notification_webhook():
    """
    Checks for a webhook trigger in the URL's query parameters.
    If the trigger and a valid secret key are present, it runs the notification engine.
    """
    # Safely get the expected key from Streamlit Secrets
    try:
        expected_key = st.secrets.webhooks.notification_trigger_key
    except (AttributeError, KeyError):
        # This will happen if the secrets are not configured.
        # We log it but don't stop the app, as a normal user might be visiting.
        logger.warning("WEBHOOK: `webhooks.notification_trigger_key` not found in Streamlit Secrets.")
        expected_key = None

    # Get the trigger parameter from the URL (e.g., ?trigger_notifications=some_key)
    query_params = st.query_params
    trigger_key = query_params.get("trigger_notifications")

    # Proceed only if a trigger key was provided in the URL
    if trigger_key:
        # Check if the provided key matches the expected secret key
        if expected_key and trigger_key == expected_key:
            logger.info("WEBHOOK: Notification trigger received with a valid key.")
            
            # Provide immediate feedback that the webhook was received.
            # This is what n8n will see as the response body.
            st.info("Webhook trigger received. Running notification checks in the background...")
            
            try:
                # Run the main notification function
                run_all_checks()
                success_message = "Notification checks completed successfully."
                logger.info(f"WEBHOOK: {success_message}")
                st.success(success_message)
            except Exception as e:
                error_message = f"Webhook notification run failed: {e}"
                logger.error(error_message, exc_info=True)
                st.error(error_message)
            
            # Stop the script here. This is crucial to prevent the full UI
            # from loading, making the webhook response fast and efficient.
            st.stop()
        else:
            # A key was provided, but it was incorrect.
            logger.warning(f"WEBHOOK: Invalid trigger key received: '{trigger_key}'")
            st.error("Invalid trigger key provided.")
            st.stop()

# --- Run the webhook handler at the very top of the script ---
# This ensures it executes before any other Streamlit command.
handle_notification_webhook()
# --- END NEW WEBHOOK LOGIC ---


# Initialize logging and load config
# This should be one of the first things to run
try:
    from utils.config_loader import APP_CONFIG, setup_logging # APP_CONFIG is loaded here
    if not APP_CONFIG: # If APP_CONFIG is empty due to load failure
        st.error("Failed to load application configuration (settings.yaml). Please check the logs and the file.")
        st.stop()
    # setup_logging(APP_CONFIG) # Logging is now set up within config_loader.py
except ImportError as e:
    st.error(f"Critical Import Error: {e}. Make sure all modules are correctly placed.")
    st.stop()
except FileNotFoundError as e:
    st.error(f"Configuration file not found: {e}. Please ensure 'settings.yaml' exists in the project root.")
    st.stop()
except Exception as e:
    st.error(f"An unexpected error occurred during initialization: {e}")
    st.stop()

st.set_page_config(
    page_title="RMS - Replenishment Management",
    layout="wide"
)

st.sidebar.success("Select a page above.")

st.title("Welcome to the Replenishment Management System (RMS)")

st.markdown("""
This system helps you manage your inventory replenishment based on sales data from various platforms.

**Please navigate using the sidebar:**
- **Upload Sales Data**: To upload your 30-day and 60-day sales reports.
- **Generate Replenishment Table**: To process the data and view the replenishment recommendations.

Make sure your `settings.yaml` file is correctly configured with Baserow API tokens and table IDs.
""")

# You can add more global information or settings display here if needed
# For example, display loaded cache directory:
# cache_dir_info = APP_CONFIG.get('cache', {}).get('directory', 'Not configured')
# st.sidebar.info(f"Cache Directory: {cache_dir_info}")