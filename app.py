# RMS/app.py
import streamlit as st
import os
import sys

# Ensure project root is in sys.path for utils and data_processing imports
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

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