# RMS/pages/1_Upload_Sales_Data.py
import streamlit as st
import os
import sys
from datetime import datetime

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from utils.file_utils import save_uploaded_file
import logging

logger = logging.getLogger(__name__)

st.set_page_config(page_title="Upload Sales Data - RMS", layout="wide")
st.title("📤 Upload Sales Data")

st.markdown("""
Upload your 30-day and 60-day sales reports for each platform and account.
Supported formats: CSV, XLSX.
""")

platforms_config = APP_CONFIG.get('platforms', [])
if not platforms_config:
    st.error("Platform configuration is missing in `settings.yaml`. Please define platforms and accounts.")
    st.stop()

if 'uploaded_file_paths' not in st.session_state:
    st.session_state.uploaded_file_paths = {}

for platform in platforms_config:
    platform_name = platform.get('name', 'Unknown Platform')
    platform_slug = platform.get('slug')
    accounts = platform.get('accounts', [])

    if not platform_slug:
        st.warning(f"Platform '{platform_name}' is missing a 'slug' in config. Skipping.")
        continue

    st.subheader(f"Platform: {platform_name}")

    # Determine allowed file types for this platform
    # Default to CSV, allow XLSX if platform is Flipkart
    allowed_types = ["csv"]
    if platform_slug.lower() == "flipkart":
        allowed_types.append("xlsx")

    for account in accounts:
        account_name = account.get('name', 'Unknown Account')
        account_slug = account.get('slug')

        if not account_slug:
            st.warning(f"Account under '{platform_name}' is missing a 'slug'. Skipping.")
            continue
        
        key_prefix = f"{platform_slug}_{account_slug}"
        st.markdown(f"**Account: {account_name}**")
        col1, col2 = st.columns(2)

        with col1:
            file_key_30day = f"{key_prefix}_30day"
            uploaded_30day = st.file_uploader(
                f"Upload 30-Day Sales Report (.{', .'.join(allowed_types)})", # Dynamically show allowed types
                type=allowed_types, # Pass the list of allowed types
                key=file_key_30day,
                help=f"Sales data for the last 30 days for {platform_name} - {account_name}"
            )
            if uploaded_30day is not None:
                if st.session_state.get(f"{file_key_30day}_processed_id") != id(uploaded_30day):
                    file_path = save_uploaded_file(uploaded_30day, platform_slug, account_slug, "30day", APP_CONFIG)
                    if file_path:
                        st.success(f"30-day report for {account_name} saved: {os.path.basename(file_path)}")
                        st.session_state.uploaded_file_paths[file_key_30day] = file_path
                        st.session_state[f"{file_key_30day}_processed_id"] = id(uploaded_30day)
                    else:
                        st.error(f"Failed to save 30-day report for {account_name}.")
                elif file_key_30day in st.session_state.uploaded_file_paths:
                     st.info(f"Previously uploaded 30-day report for {account_name}: {os.path.basename(st.session_state.uploaded_file_paths[file_key_30day])}")

        with col2:
            file_key_60day = f"{key_prefix}_60day"
            uploaded_60day = st.file_uploader(
                f"Upload 60-Day Sales Report (.{', .'.join(allowed_types)})", # Dynamically show allowed types
                type=allowed_types, # Pass the list of allowed types
                key=file_key_60day,
                help=f"Sales data for the last 60 days for {platform_name} - {account_name}"
            )
            if uploaded_60day is not None:
                if st.session_state.get(f"{file_key_60day}_processed_id") != id(uploaded_60day):
                    file_path = save_uploaded_file(uploaded_60day, platform_slug, account_slug, "60day", APP_CONFIG)
                    if file_path:
                        st.success(f"60-day report for {account_name} saved: {os.path.basename(file_path)}")
                        st.session_state.uploaded_file_paths[file_key_60day] = file_path
                        st.session_state[f"{file_key_60day}_processed_id"] = id(uploaded_60day)
                    else:
                        st.error(f"Failed to save 60-day report for {account_name}.")
                elif file_key_60day in st.session_state.uploaded_file_paths:
                     st.info(f"Previously uploaded 60-day report for {account_name}: {os.path.basename(st.session_state.uploaded_file_paths[file_key_60day])}")
        st.divider()

st.sidebar.subheader("Uploaded Files Summary")
if st.session_state.uploaded_file_paths:
    for key, path in st.session_state.uploaded_file_paths.items():
        if path:
            parts = key.split('_')
            display_name = f"{parts[0].capitalize()} ({parts[1]}) - {parts[2].replace('day','-day')}"
            st.sidebar.caption(f"{display_name}: {os.path.basename(path)}")
else:
    st.sidebar.caption("No files uploaded in this session yet.")
logger.info("Upload Sales Data page loaded.")