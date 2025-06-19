# RMS/pages/1_Upload_Sales_Data.py
import streamlit as st
import os
import sys
# from datetime import datetime # Not directly used here anymore

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
Upload your sales reports for each platform and account.
- For **30-day reports**: Upload a single file.
- For **60-day reports**:
    - For Amazon (and others not specified): Upload a single 60-day report.
    - For Flipkart & Meesho: Upload two separate 30-day reports (Most Recent Month and Previous Month).
Supported formats: CSV, XLSX.
""")

platforms_config = APP_CONFIG.get('platforms', [])
if not platforms_config:
    st.error("Platform configuration is missing in `settings.yaml`. Please define platforms and accounts.")
    st.stop()

if 'uploaded_file_paths' not in st.session_state:
    st.session_state.uploaded_file_paths = {}

platforms_needing_two_files_for_60day = ["meesho", "flipkart"] # Configurable if needed

for platform in platforms_config:
    platform_name = platform.get('name', 'Unknown Platform')
    platform_slug = platform.get('slug')
    accounts = platform.get('accounts', [])

    if not platform_slug:
        st.warning(f"Platform '{platform_name}' is missing a 'slug' in config. Skipping.")
        continue

    st.subheader(f"Platform: {platform_name}")

    allowed_types = ["csv"]
    if platform_slug.lower() == "flipkart": # Flipkart specifically uses xlsx
        allowed_types = ["xlsx"] # Only xlsx for flipkart as per previous request
    # For other platforms, you might want to allow both ["csv", "xlsx"]
    # For simplicity, let's assume Meesho also uses CSV for now, adjust if needed.
    # if platform_slug.lower() in ["meesho", "amazon"]:
    #     allowed_types = ["csv"]


    for account in accounts:
        account_name = account.get('name', 'Unknown Account')
        account_slug = account.get('slug')

        if not account_slug:
            st.warning(f"Account under '{platform_name}' is missing a 'slug'. Skipping.")
            continue
        
        key_prefix = f"{platform_slug}_{account_slug}"
        st.markdown(f"**Account: {account_name}**")
        
        col1, col2 = st.columns(2)

        with col1: # 30-Day Report Uploader
            file_key_30day = f"{key_prefix}_30day"
            uploaded_30day = st.file_uploader(
                f"Upload 30-Day Sales Report (.{', .'.join(allowed_types)})",
                type=allowed_types,
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
                    else: st.error(f"Failed to save 30-day report for {account_name}.")
                elif file_key_30day in st.session_state.uploaded_file_paths:
                     st.info(f"Using previously uploaded 30-day report for {account_name}: {os.path.basename(st.session_state.uploaded_file_paths[file_key_30day])}")

        with col2: # 60-Day Report Uploader(s)
            if platform_slug.lower() in platforms_needing_two_files_for_60day:
                st.markdown("For 60-Day Sales (Upload two 30-day files):")
                # Uploader for Month 1 (Most Recent)
                file_key_60day_m1 = f"{key_prefix}_60day_month1"
                uploaded_60day_m1 = st.file_uploader(
                    f"60-Day: Recent Month (.{', .'.join(allowed_types)})",
                    type=allowed_types, key=file_key_60day_m1,
                    help=f"Most recent 30-day sales report for {platform_name} - {account_name}"
                )
                if uploaded_60day_m1 is not None:
                    if st.session_state.get(f"{file_key_60day_m1}_processed_id") != id(uploaded_60day_m1):
                        path_m1 = save_uploaded_file(uploaded_60day_m1, platform_slug, account_slug, "60day_month1", APP_CONFIG)
                        if path_m1:
                            st.success(f"60-Day (Recent Month) for {account_name} saved: {os.path.basename(path_m1)}")
                            st.session_state.uploaded_file_paths[file_key_60day_m1] = path_m1
                            st.session_state[f"{file_key_60day_m1}_processed_id"] = id(uploaded_60day_m1)
                        else: st.error(f"Failed to save 60-Day (Recent Month) for {account_name}.")
                    elif file_key_60day_m1 in st.session_state.uploaded_file_paths:
                        st.info(f"Using previously uploaded 60-Day (Recent Month) for {account_name}: {os.path.basename(st.session_state.uploaded_file_paths[file_key_60day_m1])}")

                # Uploader for Month 2 (Previous)
                file_key_60day_m2 = f"{key_prefix}_60day_month2"
                uploaded_60day_m2 = st.file_uploader(
                    f"60-Day: Previous Month (.{', .'.join(allowed_types)})",
                    type=allowed_types, key=file_key_60day_m2,
                    help=f"Previous 30-day sales report for {platform_name} - {account_name}"
                )
                if uploaded_60day_m2 is not None:
                    if st.session_state.get(f"{file_key_60day_m2}_processed_id") != id(uploaded_60day_m2):
                        path_m2 = save_uploaded_file(uploaded_60day_m2, platform_slug, account_slug, "60day_month2", APP_CONFIG)
                        if path_m2:
                            st.success(f"60-Day (Previous Month) for {account_name} saved: {os.path.basename(path_m2)}")
                            st.session_state.uploaded_file_paths[file_key_60day_m2] = path_m2
                            st.session_state[f"{file_key_60day_m2}_processed_id"] = id(uploaded_60day_m2)
                        else: st.error(f"Failed to save 60-Day (Previous Month) for {account_name}.")
                    elif file_key_60day_m2 in st.session_state.uploaded_file_paths:
                        st.info(f"Using previously uploaded 60-Day (Previous Month) for {account_name}: {os.path.basename(st.session_state.uploaded_file_paths[file_key_60day_m2])}")
            else: # Standard single 60-day uploader
                file_key_60day = f"{key_prefix}_60day"
                uploaded_60day = st.file_uploader(
                    f"Upload 60-Day Sales Report (.{', .'.join(allowed_types)})",
                    type=allowed_types, key=file_key_60day,
                    help=f"Sales data for the last 60 days for {platform_name} - {account_name}"
                )
                if uploaded_60day is not None:
                    if st.session_state.get(f"{file_key_60day}_processed_id") != id(uploaded_60day):
                        file_path = save_uploaded_file(uploaded_60day, platform_slug, account_slug, "60day", APP_CONFIG)
                        if file_path:
                            st.success(f"60-day report for {account_name} saved: {os.path.basename(file_path)}")
                            st.session_state.uploaded_file_paths[file_key_60day] = file_path
                            st.session_state[f"{file_key_60day}_processed_id"] = id(uploaded_60day)
                        else: st.error(f"Failed to save 60-day report for {account_name}.")
                    elif file_key_60day in st.session_state.uploaded_file_paths:
                        st.info(f"Using previously uploaded 60-day report for {account_name}: {os.path.basename(st.session_state.uploaded_file_paths[file_key_60day])}")
        st.divider()

st.sidebar.subheader("Uploaded Files Summary")
if st.session_state.uploaded_file_paths:
    for key, path in st.session_state.uploaded_file_paths.items():
        if path:
            # Reconstruct a user-friendly name from the key
            parts = key.split('_') # e.g. ['flipkart', 'fkacc1', '60day', 'month1']
            platform_display = parts[0].capitalize()
            account_display = parts[1] # slug
            report_period = parts[2].replace('day','-day')
            month_specifier = ""
            if "month1" in parts: month_specifier = " (Recent Month)"
            elif "month2" in parts: month_specifier = " (Previous Month)"
            
            display_name = f"{platform_display} ({account_display}) - {report_period}{month_specifier}"
            st.sidebar.caption(f"{display_name}: {os.path.basename(path)}")
else:
    st.sidebar.caption("No files uploaded in this session yet.")
logger.info("Upload Sales Data page loaded.")