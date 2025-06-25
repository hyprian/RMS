# RMS/pages/01_Sales_Data_Ingestion.py
import streamlit as st
import pandas as pd
import os
import sys
from datetime import datetime, date
import uuid

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
from data_processing.sku_mapper import SKUMapper
from data_ingestion.amazon_parser import AmazonSalesParser
from data_ingestion.flipkart_parser import FlipkartSalesParser
from data_ingestion.meesho_parser import MeeshoSalesParser

import logging
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Sales Data Ingestion - RMS", layout="wide")
st.title("📊 Sales Data Ingestion")
st.markdown("Upload platform sales reports to be processed and stored in Baserow.")

# --- Initialize Session State for this page ---
if 'ingestion_standardized_df' not in st.session_state:
    st.session_state.ingestion_standardized_df = None
if 'ingestion_records_to_upload' not in st.session_state:
    st.session_state.ingestion_records_to_upload = None
if 'ingestion_platform_conf' not in st.session_state:
    st.session_state.ingestion_platform_conf = None
if 'ingestion_account_name' not in st.session_state:
    st.session_state.ingestion_account_name = None


# --- Initialize Tools ---
@st.cache_resource
def get_ingestion_tools():
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        sku_mapper_instance = SKUMapper(
            baserow_fetcher=fetcher,
            sku_mapping_table_id=APP_CONFIG['baserow']['sku_mapping_table_id'],
            combo_sku_table_id=APP_CONFIG['baserow']['combo_sku_table_id'],
            cache_config=APP_CONFIG.get('cache', {}),
            project_root_dir=project_root,
            force_refresh_cache=st.session_state.get('force_refresh_baserow_cache_ingestion', False)
        )
        return fetcher, sku_mapper_instance
    except Exception as e:
        st.error(f"Error initializing tools: {e}")
        return None, None

fetcher, sku_mapper = get_ingestion_tools()
if not fetcher or not sku_mapper:
    st.error("Failed to initialize data processing tools. Check Baserow connection and config.")
    st.stop()

st.sidebar.checkbox("Force refresh SKU/Combo cache for ingestion", key="force_refresh_baserow_cache_ingestion")

# --- UI for Upload ---
platforms_config = APP_CONFIG.get('platforms', [])
platform_names = [p['name'] for p in platforms_config]
selected_platform_name = st.selectbox("Select Platform:", platform_names, key="ingest_platform")

selected_platform_conf = next((p for p in platforms_config if p['name'] == selected_platform_name), None)
account_names = []
if selected_platform_conf:
    account_names = [acc['name'] for acc in selected_platform_conf.get('accounts', [])]
selected_account_name = st.selectbox("Select Account:", account_names, key="ingest_account", disabled=not selected_platform_conf)

today = date.today()
col_date1, col_date2 = st.columns(2)
with col_date1:
    report_start_date = st.date_input("Report Start Date:", value=None, key="ingest_report_start", help="The first day of data included in your report.") # Changed value to None
with col_date2:
    report_end_date = st.date_input("Report End Date:", value=None, key="ingest_report_end", help="The last day of data included in your report.") # Changed value to None

if report_start_date and report_end_date and report_start_date > report_end_date:
    st.error("Report Start Date cannot be after Report End Date.")
    st.stop()

processed_sales_table_id = APP_CONFIG['baserow'].get('processed_sales_data_table_id')
if processed_sales_table_id and selected_platform_conf and selected_account_name:
    # ... (Existing data ranges display logic - keep as is) ...
    with st.spinner("Fetching existing data ranges..."):
        key_for_ranges = f"date_ranges_{selected_platform_conf['slug']}_{selected_account_name}"
        if key_for_ranges not in st.session_state or st.button("Refresh Data Ranges", key=f"refresh_{key_for_ranges}"):
            st.session_state[key_for_ranges] = fetcher.get_existing_sales_date_ranges(processed_sales_table_id)
        existing_ranges = st.session_state.get(key_for_ranges, {})
        platform_ranges = existing_ranges.get(selected_platform_name, {})
        account_range_info = platform_ranges.get(selected_account_name)
        if account_range_info:
            st.info(f"Data for {selected_platform_name} - {selected_account_name} currently exists in Baserow from **{account_range_info['min_date']}** to **{account_range_info['max_date']}**.")
        else:
            st.info(f"No existing sales data found in Baserow for {selected_platform_name} - {selected_account_name}.")
else:
    st.warning("Configure `processed_sales_data_table_id` in settings.yaml to see existing data ranges.")

allowed_types_upload = ["csv"]
if selected_platform_conf and selected_platform_conf['slug'].lower() == "flipkart":
    allowed_types_upload = ["xlsx"]
uploaded_file = st.file_uploader(f"Upload {selected_platform_name} Sales Report", type=allowed_types_upload, key="ingest_file_uploader")

# "Process File" Button (Step 1)
if st.button("Process File (Preview Data)", key="ingest_process_file_button", disabled=not uploaded_file or not report_start_date or not report_end_date):
    if not selected_platform_conf or not selected_account_name:
        st.error("Please select a valid platform and account.")
        st.stop()

    account_conf = next((acc for acc in selected_platform_conf['accounts'] if acc['name'] == selected_account_name), None)
    if not account_conf: st.error("Could not find account configuration."); st.stop()

    parser = None; platform_slug_lower = selected_platform_conf['slug'].lower()
    if platform_slug_lower == "amazon": parser = AmazonSalesParser(selected_platform_name, account_conf, sku_mapper)
    elif platform_slug_lower == "flipkart": parser = FlipkartSalesParser(selected_platform_name, account_conf, sku_mapper)
    elif platform_slug_lower == "meesho": parser = MeeshoSalesParser(selected_platform_name, account_conf, sku_mapper)
    else: st.error(f"No parser available for platform: {selected_platform_name}"); st.stop()

    with st.spinner(f"Processing {selected_platform_name} - {selected_account_name} data..."):
        temp_dir = os.path.join(project_root, ".tmp_uploads"); os.makedirs(temp_dir, exist_ok=True)
        temp_file_path = os.path.join(temp_dir, uploaded_file.name)
        with open(temp_file_path, "wb") as f: f.write(uploaded_file.getbuffer())
        
        try:
            standardized_df = parser.parse(temp_file_path, report_start_date, report_end_date)
        finally:
            if os.path.exists(temp_file_path): os.remove(temp_file_path)

        if standardized_df is None or standardized_df.empty:
            st.warning("No data processed from the file. It might be empty, have incorrect format, or no valid sales after filtering.")
            st.session_state.ingestion_standardized_df = None
            st.session_state.ingestion_records_to_upload = None
        else:
            st.success(f"Successfully parsed {len(standardized_df)} records from the file.")
            
            upload_batch_id = str(uuid.uuid4())
            processed_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            standardized_df['Upload Batch ID'] = upload_batch_id
            standardized_df['Data Processed Timestamp'] = processed_timestamp
            standardized_df['COGS per Unit'] = standardized_df.get('COGS per Unit', 0.0) # Ensure float
            
            # Ensure all target columns exist before converting to dicts
            target_cols = ['Sale Date', 'MSKU', 'Platform', 'Account Name', 'Platform SKU', 
                           'Order ID', 'Quantity Sold', 'Gross Revenue', 'Discounts', 
                           'Platform Fees', 'Net Revenue', 'COGS per Unit', 
                           'Report Source File', 'Upload Batch ID', 'Data Processed Timestamp']
            for col in target_cols:
                if col not in standardized_df.columns:
                    if col in ['Quantity Sold', 'Gross Revenue', 'Discounts', 'Platform Fees', 'Net Revenue', 'COGS per Unit']:
                        standardized_df[col] = 0.0 # Default numeric to 0.0
                    else:
                        standardized_df[col] = None # Default others to None (or empty string)
            
            # Reorder columns to match target table for clarity, though not strictly necessary for to_dict('records')
            standardized_df = standardized_df.reindex(columns=target_cols)


            st.session_state.ingestion_standardized_df = standardized_df
            st.session_state.ingestion_records_to_upload = standardized_df.to_dict('records')
            st.session_state.ingestion_platform_conf = selected_platform_conf # Store for upload button
            st.session_state.ingestion_account_name = selected_account_name   # Store for upload button


# Display Preview and Upload Button (Step 2)
if st.session_state.ingestion_standardized_df is not None:
    st.subheader("Preview of Processed Data")
    st.dataframe(st.session_state.ingestion_standardized_df.head())
    
    num_records = len(st.session_state.ingestion_records_to_upload)
    st.markdown(f"---")
    st.markdown(f"**Ready to upload {num_records} records to Baserow.**")
    
    # De-duplication strategy note
    st.warning("""
    **De-duplication Note:** Currently, this upload will **append** data. 
    If you upload data for the same MSKU/Platform/Account/Sale Date multiple times, you will get duplicate entries.
    A robust de-duplication (e.g., delete existing matching records before insert) needs to be implemented
    if re-uploading data for the same period is expected.
    """)

    if st.button(f"Confirm and Upload {num_records} Records to Baserow", key="ingest_confirm_upload_button"):
        if not processed_sales_table_id:
            st.error("`processed_sales_data_table_id` is not configured in settings.yaml.")
            st.stop()
        
        with st.spinner("Uploading to Baserow..."):
            # --- DELETION LOGIC WILL GO HERE (See Section II) ---

            success = fetcher.batch_create_rows(processed_sales_table_id, st.session_state.ingestion_records_to_upload)
            if success:
                st.success("Data successfully uploaded to Baserow!")
                
                # Invalidate cached date ranges so it refreshes
                if st.session_state.ingestion_platform_conf and st.session_state.ingestion_account_name:
                    range_cache_key = f"date_ranges_{st.session_state.ingestion_platform_conf['slug']}_{st.session_state.ingestion_account_name}"
                    if range_cache_key in st.session_state:
                        del st.session_state[range_cache_key] # This forces a refresh of the date range display on rerun
                        logger.info(f"Invalidated date range cache key: {range_cache_key}")
                
                # Clear the processed data from session state after successful upload
                st.session_state.ingestion_standardized_df = None
                st.session_state.ingestion_records_to_upload = None
                st.session_state.ingestion_platform_conf = None
                st.session_state.ingestion_account_name = None
                st.rerun() # To refresh the page and date range display
            else:
                st.error("Failed to upload some or all records to Baserow. Check logs.")
elif st.session_state.ingestion_standardized_df is None and uploaded_file and (report_start_date and report_end_date):
    # This case handles if "Process File" was clicked but resulted in no data
    # The warning from the processing block itself should cover this.
    pass