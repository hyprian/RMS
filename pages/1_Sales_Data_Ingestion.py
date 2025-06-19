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
from data_processing.sku_mapper import SKUMapper # Re-use existing mapper
from data_ingestion.amazon_parser import AmazonSalesParser
from data_ingestion.flipkart_parser import FlipkartSalesParser
from data_ingestion.meesho_parser import MeeshoSalesParser
# from utils.file_utils import save_uploaded_file # We might not save raw files long-term if processed immediately

import logging
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Sales Data Ingestion - RMS", layout="wide")
st.title("📊 Sales Data Ingestion")
st.markdown("Upload platform sales reports to be processed and stored in Baserow.")

# --- Initialize Tools ---
@st.cache_resource
def get_ingestion_tools():
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        # Assuming project_root is correctly defined globally for SKUMapper cache path
        sku_mapper_instance = SKUMapper(
            baserow_fetcher=fetcher,
            sku_mapping_table_id=APP_CONFIG['baserow']['sku_mapping_table_id'],
            combo_sku_table_id=APP_CONFIG['baserow']['combo_sku_table_id'],
            cache_config=APP_CONFIG.get('cache', {}),
            project_root_dir=project_root,
            force_refresh_cache=st.session_state.get('force_refresh_baserow_cache_ingestion', False) # Separate cache refresh for this page
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

# Date range for the report being uploaded
today = date.today()
col_date1, col_date2 = st.columns(2)
with col_date1:
    report_start_date = st.date_input("Report Start Date:", value=today, key="ingest_report_start")
with col_date2:
    report_end_date = st.date_input("Report End Date:", value=today, key="ingest_report_end")

if report_start_date > report_end_date:
    st.error("Report Start Date cannot be after Report End Date.")
    st.stop()

# Display existing data ranges
processed_sales_table_id = APP_CONFIG['baserow'].get('processed_sales_data_table_id')
if processed_sales_table_id and selected_platform_conf and selected_account_name:
    with st.spinner("Fetching existing data ranges..."):
        # This might be slow if the table is huge. Consider optimizing or caching.
        # For now, let's fetch it on each interaction that changes platform/account.
        # A better UX might be to fetch once and store in session state, updating after successful upload.
        key_for_ranges = f"date_ranges_{selected_platform_conf['slug']}_{selected_account_name}" # Crude cache key
        
        # Simple caching of the fetched ranges to avoid repeated API calls on minor UI changes
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


# File uploader
allowed_types_upload = ["csv"]
if selected_platform_conf and selected_platform_conf['slug'].lower() == "flipkart":
    allowed_types_upload = ["xlsx"]
uploaded_file = st.file_uploader(f"Upload {selected_platform_name} Sales Report", type=allowed_types_upload, key="ingest_file_uploader")


if st.button("Process and Upload to Baserow", key="ingest_process_button", disabled=not uploaded_file):
    if not selected_platform_conf or not selected_account_name:
        st.error("Please select a valid platform and account.")
        st.stop()
    if not processed_sales_table_id:
        st.error("`processed_sales_data_table_id` is not configured in settings.yaml.")
        st.stop()

    account_conf = next((acc for acc in selected_platform_conf['accounts'] if acc['name'] == selected_account_name), None)
    if not account_conf:
        st.error("Could not find account configuration.")
        st.stop()

    parser = None
    platform_slug_lower = selected_platform_conf['slug'].lower()

    if platform_slug_lower == "amazon":
        parser = AmazonSalesParser(selected_platform_name, account_conf, sku_mapper)
    elif platform_slug_lower == "flipkart":
        parser = FlipkartSalesParser(selected_platform_name, account_conf, sku_mapper)
    elif platform_slug_lower == "meesho":
        parser = MeeshoSalesParser(selected_platform_name, account_conf, sku_mapper)
    else:
        st.error(f"No parser available for platform: {selected_platform_name}")
        st.stop()

    with st.spinner(f"Processing {selected_platform_name} - {selected_account_name} data..."):
        # Save uploaded file temporarily to pass path to parser
        # In a real scenario, you might process from buffer if parsers support it
        temp_dir = os.path.join(project_root, ".tmp_uploads")
        os.makedirs(temp_dir, exist_ok=True)
        temp_file_path = os.path.join(temp_dir, uploaded_file.name)
        with open(temp_file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        try:
            standardized_df = parser.parse(temp_file_path, report_start_date, report_end_date)
        finally:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path) # Clean up temp file

        if standardized_df is None or standardized_df.empty:
            st.warning("No data processed from the file. It might be empty, have incorrect format, or no valid sales after filtering.")
        else:
            st.success(f"Successfully parsed {len(standardized_df)} records from the file.")
            st.dataframe(standardized_df.head()) # Show a preview

            # Add Upload Batch ID and Timestamp
            upload_batch_id = str(uuid.uuid4())
            processed_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            standardized_df['Upload Batch ID'] = upload_batch_id
            standardized_df['Data Processed Timestamp'] = processed_timestamp
            
            # Add/ensure all columns for Baserow table exist, fill blanks if necessary
            # This depends on your Baserow table structure. Example:
            standardized_df['COGS per Unit'] = standardized_df.get('COGS per Unit', 0) # Example default

            # Convert DataFrame to list of dicts for Baserow API
            records_to_upload = standardized_df.to_dict('records')
            
            # De-duplication: For each record, check if it exists for Sale Date, MSKU, Platform, Account.
            # If so, delete existing before inserting. This is a simplified "replace" strategy.
            # This part needs careful implementation of Baserow queries and deletes.
            # For now, we'll assume a simpler append or a more manual de-duplication process in Baserow.
            # A robust solution would involve querying Baserow for existing records matching
            # (Sale Date, MSKU, Platform, Account Name) for the dates in standardized_df,
            # deleting them, and then inserting the new batch.

            # For this POC, let's just try to upload.
            # You would need to implement the delete logic based on your chosen strategy.
            # Example (conceptual - needs actual Baserow field IDs for filtering):
            # for record in records_to_upload:
            #    fetcher.delete_sales_records_for_period(
            #        processed_sales_table_id,
            #        record['Platform'], record['Account Name'], record['MSKU'],
            #        record['Sale Date'], record['Sale Date'] # Deleting for specific day
            #    )

            if st.confirm(f"Upload {len(records_to_upload)} processed records to Baserow?"):
                with st.spinner("Uploading to Baserow..."):
                    success = fetcher.batch_create_rows(processed_sales_table_id, records_to_upload)
                    if success:
                        st.success("Data successfully uploaded to Baserow!")
                        # Invalidate cached date ranges so it refreshes on next interaction
                        range_cache_key = f"date_ranges_{selected_platform_conf['slug']}_{selected_account_name}"
                        if range_cache_key in st.session_state:
                            del st.session_state[range_cache_key]
                        st.rerun() # To refresh the date range display
                    else:
                        st.error("Failed to upload some or all records to Baserow. Check logs.")
            else:
                st.info("Upload cancelled.")