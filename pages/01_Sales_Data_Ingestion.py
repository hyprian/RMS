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
from data_ingestion.shopify_parser import ShopifySalesParser
from data_ingestion.firstcry_parser import FirstCrySalesParser # NEW IMPORT


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
if selected_platform_conf:
    platform_slug = selected_platform_conf['slug'].lower()
    if platform_slug == "flipkart":
        allowed_types_upload = ["xlsx"]
    # --- NEW LOGIC ---
    elif platform_slug == "firstcry":
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
    elif platform_slug_lower == "shopify":
        parser = ShopifySalesParser(selected_platform_name, account_conf, sku_mapper)
    elif platform_slug_lower == "firstcry":
        parser = FirstCrySalesParser(selected_platform_name, account_conf, sku_mapper)
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
        st.warning("No data processed from the file...")
        st.session_state.ingestion_standardized_df = None
        st.session_state.ingestion_records_to_upload = None
        st.session_state.ingestion_proceed_with_upload = False # New flag
    else:
        st.success(f"Successfully parsed {len(standardized_df)} records from the file.")
        # ... (add Upload Batch ID, Timestamp, COGS, target_cols reindex) ...
        upload_batch_id = str(uuid.uuid4())
        processed_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        standardized_df['Upload Batch ID'] = upload_batch_id
        standardized_df['Data Processed Timestamp'] = processed_timestamp
        standardized_df['COGS per Unit'] = standardized_df.get('COGS per Unit', 0.0)
        target_cols = ['Sale Date', 'MSKU', 'Platform', 'Account Name', 'Platform SKU', 
                       'Order ID', 'Quantity Sold', 'Gross Revenue', 'Discounts', 
                       'Platform Fees', 'Net Revenue', 'COGS per Unit', 
                       'Report Source File', 'Upload Batch ID', 'Data Processed Timestamp',
                       'Report Period Start Date']
        for col in target_cols:
            if col not in standardized_df.columns:
                default_val = 0.0 if col in ['Quantity Sold', 'Gross Revenue', 'Discounts', 'Platform Fees', 'Net Revenue', 'COGS per Unit'] else None
                standardized_df[col] = default_val
        standardized_df = standardized_df.reindex(columns=target_cols)


        st.session_state.ingestion_standardized_df = standardized_df
        st.session_state.ingestion_records_to_upload = standardized_df.to_dict('records')
        st.session_state.ingestion_platform_conf = selected_platform_conf
        st.session_state.ingestion_account_name = selected_account_name
        st.session_state.ingestion_report_start_date_str = report_start_date.strftime('%Y-%m-%d')
        st.session_state.ingestion_report_end_date_str = report_end_date.strftime('%Y-%m-%d')
        
        # Check for existing data for the *entire report period* of the new upload
        with st.spinner("Checking for existing data in the report's date range..."):
            data_exists = fetcher.check_existing_data_for_period(
                processed_sales_table_id,
                selected_platform_name,
                selected_account_name,
                st.session_state.ingestion_report_start_date_str,
                st.session_state.ingestion_report_end_date_str
            )
        st.session_state.ingestion_data_exists_in_range = data_exists
        st.session_state.ingestion_proceed_with_upload = False # Reset proceed flag


# --- Display Preview and Upload Button Logic ---
if st.session_state.get('ingestion_standardized_df') is not None:
    st.subheader("Preview of Processed Data")
    st.dataframe(st.session_state.ingestion_standardized_df.head())
    
    num_records = len(st.session_state.ingestion_records_to_upload)
    st.markdown(f"---")
    
    if st.session_state.get('ingestion_data_exists_in_range'):
        st.warning(f"""
        ⚠️ **Data Overlap Warning!**
        Existing data for **{st.session_state.ingestion_platform_conf['name']} - {st.session_state.ingestion_account_name}**
        has been found within the date range of your current upload 
        (**{st.session_state.ingestion_report_start_date_str}** to **{st.session_state.ingestion_report_end_date_str}**).

        If you proceed, existing records for specific MSKUs on specific dates within this range that are also present in your new upload will be **DELETED and REPLACED**. 
        Data outside this new report's specific daily entries but within the overall range will remain untouched unless directly overwritten by a new daily entry.
        """)
        if st.button(f"Proceed and Upload {num_records} Records (Overwrite Overlapping)", key="ingest_confirm_overwrite_button"):
            st.session_state.ingestion_proceed_with_upload = True
    else:
        if st.button(f"Confirm and Upload {num_records} Records to Baserow", key="ingest_confirm_new_upload_button"):
            st.session_state.ingestion_proceed_with_upload = True

    if st.session_state.get('ingestion_proceed_with_upload'):
        if not processed_sales_table_id:
            st.error("`processed_sales_data_table_id` is not configured in settings.yaml.")
            st.stop() # Should not happen if preview is shown
        
        with st.spinner("Preparing and Uploading to Baserow..."):
            records_to_upload_final = st.session_state.ingestion_records_to_upload
            
            # Deletion Logic
            if st.session_state.get('ingestion_data_exists_in_range'):
                logger.info("Overlap detected. Identifying records to delete...")
                # Create a list of criteria for records to delete based on the new upload
                # Each criterion is a dict: {'Sale Date', 'MSKU', 'Platform', 'Account Name'}
                criteria_for_deletion = []
                for rec in records_to_upload_final:
                    criteria_for_deletion.append({
                        'Sale Date': rec['Sale Date'], # Assumes YYYY-MM-DD string
                        'MSKU': rec['MSKU'],
                        'Platform': rec['Platform'],
                        'Account Name': rec['Account Name']
                    })
                
                # Get IDs of rows in Baserow that match these exact criteria
                # The get_row_ids_for_criteria needs to be robust.
                # It should ideally fetch rows matching platform, account, and the MIN/MAX Sale Date of records_to_upload_final,
                # then locally filter against the full criteria_for_deletion list.
                if criteria_for_deletion: # Only proceed if there are records to potentially delete/replace
                    row_ids_to_delete = fetcher.get_row_ids_for_criteria(
                        processed_sales_table_id,
                        criteria_for_deletion # Pass the list of specific records from the new upload
                    )
                    if row_ids_to_delete:
                        logger.info(f"Attempting to delete {len(row_ids_to_delete)} existing records before insert.")
                        delete_success = fetcher.delete_rows_by_ids_one_by_one(processed_sales_table_id, row_ids_to_delete)
                        if not delete_success:
                            st.error("Failed to delete existing overlapping records. Upload aborted. Check logs.")
                            st.stop() # Stop further processing
                        else:
                            st.info(f"Successfully deleted {len(row_ids_to_delete)} overlapping records.")
                    else:
                        logger.info("No specific existing records found matching the new upload's daily entries for deletion.")
                else:
                    logger.info("New upload is empty, no deletion criteria formed.")


            # Proceed with upload
            upload_success = fetcher.batch_create_rows(processed_sales_table_id, records_to_upload_final)
            if upload_success:
                st.success("Data successfully uploaded to Baserow!")
                if st.session_state.get('ingestion_platform_conf') and st.session_state.get('ingestion_account_name'):
                    platform_slug_for_cache = st.session_state.ingestion_platform_conf['slug']
                    account_name_for_cache = st.session_state.ingestion_account_name
                    range_cache_key = f"date_ranges_{platform_slug_for_cache}_{account_name_for_cache}"
                    if range_cache_key in st.session_state:
                        del st.session_state[range_cache_key]
                        logger.info(f"Cleared cached date range for key: {range_cache_key}")

                
                # Clear processed data from session state
                st.session_state.ingestion_standardized_df = None
                st.session_state.ingestion_records_to_upload = None
                st.session_state.ingestion_platform_conf = None
                st.session_state.ingestion_account_name = None
                st.session_state.ingestion_data_exists_in_range = False
                st.session_state.ingestion_proceed_with_upload = False
                st.rerun()
            else:
                st.error("Failed to upload some or all records to Baserow. Check logs.")
        # Reset proceed flag after action or if no action taken
        # st.session_state.ingestion_proceed_with_upload = False # This is now handled by rerun or if block ends
