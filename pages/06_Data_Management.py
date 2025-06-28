# RMS/pages/05_Data_Management.py
import streamlit as st
import pandas as pd
import os
import sys
from datetime import date

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher

import logging
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Data Management - RMS", layout="wide")
st.title("🧹 Sales Data Management")
st.markdown("Manage and delete processed sales data from the Baserow `Processed Sales Data` table.")

# --- Initialize Tools ---
@st.cache_resource
def get_management_tools():
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        return fetcher
    except Exception as e:
        st.error(f"Error initializing Baserow connection: {e}")
        return None

fetcher = get_management_tools()
if not fetcher:
    st.error("Failed to initialize Baserow connection. Check configuration.")
    st.stop()

processed_sales_table_id = APP_CONFIG['baserow'].get('processed_sales_data_table_id')
if not processed_sales_table_id:
    st.error("`processed_sales_data_table_id` is not configured in settings.yaml. Cannot manage data.")
    st.stop()

# --- Display Existing Data Ranges ---
st.subheader("Available Data Ranges in Baserow")
with st.spinner("Fetching existing data ranges..."):
    # Use a session state key to cache this for the page session
    if 'data_management_ranges' not in st.session_state or st.button("Refresh Data Ranges"):
        st.session_state.data_management_ranges = fetcher.get_existing_sales_date_ranges(processed_sales_table_id)
    
    existing_ranges = st.session_state.get('data_management_ranges', {})

    if not existing_ranges:
        st.info("No processed sales data found in the Baserow table.")
    else:
        range_data = []
        for platform, accounts in existing_ranges.items():
            for account, dates in accounts.items():
                range_data.append({
                    "Platform": platform,
                    "Account": account,
                    "Earliest Record": dates.get('min_date'),
                    "Latest Record": dates.get('max_date')
                })
        st.dataframe(pd.DataFrame(range_data), use_container_width=True)

st.divider()

# --- UI for Deletion ---
st.subheader("Delete Sales Data")

# Section for specific deletion
with st.expander("Delete Data for a Specific Period", expanded=True):
    platforms_config = APP_CONFIG.get('platforms', [])
    platform_names = ["All Platforms"] + [p['name'] for p in platforms_config]
    selected_platform_name_del = st.selectbox("Select Platform:", platform_names, key="delete_platform", index=0)

    account_names_del = ["All Accounts"]
    if selected_platform_name_del != "All Platforms":
        selected_platform_conf_del = next((p for p in platforms_config if p['name'] == selected_platform_name_del), None)
        if selected_platform_conf_del:
            account_names_del = ["All Accounts"] + [acc['name'] for acc in selected_platform_conf_del.get('accounts', [])]
    selected_account_name_del = st.selectbox("Select Account:", account_names_del, key="delete_account", index=0, disabled=(selected_platform_name_del == "All Platforms"))

    col_date_del1, col_date_del2 = st.columns(2)
    with col_date_del1:
        delete_start_date = st.date_input("Delete Data From (inclusive):", value=None, key="delete_report_start")
    with col_date_del2:
        delete_end_date = st.date_input("Delete Data To (inclusive):", value=None, key="delete_report_end")

    if st.button("Find Records to Delete", key="find_records_button", disabled=not delete_start_date or not delete_end_date):
        if delete_start_date > delete_end_date:
            st.error("Start Date cannot be after End Date for deletion.")
        else:
            platform_to_delete = selected_platform_name_del if selected_platform_name_del != "All Platforms" else None
            account_to_delete = selected_account_name_del if selected_account_name_del != "All Accounts" else None
            start_date_str = delete_start_date.strftime('%Y-%m-%d')
            end_date_str = delete_end_date.strftime('%Y-%m-%d')
            
            with st.spinner("Identifying records to delete... This may take a moment for large ranges."):
                ids_to_delete = fetcher.get_row_ids_for_range_deletion(
                    processed_sales_table_id,
                    start_date_str,
                    end_date_str,
                    platform=platform_to_delete,
                    account_name=account_to_delete
                )
                # Store the found IDs in session state for the final confirmation step
                st.session_state.ids_to_delete_for_confirmation = ids_to_delete
                st.session_state.delete_criteria_summary = f"Platform: {platform_to_delete or 'All'}, Account: {account_to_delete or 'All'}, Period: {start_date_str} to {end_date_str}"

# Display confirmation and final delete button if IDs have been found
if 'ids_to_delete_for_confirmation' in st.session_state and st.session_state.ids_to_delete_for_confirmation is not None:
    ids_to_delete = st.session_state.ids_to_delete_for_confirmation
    criteria_summary = st.session_state.delete_criteria_summary
    
    st.warning(f"**Confirmation Required:** Found **{len(ids_to_delete)}** records matching your criteria: *{criteria_summary}*.")
    
    if len(ids_to_delete) > 0:
        # Use a text input for strong confirmation
        confirm_text = st.text_input('To confirm deletion, please type "DELETE" into the box below:')
        if st.button("CONFIRM PERMANENT DELETION", type="primary", disabled=(confirm_text != "DELETE")):
            with st.spinner(f"Deleting {len(ids_to_delete)} records one by one..."):
                delete_success = fetcher.batch_delete_rows(processed_sales_table_id, ids_to_delete)
                if delete_success:
                    st.success(f"Successfully processed deletion request for {len(ids_to_delete)} records.")
                    # Clear the cached ranges and the IDs to delete from session state
                    st.session_state.pop('data_management_ranges', None)
                    st.session_state.pop('ids_to_delete_for_confirmation', None)
                    st.rerun()
                else:
                    st.error("Failed to delete some or all records. Check logs for details.")
    else:
        st.info("No records were found matching your criteria, so there is nothing to delete.")
        # Clear the state
        st.session_state.pop('ids_to_delete_for_confirmation', None)

st.divider()

# Section for deleting ALL data
with st.expander("⚠️ Danger Zone: Delete All Sales Data"):
    st.error("This will permanently delete **ALL** records from the `Processed Sales Data` table. This action is irreversible.")
    
    delete_all_confirm_text = st.text_input('To confirm deleting ALL sales data, type "DELETE ALL DATA":')
    
    if st.button("DELETE ALL PROCESSED SALES DATA", disabled=(delete_all_confirm_text != "DELETE ALL DATA")):
        with st.spinner("Fetching all record IDs to delete..."):
            # Call get_row_ids_for_range_deletion with no filters to get all IDs
            all_ids_to_delete = fetcher.get_row_ids_for_range_deletion(processed_sales_table_id, None, None)
            if all_ids_to_delete:
                st.write(f"Found {len(all_ids_to_delete)} total records. Proceeding with deletion...")
                with st.spinner(f"Deleting all {len(all_ids_to_delete)} records one by one..."):
                    delete_success = fetcher.batch_delete_rows(processed_sales_table_id, all_ids_to_delete)
                    if delete_success:
                        st.success("Successfully deleted all processed sales data.")
                        st.session_state.pop('data_management_ranges', None)
                        st.session_state.pop('ids_to_delete_for_confirmation', None)
                        st.rerun()
                    else:
                        st.error("An error occurred during mass deletion. Some data may remain. Check logs.")
            else:
                st.info("No data was found in the table to delete.")