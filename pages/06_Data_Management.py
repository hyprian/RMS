# RMS/pages/03_Data_Management.py
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
st.markdown("Manage and delete processed sales data from Baserow.")

# --- Initialize Tools ---
@st.cache_resource # Cache the fetcher for this page session
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

# --- UI for Deletion ---
st.subheader("Delete Sales Data by Period")

platforms_config = APP_CONFIG.get('platforms', [])
platform_names = ["All Platforms"] + [p['name'] for p in platforms_config] # Add "All" option
selected_platform_name_del = st.selectbox("Select Platform to Delete From:", platform_names, key="delete_platform", index=0)

selected_platform_conf_del = None
account_names_del = ["All Accounts"] # Default for "All Platforms"
selected_account_name_del = "All Accounts"

if selected_platform_name_del != "All Platforms":
    selected_platform_conf_del = next((p for p in platforms_config if p['name'] == selected_platform_name_del), None)
    if selected_platform_conf_del:
        account_names_del = ["All Accounts"] + [acc['name'] for acc in selected_platform_conf_del.get('accounts', [])]
selected_account_name_del = st.selectbox("Select Account to Delete From:", account_names_del, key="delete_account", index=0, disabled=(selected_platform_name_del == "All Platforms"))


col_date_del1, col_date_del2 = st.columns(2)
with col_date_del1:
    delete_start_date = st.date_input("Delete Data From (inclusive):", value=None, key="delete_report_start")
with col_date_del2:
    delete_end_date = st.date_input("Delete Data To (inclusive):", value=None, key="delete_report_end")

st.warning("⚠️ **Caution:** This action will permanently delete sales data from the Baserow table for the selected criteria. This cannot be undone easily.")

if st.button("🗑️ Delete Specified Sales Data", key="delete_data_button", type="primary", disabled=not delete_start_date or not delete_end_date):
    if delete_start_date > delete_end_date:
        st.error("Start Date cannot be after End Date for deletion.")
    else:
        platform_to_delete = selected_platform_name_del if selected_platform_name_del != "All Platforms" else None
        account_to_delete = selected_account_name_del if selected_account_name_del != "All Accounts" else None
        
        start_date_str = delete_start_date.strftime('%Y-%m-%d')
        end_date_str = delete_end_date.strftime('%Y-%m-%d')

        confirmation_message = f"Are you sure you want to delete data for "
        if platform_to_delete:
            confirmation_message += f"Platform: **{platform_to_delete}**"
            if account_to_delete:
                confirmation_message += f", Account: **{account_to_delete}**"
            else:
                confirmation_message += " (All Accounts)"
        else:
            confirmation_message += "**All Platforms**"
        confirmation_message += f" between **{start_date_str}** and **{end_date_str}**?"

        # For a real confirm, you'd ideally use a modal or a two-step process.
        # Streamlit doesn't have native modals. A common workaround is a checkbox or a second button.
        # For simplicity here, we'll proceed if the button is clicked, but in production, add more safeguards.
        
        # Placeholder for a more robust confirmation (e.g., text input "DELETE")
        # if st.text_input("Type DELETE to confirm:", key="delete_confirm_text") == "DELETE":

        with st.spinner(f"Identifying records to delete..."):
            # Construct criteria for get_row_ids_for_criteria
            # This is tricky: get_row_ids_for_criteria expects specific MSKUs.
            # For deleting a whole range, we need a different BaserowFetcher method.
            # Let's add a new method: delete_sales_records_by_range
            
            logger.info(f"Requesting deletion for Table: {processed_sales_table_id}, P: {platform_to_delete}, A: {account_to_delete}, Start: {start_date_str}, End: {end_date_str}")

            # This requires a new BaserowFetcher method that can delete based on broader criteria
            # For now, this is a conceptual call. The actual implementation in BaserowFetcher needs to be robust.
            # It would involve fetching all row IDs matching these broader criteria and then batch deleting them.
            
            # Let's simulate the process:
            # 1. Fetch IDs matching the broader criteria (Platform, Account, Date Range)
            # This is similar to check_existing_data_for_period but returns IDs
            
            # --- This part needs a new BaserowFetcher method ---
            # ids_to_delete = fetcher.get_row_ids_for_range_deletion(
            #     processed_sales_table_id,
            #     platform_to_delete,
            #     account_to_delete,
            #     start_date_str,
            #     end_date_str
            # )
            # For this example, we'll show a message that this needs implementation
            st.error("Deletion by broad range is a complex operation and requires a dedicated Baserow API interaction pattern (fetch IDs by filter, then batch delete). This part is not fully implemented in the current BaserowFetcher for safety. Manual deletion in Baserow or a more specific script is advised for now for bulk range deletes.")
            logger.error("Attempted range deletion - function not fully implemented for broad criteria.")

            # if ids_to_delete:
            #     st.write(f"Found {len(ids_to_delete)} records matching the criteria.")
            #     if st.button(f"Confirm Deletion of {len(ids_to_delete)} Records", key="final_delete_confirm"):
            #         with st.spinner("Deleting records from Baserow..."):
            #             delete_success = fetcher.delete_rows_by_ids_one_by_one(processed_sales_table_id, ids_to_delete) # Or batch if it worked
            #             if delete_success:
            #                 st.success(f"Successfully processed deletion request for {len(ids_to_delete)} records.")
            #                 # Invalidate relevant date range caches if you have them for this page
            #                 st.rerun()
            #             else:
            #                 st.error("Failed to delete some or all records. Check logs.")
            # else:
            #     st.info("No records found matching the specified criteria for deletion.")
        # else:
        #     st.warning("Deletion not confirmed.")