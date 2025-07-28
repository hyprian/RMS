# RMS/pages/10_Receive_Stock_GRN.py
import streamlit as st
import pandas as pd
import os
import sys
from datetime import date

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
from po_module.po_management import get_all_pos, get_po_details, update_po_line_item

import logging
logger = logging.getLogger(__name__)

st.set_page_config(page_title="GRN / Receive Stock - RMS", layout="wide")
st.title("📦 GRN / Receive Stock")
st.markdown("Log received quantities and quality check details against Purchase Orders.")

# --- Initialize Tools & Load Data ---
@st.cache_resource
def get_grn_tools():
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        return fetcher
    except Exception as e:
        st.error(f"Error initializing Baserow connection: {e}"); return None

fetcher = get_grn_tools()
if not fetcher: st.error("Failed to initialize Baserow connection."); st.stop()

po_table_id = APP_CONFIG['baserow'].get('purchase_orders_table_id')
if not po_table_id:
    st.error("`purchase_orders_table_id` must be configured in settings.yaml.")
    st.stop()

# --- Data Loading and Caching ---
def load_po_data_for_grn():
    with st.spinner("Loading open purchase orders from Baserow..."):
        po_df = get_all_pos(fetcher, po_table_id)
        return po_df

if 'grn_all_pos_df' not in st.session_state or st.sidebar.button("Refresh PO List"):
    st.session_state.grn_all_pos_df = load_po_data_for_grn()

all_pos_df = st.session_state.get('grn_all_pos_df')

if all_pos_df is None or all_pos_df.empty:
    st.info("No Purchase Orders found in Baserow."); st.stop()

# --- UI for PO & Vendor Selection ---
st.header("1. Select Purchase Order and Vendor to Receive")

# Filter for POs that are not yet fully completed
open_pos_df = all_pos_df[all_pos_df['GRN Status'] != 'GRN Completed']
po_numbers = [""] + sorted(open_pos_df['Po No.'].unique().tolist())

col1, col2 = st.columns(2)
with col1:
    selected_po_number = st.selectbox(
        "Select an open Purchase Order:",
        options=po_numbers,
        key="grn_po_select"
    )

# --- NEW: Dynamic Vendor Selection ---
vendor_options = [""]
if selected_po_number:
    # Find all vendors associated with the selected PO number
    vendors_in_po = open_pos_df[open_pos_df['Po No.'] == selected_po_number]['Vendor Name'].unique().tolist()
    vendor_options = [""] + sorted(vendors_in_po)

with col2:
    selected_vendor = st.selectbox(
        "Select Vendor for this GRN:",
        options=vendor_options,
        key="grn_vendor_select",
        disabled=(not selected_po_number)
    )
# --- END NEW ---

if not selected_po_number or not selected_vendor:
    st.info("Select a PO and a Vendor to begin the GRN process."); st.stop()

# --- Display and Edit Line Items for Selected PO and Vendor ---
st.header(f"2. Enter Received Quantities for PO #{selected_po_number} from Vendor: {selected_vendor}")

# Filter the line items for the selected PO AND Vendor
po_line_items_df = all_pos_df[
    (all_pos_df['Po No.'] == selected_po_number) & 
    (all_pos_df['Vendor Name'] == selected_vendor)
]

# Unique key for session state based on PO and Vendor
session_key_suffix = f"{selected_po_number}_{selected_vendor}".replace(" ", "_")

if 'grn_editor_df' not in st.session_state or st.session_state.get('grn_current_key') != session_key_suffix:
    df_for_editor = po_line_items_df[['id', 'Msku Code', 'Category', 'Quantity']].copy()
    df_for_editor.rename(columns={'Quantity': 'Qty Ordered'}, inplace=True)
    
    df_for_editor['Actual Qty Received'] = df_for_editor['Qty Ordered']
    df_for_editor['Damage/Dust'] = 0
    df_for_editor['Missing'] = 0
    df_for_editor['Extra'] = 0
    
    st.session_state.grn_editor_df = df_for_editor
    st.session_state.grn_current_key = session_key_suffix

# Use st.data_editor for GRN input
edited_grn_df = st.data_editor(
    st.session_state.grn_editor_df,
    column_config={
        "id": None,
        "Msku Code": st.column_config.TextColumn(disabled=True),
        "Category": st.column_config.TextColumn(disabled=True),
        "Qty Ordered": st.column_config.NumberColumn(format="%d", disabled=True),
        "Actual Qty Received": st.column_config.NumberColumn("Actual Qty Rcvd", min_value=0, step=1, required=True),
        "Damage/Dust": st.column_config.NumberColumn(min_value=0, step=1, required=True),
        "Missing": st.column_config.NumberColumn(min_value=0, step=1, required=True),
        "Extra": st.column_config.NumberColumn(min_value=0, step=1, required=True),
    },
    hide_index=True,
    use_container_width=True,
    key=f"grn_editor_{session_key_suffix}"
)

# --- Final GRN Details and Submission ---
st.divider()
st.header("3. Finalize GRN")

col_final1, col_final2 = st.columns(2)
with col_final1:
    qc_date = st.date_input("Date of QC", value=date.today())
with col_final2:
    final_grn_status = st.selectbox(
        "Set GRN Status for these items:",
        options=["In-Process", "GRN Completed", "On Hold", "Pending"]
    )

if st.button("Submit GRN to Baserow", type="primary"):
    with st.spinner("Updating Purchase Order with GRN details..."):
        
        update_success_count = 0
        total_items_to_update = len(edited_grn_df)
        
        for index, row in edited_grn_df.iterrows():
            row_id = row['id']
            
            update_data = {
                "Actual Qty Received": str(row['Actual Qty Received']),
                "Damage/Dust": str(row['Damage/Dust']),
                "Missing": str(row['Missing']),
                "Extra": str(row['Extra']),
                "Date Of Qc": qc_date.strftime('%d-%b-%Y'),
                "GRN Status": final_grn_status
            }
            
            if update_po_line_item(fetcher, po_table_id, row_id, update_data):
                update_success_count += 1
            else:
                st.error(f"Failed to update GRN for MSKU: {row['Msku Code']} (Row ID: {row_id})")
        
        if update_success_count == total_items_to_update:
            st.success(f"Successfully updated {update_success_count} line items for PO #{selected_po_number}!")
            st.balloons()
            st.session_state.pop('grn_all_pos_df', None)
            st.session_state.pop('grn_editor_df', None)
            st.session_state.pop('grn_current_key', None)
            st.rerun()
        else:
            st.error("Some items failed to update. Please check the logs and Baserow.")