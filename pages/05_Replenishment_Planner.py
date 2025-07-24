# RMS/pages/06_Replenishment_Planner.py
import streamlit as st
import pandas as pd
from datetime import date
import os
import sys
import numpy as np
from datetime import datetime, timedelta
from utils.pdf_generator import generate_replenishment_pdf

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
from analytics_dashboard.data_loader import load_and_cache_analytics_data
from analytics_dashboard.kpi_calculations import (
    process_sales_data_for_analytics as get_sales_data,
    get_current_inventory,
    calculate_sales_velocity
)
from replenishment.replenishment_logic import calculate_replenishment_data
from po_module.po_management import get_all_pos, get_distinct_values # Import this helper

import logging
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Replenishment Planner - RMS", layout="wide")
st.title("🚚 Replenishment Planner")

# --- Initialize Tools & Load Data ---
@st.cache_resource
def get_analytics_fetcher_replen():
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        return fetcher
    except Exception as e: st.error(f"Error initializing Baserow connection: {e}"); return None

fetcher = get_analytics_fetcher_replen()
if not fetcher: st.error("Failed to initialize Baserow connection."); st.stop()

# Get all table IDs
processed_sales_table_id = APP_CONFIG['baserow'].get('processed_sales_data_table_id')
inventory_table_id = APP_CONFIG['baserow'].get('inventory_table_id')
category_table_id = APP_CONFIG['baserow'].get('category_table_id')
catalogue_table_id = APP_CONFIG['baserow'].get('catalogue_table_id') 
po_table_id = APP_CONFIG['baserow'].get('purchase_orders_table_id') # For vendor list

if not all([processed_sales_table_id, inventory_table_id, category_table_id, catalogue_table_id, po_table_id]):
    st.error("All table IDs (sales, inventory, category, catalogue, purchase_orders) must be configured.")
    st.stop()

# Load all data into session state
load_and_cache_analytics_data(fetcher, processed_sales_table_id, inventory_table_id, category_table_id, catalogue_table_id)
all_sales_df = st.session_state.get('analytics_sales_df')
all_inventory_df = st.session_state.get('analytics_inventory_df')
all_category_df = st.session_state.get('analytics_category_df')
all_catalogue_df = st.session_state.get('analytics_catalogue_df')
# Load PO data to get vendor list
if 'po_all_pos_df' not in st.session_state:
    st.session_state.po_all_pos_df = get_all_pos(fetcher, po_table_id)
all_pos_df = st.session_state.get('po_all_pos_df', pd.DataFrame())


# --- Initialize Session State for this page ---
if 'replenishment_overview_df' not in st.session_state:
    st.session_state.replenishment_overview_df = None
if 'replenishment_plan_draft_df' not in st.session_state:
    st.session_state.replenishment_plan_draft_df = pd.DataFrame(columns=['MSKU', 'Category', 'HSN Code', 'Image URL', 'Order Quantity', 'Notes'])

# --- Sidebar Controls ---
# ... (sidebar controls remain the same) ...
st.sidebar.header("Replenishment Parameters")
velocity_period = st.sidebar.selectbox("Calculate Sales Velocity based on last:", options=[7, 14, 30, 60, 90], index=2, format_func=lambda x: f"{x} days")
st.sidebar.subheader("Default Parameters")
default_lead_time = st.sidebar.number_input("Supplier Lead Time (days)", min_value=1, value=30)
default_stock_cover = st.sidebar.number_input("Desired Stock Cover (days)", min_value=0, value=15)
default_order_cycle = st.sidebar.number_input("Order Cycle (days)", min_value=1, value=30)
default_moq = st.sidebar.number_input("Minimum Order Quantity (MOQ)", min_value=0, value=0)

# --- Main Page Logic ---
if all_sales_df is None or all_inventory_df is None:
    st.warning("Sales or Inventory data is not available.")
else:
    if st.button("Calculate Replenishment Overview", type="primary"):
        with st.spinner("Calculating..."):
            # ... (calculation logic is the same as before) ...
            end_date = all_sales_df['Sale Date'].max()
            start_date = end_date - timedelta(days=velocity_period - 1)
            sales_df_for_velocity = get_sales_data(all_sales_df, start_date, end_date)
            sales_velocity = calculate_sales_velocity(sales_df_for_velocity, days_period=velocity_period)
            current_inventory = get_current_inventory(all_inventory_df)
            lead_times = {'default': default_lead_time}; stock_cover_days = {'default': default_stock_cover}
            order_cycle_days = {'default': default_order_cycle}; moqs = {'default': default_moq}
            replenishment_plan_df = calculate_replenishment_data(current_inventory, sales_velocity, lead_times, stock_cover_days, order_cycle_days, moqs)
            if all_category_df is not None:
                replenishment_plan_df = pd.merge(replenishment_plan_df, all_category_df, on='MSKU', how='left')
                replenishment_plan_df['Category'].fillna('Uncategorized', inplace=True)
            if all_catalogue_df is not None:
                replenishment_plan_df = pd.merge(replenishment_plan_df, all_catalogue_df, on='MSKU', how='left')
                replenishment_plan_df['Image URL'].fillna('', inplace=True)
            
            st.session_state.replenishment_overview_df = replenishment_plan_df

# --- SECTION 1: REPLENISHMENT OVERVIEW ---
st.header("Replenishment Overview")
overview_df = st.session_state.get('replenishment_overview_df')

if overview_df is not None and not overview_df.empty:
    # ... (Filter section remains the same) ...
    col_filter1, col_filter2 = st.columns(2)
    with col_filter1:
        all_categories = ['All Categories'] + sorted(overview_df['Category'].unique().tolist())
        selected_category = st.selectbox("Filter by Category:", options=all_categories)
    with col_filter2:
        all_statuses = ['All Statuses'] + sorted(overview_df['Status'].unique().tolist())
        selected_status = st.selectbox("Filter by Status:", options=all_statuses)
    hide_zero_rows = st.checkbox("Hide items with zero inventory AND zero average daily sales", value=True)
    filtered_overview_df = overview_df.copy()
    if selected_category != 'All Categories': filtered_overview_df = filtered_overview_df[filtered_overview_df['Category'] == selected_category]
    if selected_status != 'All Statuses': filtered_overview_df = filtered_overview_df[filtered_overview_df['Status'] == selected_status]
    if hide_zero_rows:
        filtered_overview_df = filtered_overview_df[(filtered_overview_df['Current Inventory'] != 0) | (filtered_overview_df['Avg Daily Sales'] != 0)]

    filtered_overview_df['Select'] = False
    overview_display_cols = ['Select', 'Image URL', 'MSKU', 'Category', 'Status', 'Current Inventory', 'Avg Daily Sales', 'DOS', 'Reorder Point', 'Suggested Order Qty']
    
    edited_overview_df = st.data_editor(
        filtered_overview_df[overview_display_cols],
        column_config={
            "Select": st.column_config.CheckboxColumn(required=True),
            "Image URL": st.column_config.ImageColumn("Image"),
            "MSKU": st.column_config.TextColumn(disabled=True),
            "Category": st.column_config.TextColumn(disabled=True),
            "Status": st.column_config.TextColumn(disabled=True),
            "Current Inventory": st.column_config.NumberColumn("Current Inv.", disabled=True),
            "Avg Daily Sales": st.column_config.NumberColumn(format="%.2f", disabled=True),
            "DOS": st.column_config.NumberColumn("Days of Stock", format="%.1f", disabled=True),
            "Reorder Point": st.column_config.NumberColumn(disabled=True),
            "Suggested Order Qty": st.column_config.NumberColumn(disabled=True),
        },
        hide_index=True, use_container_width=True, key="overview_editor"
    )

    selected_rows = edited_overview_df[edited_overview_df['Select']]
    if st.button("Add Selected to Plan Draft", disabled=selected_rows.empty):
        for index, row in selected_rows.iterrows():
            if row['MSKU'] not in st.session_state.replenishment_plan_draft_df['MSKU'].values:
                # Get full data for the selected MSKU from the original overview_df
                full_row_data = overview_df[overview_df['MSKU'] == row['MSKU']].iloc[0]
                
                new_item = {
                    'MSKU': row['MSKU'],
                    'Category': row['Category'],
                    'HSN Code': full_row_data.get('HSN Code', ''),
                    'Image URL': row['Image URL'],
                    'Order Quantity': row['Suggested Order Qty'],
                    'Notes': '',
                    'Vendor Name': '', # Add a blank supplier field
                    'Unit Cost': 0.0, # Add a blank cost field
                    'Currency': 'USD' # Default currency
                }
                new_item_df = pd.DataFrame([new_item])
                st.session_state.replenishment_plan_draft_df = pd.concat(
                    [st.session_state.replenishment_plan_draft_df, new_item_df],
                    ignore_index=True
                )
        st.success(f"Added {len(selected_rows)} item(s) to the plan draft below.")
        st.rerun()

else:
    st.info("Click 'Calculate Replenishment Overview' to begin.")

st.divider()

# --- SECTION 2: REPLENISHMENT PLAN DRAFT ---
st.header("Replenishment Plan Draft")
draft_df = st.session_state.get('replenishment_plan_draft_df')

if draft_df is not None and not draft_df.empty:
    st.info("Assign a **Supplier** and confirm the **Unit Cost** and **Order Quantity** for each item.")
    
    # Get vendor options for the dropdown
    vendor_options = [""] + get_distinct_values(all_pos_df, 'Vendor Name')
    
# Draft table display
    edited_draft_df = st.data_editor(
        draft_df,
        column_config={
            "MSKU": st.column_config.TextColumn(disabled=True),
            "Category": st.column_config.TextColumn(disabled=True),
            "HSN Code": st.column_config.TextColumn(disabled=True),
            "Image URL": st.column_config.ImageColumn("Image"),
            "Order Quantity": st.column_config.NumberColumn(min_value=0, step=10, required=True),
            "Notes": st.column_config.TextColumn(width="large"),
            "Vendor Name": st.column_config.SelectboxColumn(options=vendor_options, required=True),
            "Unit Cost": st.column_config.NumberColumn(min_value=0.0, format="%.4f", required=True),
            "Currency": st.column_config.SelectboxColumn(options=["USD", "CNY", "INR"], required=True)
        },
        hide_index=True,
        use_container_width=True,
        key="draft_editor",
        num_rows=""
    )

    # Add an explicit "Save Changes" button
    if st.button("Save Draft Changes"):
        st.session_state.replenishment_plan_draft_df = edited_draft_df
        st.success("Draft updated successfully.")

    # Optional: Warn users to click Save
    st.caption("ℹ️ Make sure to click **'Save Draft Changes'** after editing to avoid losing your inputs.")

    
    st.session_state.replenishment_plan_draft_df = edited_draft_df

    # --- NEW: "Add to PO Drafts" Button ---
    st.markdown("---")
    if st.button("Send Plan to PO Workspace", type="primary"):
        draft_to_process = edited_draft_df.copy()
        
        if draft_to_process['Vendor Name'].eq('').any():
            st.error("Please assign a Vendor Name to every item in the draft before proceeding.")
        else:
            with st.spinner("Preparing PO Draft..."):
                # Initialize the PO creation draft state if it doesn't exist
                if 'po_draft_items' not in st.session_state:
                    st.session_state.po_draft_items = []

                # Convert the DataFrame of items into a list of dicts for the PO page
                items_list = []
                for i, item_row in draft_to_process.iterrows():
                    qty = item_row['Order Quantity']
                    price = item_row['Unit Cost']
                    currency = item_row['Currency']
                    total_foreign_amt = float(qty) * float(price)
                    
                    items_list.append({
                        "MSKU": item_row['MSKU'],
                        "Vendor Name": item_row['Vendor Name'],
                        "Forwarder": "",  # Default to blank, user will select on PO page
                        "Shipment Route": "Air", # Default to "Air"
                        "Arrive by": date.today() + timedelta(days=45), # Default to 45 days from now
                        "Category": item_row['Category'],
                        "Quantity": qty,
                        "Currency": currency,
                        "per pcs price usd": price,
                        "USD Amt": total_foreign_amt,
                        "INR Amt": 0.0, # This will be recalculated on the PO page
                        "HSN Code": item_row['HSN Code']
                    },)

                # Append these items to the main PO draft state
                st.session_state.po_draft_items.extend(items_list)
                
                st.success(f"Successfully sent {len(items_list)} items to the PO Workspace!")
                st.info("Navigate to the 'Create Purchase Order' page to finalize.")
                # Clear the replenishment draft after processing
                st.session_state.replenishment_plan_draft_df = pd.DataFrame(columns=draft_df.columns)
                st.rerun()

    st.subheader("Download Plan Draft")
    col_dl1, col_dl2 = st.columns(2)
    with col_dl1:
        csv_export_df = edited_draft_df.copy()
        st.download_button("Download as CSV", csv_export_df.to_csv(index=False).encode('utf-8'),
                           f"replenishment_plan_draft_{date.today().strftime('%Y%m%d')}.csv", "text/csv",
                           key='download-draft-csv', use_container_width=True)
    with col_dl2:
        if st.button("Generate & Download as PDF", key='generate-draft-pdf-btn', use_container_width=True):
            with st.spinner("Generating PDF..."):
                pdf_bytes = generate_replenishment_pdf(edited_draft_df)
                if pdf_bytes:
                    st.download_button(label="PDF Ready! Click to Download", data=pdf_bytes,
                                       file_name=f"replenishment_plan_draft_{date.today().strftime('%Y%m%d')}.pdf",
                                       mime="application/pdf", key='download-draft-pdf')
                else: st.error("Failed to generate PDF.")
else:
    st.info("No items have been added to the replenishment plan draft yet. Select items from the overview above.")