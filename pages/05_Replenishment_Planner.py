# # RMS/pages/06_Replenishment_Planner.py
# import streamlit as st
# import pandas as pd
# from datetime import date
# import os
# import sys
# import numpy as np
# from datetime import datetime, timedelta
# from utils.pdf_generator import generate_replenishment_pdf

# project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
# if project_root not in sys.path: sys.path.insert(0, project_root)

# from utils.config_loader import APP_CONFIG
# from data_processing.baserow_fetcher import BaserowFetcher
# from analytics_dashboard.data_loader import load_and_cache_analytics_data
# from analytics_dashboard.kpi_calculations import (
#     process_sales_data_for_analytics as get_sales_data,
#     get_current_inventory,
#     calculate_sales_velocity
# )
# from replenishment.replenishment_logic import calculate_replenishment_data
# from po_module.po_management import get_all_pos, get_distinct_values # Import this helper

# import logging
# logger = logging.getLogger(__name__)

# st.set_page_config(page_title="Replenishment Planner - RMS", layout="wide")
# st.title("🚚 Replenishment Planner")

# # --- Initialize Tools & Load Data ---
# @st.cache_resource
# def get_analytics_fetcher_replen():
#     try:
#         fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
#         return fetcher
#     except Exception as e: st.error(f"Error initializing Baserow connection: {e}"); return None

# fetcher = get_analytics_fetcher_replen()
# if not fetcher: st.error("Failed to initialize Baserow connection."); st.stop()

# # Get all table IDs
# processed_sales_table_id = APP_CONFIG['baserow'].get('processed_sales_data_table_id')
# inventory_table_id = APP_CONFIG['baserow'].get('inventory_table_id')
# category_table_id = APP_CONFIG['baserow'].get('category_table_id')
# catalogue_table_id = APP_CONFIG['baserow'].get('catalogue_table_id') 
# po_table_id = APP_CONFIG['baserow'].get('purchase_orders_table_id') # For vendor list

# if not all([processed_sales_table_id, inventory_table_id, category_table_id, catalogue_table_id, po_table_id]):
#     st.error("All table IDs (sales, inventory, category, catalogue, purchase_orders) must be configured.")
#     st.stop()

# # Load all data into session state
# load_and_cache_analytics_data(fetcher, processed_sales_table_id, inventory_table_id, category_table_id, catalogue_table_id)
# all_sales_df = st.session_state.get('analytics_sales_df')
# all_inventory_df = st.session_state.get('analytics_inventory_df')
# all_category_df = st.session_state.get('analytics_category_df')
# all_catalogue_df = st.session_state.get('analytics_catalogue_df')
# # Load PO data to get vendor list
# if 'po_all_pos_df' not in st.session_state:
#     st.session_state.po_all_pos_df = get_all_pos(fetcher, po_table_id)
# all_pos_df = st.session_state.get('po_all_pos_df', pd.DataFrame())


# # --- Initialize Session State for this page ---
# if 'replenishment_overview_df' not in st.session_state:
#     st.session_state.replenishment_overview_df = None
# if 'replenishment_plan_draft_df' not in st.session_state:
#     st.session_state.replenishment_plan_draft_df = pd.DataFrame(columns=['MSKU', 'Category', 'HSN Code', 'Image URL', 'Order Quantity', 'Notes'])

# # --- Sidebar Controls ---
# # ... (sidebar controls remain the same) ...
# st.sidebar.header("Replenishment Parameters")
# velocity_period = st.sidebar.selectbox("Calculate Sales Velocity based on last:", options=[7, 14, 30, 60, 90], index=2, format_func=lambda x: f"{x} days")
# st.sidebar.subheader("Default Parameters")
# default_lead_time = st.sidebar.number_input("Supplier Lead Time (days)", min_value=1, value=30)
# default_stock_cover = st.sidebar.number_input("Desired Stock Cover (days)", min_value=0, value=15)
# default_order_cycle = st.sidebar.number_input("Order Cycle (days)", min_value=1, value=30)
# default_moq = st.sidebar.number_input("Minimum Order Quantity (MOQ)", min_value=0, value=0)

# # --- Main Page Logic ---
# if all_sales_df is None or all_inventory_df is None:
#     st.warning("Sales or Inventory data is not available.")
# else:
#     if st.button("Calculate Replenishment Overview", type="primary"):
#         with st.spinner("Calculating..."):
#             # ... (calculation logic is the same as before) ...
#             end_date = all_sales_df['Sale Date'].max()
#             start_date = end_date - timedelta(days=velocity_period - 1)
#             sales_df_for_velocity = get_sales_data(all_sales_df, start_date, end_date)
#             sales_velocity = calculate_sales_velocity(sales_df_for_velocity, days_period=velocity_period)
#             current_inventory = get_current_inventory(all_inventory_df)
#             lead_times = {'default': default_lead_time}; stock_cover_days = {'default': default_stock_cover}
#             order_cycle_days = {'default': default_order_cycle}; moqs = {'default': default_moq}
#             replenishment_plan_df = calculate_replenishment_data(current_inventory, sales_velocity, lead_times, stock_cover_days, order_cycle_days, moqs)
#             if all_category_df is not None:
#                 replenishment_plan_df = pd.merge(replenishment_plan_df, all_category_df, on='MSKU', how='left')
#                 replenishment_plan_df['Category'].fillna('Uncategorized', inplace=True)
#             if all_catalogue_df is not None:
#                 replenishment_plan_df = pd.merge(replenishment_plan_df, all_catalogue_df, on='MSKU', how='left')
#                 replenishment_plan_df['Image URL'].fillna('', inplace=True)
            
#             st.session_state.replenishment_overview_df = replenishment_plan_df

# # --- SECTION 1: REPLENISHMENT OVERVIEW ---
# st.header("Replenishment Overview")
# overview_df = st.session_state.get('replenishment_overview_df')

# if overview_df is not None and not overview_df.empty:
#     # ... (Filter section remains the same) ...
#     col_filter1, col_filter2 = st.columns(2)
#     with col_filter1:
#         all_categories = ['All Categories'] + sorted(overview_df['Category'].unique().tolist())
#         selected_category = st.selectbox("Filter by Category:", options=all_categories)
#     with col_filter2:
#         all_statuses = ['All Statuses'] + sorted(overview_df['Status'].unique().tolist())
#         selected_status = st.selectbox("Filter by Status:", options=all_statuses)
#     hide_zero_rows = st.checkbox("Hide items with zero inventory AND zero average daily sales", value=True)
#     filtered_overview_df = overview_df.copy()
#     if selected_category != 'All Categories': filtered_overview_df = filtered_overview_df[filtered_overview_df['Category'] == selected_category]
#     if selected_status != 'All Statuses': filtered_overview_df = filtered_overview_df[filtered_overview_df['Status'] == selected_status]
#     if hide_zero_rows:
#         filtered_overview_df = filtered_overview_df[(filtered_overview_df['Current Inventory'] != 0) | (filtered_overview_df['Avg Daily Sales'] != 0)]

#     filtered_overview_df['Select'] = False
#     overview_display_cols = ['Select', 'Image URL', 'MSKU', 'Category', 'Status', 'Current Inventory', 'Avg Daily Sales', 'DOS', 'Reorder Point', 'Suggested Order Qty']
    
#     edited_overview_df = st.data_editor(
#         filtered_overview_df[overview_display_cols],
#         column_config={
#             "Select": st.column_config.CheckboxColumn(required=True),
#             "Image URL": st.column_config.ImageColumn("Image"),
#             "MSKU": st.column_config.TextColumn(disabled=True),
#             "Category": st.column_config.TextColumn(disabled=True),
#             "Status": st.column_config.TextColumn(disabled=True),
#             "Current Inventory": st.column_config.NumberColumn("Current Inv.", disabled=True),
#             "Avg Daily Sales": st.column_config.NumberColumn(format="%.2f", disabled=True),
#             "DOS": st.column_config.NumberColumn("Days of Stock", format="%.1f", disabled=True),
#             "Reorder Point": st.column_config.NumberColumn(disabled=True),
#             "Suggested Order Qty": st.column_config.NumberColumn(disabled=True),
#         },
#         hide_index=True, use_container_width=True, key="overview_editor"
#     )

#     selected_rows = edited_overview_df[edited_overview_df['Select']]
#     if st.button("Add Selected to Plan Draft", disabled=selected_rows.empty):
#         for index, row in selected_rows.iterrows():
#             if row['MSKU'] not in st.session_state.replenishment_plan_draft_df['MSKU'].values:
#                 # Get full data for the selected MSKU from the original overview_df
#                 full_row_data = overview_df[overview_df['MSKU'] == row['MSKU']].iloc[0]
                
#                 new_item = {
#                     'MSKU': row['MSKU'],
#                     'Category': row['Category'],
#                     'HSN Code': full_row_data.get('HSN Code', ''),
#                     'Image URL': row['Image URL'],
#                     'Order Quantity': row['Suggested Order Qty'],
#                     'Notes': '',
#                     'Vendor Name': '', # Add a blank supplier field
#                     'Unit Cost': 0.0, # Add a blank cost field
#                     'Currency': 'USD' # Default currency
#                 }
#                 new_item_df = pd.DataFrame([new_item])
#                 st.session_state.replenishment_plan_draft_df = pd.concat(
#                     [st.session_state.replenishment_plan_draft_df, new_item_df],
#                     ignore_index=True
#                 )
#         st.success(f"Added {len(selected_rows)} item(s) to the plan draft below.")
#         st.rerun()

# else:
#     st.info("Click 'Calculate Replenishment Overview' to begin.")

# st.divider()

# # --- SECTION 2: REPLENISHMENT PLAN DRAFT ---
# st.header("Replenishment Plan Draft")
# draft_df = st.session_state.get('replenishment_plan_draft_df')

# if draft_df is not None and not draft_df.empty:
#     st.info("Assign a **Supplier** and confirm the **Unit Cost** and **Order Quantity** for each item.")
    
#     # Get vendor options for the dropdown
#     vendor_options = [""] + get_distinct_values(all_pos_df, 'Vendor Name')
    
# # Draft table display
#     edited_draft_df = st.data_editor(
#         draft_df,
#         column_config={
#             "MSKU": st.column_config.TextColumn(disabled=True),
#             "Category": st.column_config.TextColumn(disabled=True),
#             "HSN Code": st.column_config.TextColumn(disabled=True),
#             "Image URL": st.column_config.ImageColumn("Image"),
#             "Order Quantity": st.column_config.NumberColumn(min_value=0, step=10, required=True),
#             "Notes": st.column_config.TextColumn(width="large"),
#             "Vendor Name": st.column_config.SelectboxColumn(options=vendor_options, required=True),
#             "Unit Cost": st.column_config.NumberColumn(min_value=0.0, format="%.4f", required=True),
#             "Currency": st.column_config.SelectboxColumn(options=["USD", "CNY", "INR"], required=True)
#         },
#         hide_index=True,
#         use_container_width=True,
#         key="draft_editor",
#         num_rows=""
#     )

#     # Add an explicit "Save Changes" button
#     if st.button("Save Draft Changes"):
#         st.session_state.replenishment_plan_draft_df = edited_draft_df
#         st.success("Draft updated successfully.")

#     # Optional: Warn users to click Save
#     st.caption("ℹ️ Make sure to click **'Save Draft Changes'** after editing to avoid losing your inputs.")

    
#     st.session_state.replenishment_plan_draft_df = edited_draft_df

#     # --- NEW: "Add to PO Drafts" Button ---
#     st.markdown("---")
#     if st.button("Send Plan to PO Workspace", type="primary"):
#         draft_to_process = edited_draft_df.copy()
        
#         if draft_to_process['Vendor Name'].eq('').any():
#             st.error("Please assign a Vendor Name to every item in the draft before proceeding.")
#         else:
#             with st.spinner("Preparing PO Draft..."):
#                 # Initialize the PO creation draft state if it doesn't exist
#                 if 'po_draft_items' not in st.session_state:
#                     st.session_state.po_draft_items = []

#                 # Convert the DataFrame of items into a list of dicts for the PO page
#                 items_list = []
#                 for i, item_row in draft_to_process.iterrows():
#                     qty = item_row['Order Quantity']
#                     price = item_row['Unit Cost']
#                     currency = item_row['Currency']
#                     total_foreign_amt = float(qty) * float(price)
                    
#                     items_list.append({
#                         "MSKU": item_row['MSKU'],
#                         "Vendor Name": item_row['Vendor Name'],
#                         "Forwarder": "",  # Default to blank, user will select on PO page
#                         "Shipment Route": "Air", # Default to "Air"
#                         "Arrive by": date.today() + timedelta(days=45), # Default to 45 days from now
#                         "Category": item_row['Category'],
#                         "Quantity": qty,
#                         "Currency": currency,
#                         "per pcs price usd": price,
#                         "USD Amt": total_foreign_amt,
#                         "INR Amt": 0.0, # This will be recalculated on the PO page
#                         "HSN Code": item_row['HSN Code']
#                     },)

#                 # Append these items to the main PO draft state
#                 st.session_state.po_draft_items.extend(items_list)
                
#                 st.success(f"Successfully sent {len(items_list)} items to the PO Workspace!")
#                 st.info("Navigate to the 'Create Purchase Order' page to finalize.")
#                 # Clear the replenishment draft after processing
#                 st.session_state.replenishment_plan_draft_df = pd.DataFrame(columns=draft_df.columns)
#                 st.rerun()

#     st.subheader("Download Plan Draft")
#     col_dl1, col_dl2 = st.columns(2)
#     with col_dl1:
#         csv_export_df = edited_draft_df.copy()
#         st.download_button("Download as CSV", csv_export_df.to_csv(index=False).encode('utf-8'),
#                            f"replenishment_plan_draft_{date.today().strftime('%Y%m%d')}.csv", "text/csv",
#                            key='download-draft-csv', use_container_width=True)
#     with col_dl2:
#         if st.button("Generate & Download as PDF", key='generate-draft-pdf-btn', use_container_width=True):
#             with st.spinner("Generating PDF..."):
#                 pdf_bytes = generate_replenishment_pdf(edited_draft_df)
#                 if pdf_bytes:
#                     st.download_button(label="PDF Ready! Click to Download", data=pdf_bytes,
#                                        file_name=f"replenishment_plan_draft_{date.today().strftime('%Y%m%d')}.pdf",
#                                        mime="application/pdf", key='download-draft-pdf')
#                 else: st.error("Failed to generate PDF.")
# else:
#     st.info("No items have been added to the replenishment plan draft yet. Select items from the overview above.")


# RMS/pages/06_Replenishment_Planner.py
import streamlit as st
import pandas as pd
from datetime import date
import os
import sys
import numpy as np
from datetime import datetime, timedelta
import json
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
from po_module.po_management import get_all_pos, get_distinct_values, get_open_po_data
from packaging_module.packaging_logic import (
    process_outbound_to_daily_consumption,
    calculate_packaging_velocity,
    calculate_packaging_replenishment
)
from replenishment_engine.core import calculate_sales_stats, run_replenishment_engine
from po_module.po_management import get_all_pos, get_distinct_values, get_open_po_data, get_last_order_dates, get_last_landed_costs
from packaging_module.packaging_logic import (
    process_outbound_to_daily_consumption,
    calculate_packaging_velocity,
    calculate_packaging_replenishment
)


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
po_table_id = APP_CONFIG['baserow'].get('purchase_orders_table_id')
outbound_table_id = APP_CONFIG['baserow'].get('automated_outbound_table_id')
packaging_inv_table_id = APP_CONFIG['baserow'].get('packaging_inventory_table_id')

if not all([processed_sales_table_id, inventory_table_id, category_table_id, catalogue_table_id, po_table_id]):
    st.error("All table IDs (sales, inventory, category, catalogue, purchase_orders) must be configured.")
    st.stop()

# Load all data into session state
load_and_cache_analytics_data(fetcher, processed_sales_table_id, inventory_table_id, category_table_id, catalogue_table_id , outbound_table_id, packaging_inv_table_id)
all_sales_df = st.session_state.get('analytics_sales_df')
all_inventory_df = st.session_state.get('analytics_inventory_df')
all_category_df = st.session_state.get('analytics_category_df')
all_catalogue_df = st.session_state.get('analytics_catalogue_df')
if 'po_all_pos_df' not in st.session_state:
    st.session_state.po_all_pos_df = get_all_pos(fetcher, po_table_id)
all_pos_df = st.session_state.get('po_all_pos_df', pd.DataFrame())
packaging_outbound_df = st.session_state.get('packaging_outbound_df')
packaging_inventory_df = st.session_state.get('packaging_inventory_df')

# --- Initialize Session State for this page ---
if 'replenishment_overview_df' not in st.session_state: st.session_state.replenishment_overview_df = None
if 'packaging_overview_df' not in st.session_state: st.session_state.packaging_overview_df = None
if 'replenishment_plan_draft_df' not in st.session_state:
    st.session_state.replenishment_plan_draft_df = pd.DataFrame(columns=['MSKU', 'Category', 'HSN Code', 'Image URL', 'Order Quantity', 'Notes', 'Vendor Name', 'Unit Cost', 'Currency', 'Shipment Route'])
if 'last_params_used' not in st.session_state:
    st.session_state.last_params_used = {}

product_tab, packaging_tab = st.tabs(["📦 Product Replenishment", "🗳️ Packaging Replenishment"])

with product_tab:
    # --- Popover for Parameters ---
    replen_params = APP_CONFIG.get('replenishment_parameters', {})
    if 'param_overrides' not in st.session_state:
        st.session_state.param_overrides = {}

    with st.popover("⚙️ Edit Replenishment Parameters"):
        st.markdown("### Global Settings")
        st.info("These settings are loaded from your Google Sheet. You can override them here for this session only.")
        
        p_col1, p_col2 = st.columns(2)
        with p_col1:
            st.markdown("**Seasonal**")
            st.session_state.param_overrides['is_peak_season'] = st.toggle("Peak Season Mode", value=replen_params.get('is_peak_season', False))
            if st.session_state.param_overrides['is_peak_season']:
                st.session_state.param_overrides['peak_season_multiplier'] = st.slider("Peak Season Multiplier", 1.0, 3.0, replen_params.get('peak_season_multiplier', 1.5), 0.1)
                st.session_state.param_overrides['holiday_buffer_days'] = st.number_input("Holiday Buffer (days)", value=replen_params.get('holiday_buffer_days', 5))
            st.markdown("**Lead Times**")
            st.session_state.param_overrides['air_transit_days'] = st.number_input("Air Transit (days)", value=replen_params.get('air_transit_days', 5))
            st.session_state.param_overrides['sea_transit_days'] = st.number_input("Sea Transit (days)", value=replen_params.get('sea_transit_days', 30))
            st.session_state.param_overrides['admin_days'] = st.number_input("Admin/Processing (days)", value=replen_params.get('admin_days', 2))
        with p_col2:
            st.markdown("**Buffers & Thresholds**")
            st.session_state.param_overrides['review_period_days'] = st.number_input("Review Period (days)", value=replen_params.get('review_period_days', 15))
            st.session_state.param_overrides['danger_threshold_days'] = st.number_input("Danger Threshold (days)", value=replen_params.get('danger_threshold_days', 7))
            st.session_state.param_overrides['base_safety_buffer_pct'] = st.number_input("Base Safety Buffer (%)", value=replen_params.get('base_safety_buffer_pct', 10))
            st.session_state.param_overrides['max_safety_buffer_pct'] = st.number_input("Max Safety Buffer (%)", value=replen_params.get('max_safety_buffer_pct', 30))
            st.session_state.param_overrides['air_bridge_buffer_pct'] = st.number_input("Air Bridge Buffer (%)", value=replen_params.get('air_bridge_buffer_pct', 5))
            st.session_state.param_overrides['non_focused_multiplier'] = st.number_input("Non-Focused Multiplier", value=replen_params.get('non_focused_multiplier', 1.5))
            st.session_state.param_overrides['overstock_days'] = st.number_input("Overstock Threshold (days)", value=replen_params.get('overstock_days', 60))
            st.session_state.param_overrides['dead_stock_days'] = st.number_input("Dead Stock Threshold (days)", value=replen_params.get('dead_stock_days', 60))

    # --- Main Page Logic ---
    if all_sales_df is None or all_inventory_df is None or all_pos_df is None or all_category_df is None:
        st.warning("Sales, Inventory, PO, or Category data is not available.")
    else:
        # --- NEW: Smart Recalculation Logic ---
        # Combine base params with overrides to get the current set of parameters
        current_params = replen_params.copy()
        current_params.update(st.session_state.param_overrides)
        
        # Check if the main button was clicked OR if parameters have changed since the last run
        run_button_clicked = st.button("Run Advanced Replenishment Engine", type="primary", key="run_advanced_replen")
        params_have_changed = (st.session_state.last_params_used != current_params)
        
        # We run the engine if the button is clicked, OR if params changed AND a table already exists
        # (so it doesn't run automatically on the very first page load)
        if run_button_clicked or (params_have_changed and st.session_state.replenishment_overview_df is not None):
            if params_have_changed and not run_button_clicked:
                st.info("Parameters changed, automatically recalculating...")

            with st.spinner("Running advanced replenishment calculations..."):
                max_date = all_sales_df['Sale Date'].max()
                min_date = max_date - timedelta(days=60)
                daily_sales_for_stats = get_sales_data(all_sales_df, min_date, max_date)
                
                sales_stats = calculate_sales_stats(daily_sales_for_stats , sales_history_days=60)
                open_po_data = get_open_po_data(all_pos_df)
                last_order_dates = get_last_order_dates(all_pos_df)
                
                products_df = pd.merge(all_inventory_df, all_category_df, on='MSKU', how='outer')
                products_df = pd.merge(products_df, all_catalogue_df, on='MSKU', how='left')
                products_df['Image URL'].fillna('', inplace=True)
                if 'Product Type' in products_df.columns:
                    products_df = products_df[products_df['Product Type'] != 'Discontinued']

                replenishment_overview_df = run_replenishment_engine(products_df, sales_stats, open_po_data,last_order_dates, current_params)
                st.session_state.replenishment_overview_df = replenishment_overview_df
                
                # Store the parameters that were just used for the calculation
                st.session_state.last_params_used = current_params
                st.rerun()
                
    # --- SECTION 1: REPLENISHMENT OVERVIEW ---
    st.header("Replenishment Overview")
    overview_df = st.session_state.get('replenishment_overview_df')

    if overview_df is not None and not overview_df.empty:
        overview_tab, on_order_tab = st.tabs(["📊 Overview Table", "🔍 On-Order Details"])

        with overview_tab:
            st.markdown("##### Filters")
            filter_cols = st.columns([2, 2, 3])
            with filter_cols[0]:
                unique_categories = [str(cat) for cat in overview_df['Category'].unique().tolist() if pd.notna(cat)]
                all_categories = ['All Categories'] + sorted(unique_categories)
                selected_category = st.selectbox("Filter by Category:", options=all_categories, key="filter_cat_tab1")
            with filter_cols[1]:
                unique_statuses = [str(stat) for stat in overview_df['order_status'].unique().tolist() if pd.notna(stat)]
                all_statuses = ['All Statuses'] + sorted(unique_statuses)
                selected_status = st.selectbox("Filter by Replenishment Status:", options=all_statuses, key="filter_status_tab1")
            with filter_cols[2]:
                search_term = st.text_input("Search by MSKU:", placeholder="e.g., light, wand, cste_0330")
                
            cb_cols = st.columns(4)
            with cb_cols[0]:
                st.markdown("<br/>", unsafe_allow_html=True)
                hide_zero_inv = st.checkbox("Hide Zero Inventory", value=True)
            with cb_cols[1]:
                st.markdown("<br/>", unsafe_allow_html=True)
                hide_zero_sales = st.checkbox("Hide Zero Avg Sales", value=True)
            
            # --- THIS IS THE FIX ---
            # 1. Apply filters to a new DataFrame
            filtered_overview_df = overview_df.copy()
            if selected_category != 'All Categories':
                filtered_overview_df = filtered_overview_df[filtered_overview_df['Category'] == selected_category]
            if selected_status != 'All Statuses':
                filtered_overview_df = filtered_overview_df[filtered_overview_df['order_status'] == selected_status]
            if hide_zero_inv:
                filtered_overview_df = filtered_overview_df[filtered_overview_df['Current Inventory'] != 0]
            if hide_zero_sales:
                filtered_overview_df = filtered_overview_df[filtered_overview_df['avg_daily_sales'] != 0]

            if search_term:
                # Use .str.contains() for a case-insensitive regex search
                # na=False ensures that any MSKUs that are NaN don't cause an error
                filtered_overview_df = filtered_overview_df[
                    filtered_overview_df['MSKU'].str.contains(search_term, case=False, na=False, regex=True)
                ]

            # 2. Add the 'Select' column to the filtered DataFrame
            filtered_overview_df.insert(0, 'Select', False)

            show_all_columns = st.toggle("Show All Calculation Columns", key="toggle_all_cols", value=False)

            if show_all_columns:
                # Detailed view with all calculation columns
                # DETAILED DEBUG VIEW: Show everything
                display_cols = [
                    'Select', 'Image URL', 'MSKU','Category', 'Product Type', 'order_status', 'movement_status',
                    'Current Inventory', 'on_order_qty_air', 'on_order_qty_sea',
                    'avg_daily_sales', 'Total Sales (30d)', 'Total Sales (60d)', 'days_of_stock', 'days_since_last_sale',
                    'last_order_date', 'review_due',
                    'sea_target_stock', 'air_target_stock',
                    'sea_order_quantity', 'air_order_quantity', 'order_reason',
                    'calc_coverage_needed', 'calc_gap_to_target'
                    ]
            else:
                # STANDARD VIEW: The curated list
                display_cols = [
                    'Select', 'Image URL','MSKU', 'Category', 'Product Type', 'order_status', 
                    'movement_status', 'Current Inventory', 'avg_daily_sales', 'Total Sales (30d)', 'Total Sales (60d)',
                    'days_of_stock', 'on_order_qty_sea', 'on_order_qty_air',
                    'sea_order_quantity', 'air_order_quantity', 'order_reason'
                ]           
            # Add 'Select' to the detailed view if it's not there
            if show_all_columns and 'Select' not in display_cols:
                display_cols.insert(0, 'Select')

            # Prepare the DataFrame for display
            display_df = filtered_overview_df.copy()
            if 'Select' not in display_df.columns:
                display_df.insert(0, 'Select', False)

            # 3. Use the filtered DataFrame for the data_editor
            edited_overview_df = st.data_editor(
                filtered_overview_df,
                column_order=display_cols,
                column_config={
                    "Select": st.column_config.CheckboxColumn("Select"),
                    "Image URL": st.column_config.ImageColumn("Image"),
                    "MSKU": st.column_config.TextColumn("MSKU", disabled=True, width="medium"),
                    "Category": st.column_config.TextColumn("Category", disabled=True, width="small"),
                    "Product Type": st.column_config.TextColumn("Type", width="small", disabled=True),
                    "order_status": st.column_config.TextColumn("Order Status", width="medium", disabled=True),
                    "movement_status": st.column_config.TextColumn("Movement", width="small", disabled=True),
                    "Current Inventory": st.column_config.NumberColumn("Current Inv.", format="%d", disabled=True),
                    "avg_daily_sales": st.column_config.NumberColumn("Avg Daily Sales", format="%.2f", disabled=True),
                    "Total Sales (30d)": st.column_config.NumberColumn(format="%d", disabled=True),
                    "Total Sales (60d)": st.column_config.NumberColumn(format="%d", disabled=True),
                    "days_of_stock": st.column_config.NumberColumn("Days of Stock", format="%.1f", disabled=True),
                    "on_order_qty_sea": st.column_config.NumberColumn("On Order (Sea)", format="%d", disabled=True),
                    "on_order_qty_air": st.column_config.NumberColumn("On Order (Air)", format="%d", disabled=True),
                    "sea_order_quantity": st.column_config.NumberColumn("Suggest SEA Qty", format="%d", disabled=True),
                    "air_order_quantity": st.column_config.NumberColumn("Suggest AIR Qty", format="%d", disabled=True),
                    "order_reason": st.column_config.TextColumn("Order Reason", disabled=True),
                    "days_since_last_sale": st.column_config.NumberColumn("Days Since Sale", format="%d", disabled=True),
                    "last_order_date": st.column_config.DateColumn("Last Ordered", format="DD-MMM-YYYY", disabled=True),
                    "review_due": st.column_config.CheckboxColumn("Review Due?", disabled=True),
                    "sea_target_stock": st.column_config.NumberColumn("SEA Target", format="%d", disabled=True),
                    "air_target_stock": st.column_config.NumberColumn("AIR Target", format="%d", disabled=True),
                    "calc_coverage_needed": st.column_config.NumberColumn("Coverage Needed", format="%.1f", disabled=True),
                    "calc_gap_to_target": st.column_config.NumberColumn("Gap to Target", format="%.1f", disabled=True),
                },
                hide_index=True, use_container_width=True, key="simplified_overview_editor"
            )
            selected_rows = edited_overview_df[edited_overview_df['Select']]
            if st.button("Add Selected to Plan Draft", disabled=selected_rows.empty):
                cost_history_df = get_last_landed_costs(all_pos_df)
                items_added_count = 0
                for index, row in selected_rows.iterrows():
                    # Check if the MSKU is already in the draft to avoid duplicates
                    if row['MSKU'] not in st.session_state.replenishment_plan_draft_df['MSKU'].values:
                        items_added_count += 1
                        
                        # Get the full data for the selected MSKU from the original, unfiltered overview_df
                        # This ensures we have access to all columns, even if they were hidden in the editor view
                        full_row_data = overview_df[overview_df['MSKU'] == row['MSKU']].iloc[0]
                        
                        # Determine the suggested order quantities and primary shipment route
                        sea_qty = row.get('sea_order_quantity', 0)
                        air_qty = row.get('air_order_quantity', 0)

                        last_cost_info = cost_history_df[cost_history_df['MSKU'] == row['MSKU']]
                        last_unit_cost = last_cost_info['last_cost'].iloc[0] if not last_cost_info.empty else 0.0
                        
                        primary_route = 'Air'
                        if sea_qty > 0:
                            primary_route = 'Sea'
                        
                        order_qty = sea_qty + air_qty

                        new_item = {
                            'MSKU': row['MSKU'],
                            'Category': full_row_data.get('Category', ''),
                            'HSN Code': full_row_data.get('HSN Code', ''),
                            'Image URL': row.get('Image URL', ''),
                            'Total Sales (30d)': full_row_data.get('Total Sales (30d)', 0), # Add 30d sales
                            'Total Sales (60d)': full_row_data.get('Total Sales (60d)', 0), # Add 60d sales
                            'Order Quantity': order_qty, # Start with the suggested qty (can be 0)
                            'Notes': row.get('order_reason', ''), # Use the order_reason for notes
                            'Vendor Name': full_row_data.get('Supplier', ''),
                            'Unit Cost': last_unit_cost,
                            'Currency': 'INR',
                            'Shipment Route': primary_route
                        }
                        new_item_df = pd.DataFrame([new_item])
                        st.session_state.replenishment_plan_draft_df = pd.concat(
                            [st.session_state.replenishment_plan_draft_df, new_item_df],
                            ignore_index=True
                        )

                if items_added_count > 0:
                    st.success(f"Added {items_added_count} new item(s) to the plan draft below.")
                    st.rerun()
                else:
                    st.warning("Selected item(s) are already in the plan draft.")
        
        with on_order_tab:
            st.subheader("Details of Items Currently On Order")
            
            # --- FIX: Use the correct columns from the engine's output ---
            # The engine provides 'on_order_qty_air' and 'on_order_qty_sea'.
            on_order_df = overview_df[(overview_df['on_order_qty_air'] + overview_df['on_order_qty_sea']) > 0].copy()
            
            if on_order_df.empty:
                st.info("No items currently have open purchase orders.")
            else:
                # The get_open_po_data function now provides the detailed breakdown we need.
                # The 'PO_Details' column is now correctly merged by the engine.
                all_po_details = []
                for index, row in on_order_df.iterrows():
                    po_details_list = row.get('PO_Details', []) # The key is 'PO_Details'
                    if isinstance(po_details_list, list):
                        for detail_dict in po_details_list:
                            all_po_details.append({
                                'MSKU': row['MSKU'],
                                'Image URL': row.get('Image URL', ''),
                                'PO Number': detail_dict.get('Po No.'),
                                'Vendor': detail_dict.get('Vendor Name'),
                                'On Order Qty': detail_dict.get('Quantity'),
                                'Arrives By': detail_dict.get('Arrive by'),
                                'Shipment Route': detail_dict.get('Shipment Route', 'N/A')
                            })
                
                if all_po_details:
                    details_display_df = pd.DataFrame(all_po_details)
                    st.dataframe(
                        details_display_df,
                        column_config={
                            "Image URL": st.column_config.ImageColumn("Image"),
                            "On Order Qty": st.column_config.NumberColumn(format="%d")
                        },
                        hide_index=True,
                        use_container_width=True
                    )
                else:
                    st.info("No detailed PO information available for on-order items.")
    else:
        st.info("Click 'Run Advanced Replenishment Engine' to begin.")

with packaging_tab:
    st.sidebar.header("Packaging Parameters")
    pkg_velocity_period = st.sidebar.selectbox("Usage Velocity Period:", options=[7, 14, 30, 60, 90], index=2, format_func=lambda x: f"{x} days", key="pkg_velocity")
    st.sidebar.subheader("Packaging Defaults")
    pkg_default_lead_time = st.sidebar.number_input("Supplier Lead Time (days)", min_value=1, value=15, key="pkg_lead_time")
    pkg_default_stock_cover = st.sidebar.number_input("Stock Cover (days)", min_value=0, value=7, key="pkg_stock_cover")

    if packaging_outbound_df is None or packaging_inventory_df is None:
        st.warning("Packaging Consumption or Inventory data is not available.")
    else:
        if st.button("Calculate Packaging Replenishment Overview", type="primary", key="calc_pkg_replen"):
            with st.spinner("Calculating..."):
                daily_consumption_df = process_outbound_to_daily_consumption(packaging_outbound_df)
                packaging_velocity = calculate_packaging_velocity(daily_consumption_df, days_period=pkg_velocity_period)
                packaging_replen_df = calculate_packaging_replenishment(
                    packaging_inventory_df, packaging_velocity, pkg_default_lead_time, pkg_default_stock_cover
                )
                st.session_state.packaging_overview_df = packaging_replen_df

    st.header("Packaging Replenishment Overview")
    pkg_overview_df = st.session_state.get('packaging_overview_df')
    if pkg_overview_df is not None and not pkg_overview_df.empty:
        pkg_overview_df['Select'] = False
        pkg_overview_display_cols = ['Select', 'Material Name', 'Status', 'Current Inventory', 'Avg Daily Usage', 'DOS', 'Reorder Point', 'Suggested Order Qty']
        
        edited_pkg_overview_df = st.data_editor(
            pkg_overview_df[pkg_overview_display_cols],
            column_config={
                "Select": st.column_config.CheckboxColumn(required=True),
                "Material Name": st.column_config.TextColumn(disabled=True),
                "Status": st.column_config.TextColumn(disabled=True),
                "Current Inventory": st.column_config.NumberColumn("Current Stock", disabled=True),
                "Avg Daily Usage": st.column_config.NumberColumn(format="%.2f", disabled=True),
                "DOS": st.column_config.NumberColumn("Days of Stock", format="%.1f", disabled=True),
                "Reorder Point": st.column_config.NumberColumn(disabled=True),
                "Suggested Order Qty": st.column_config.NumberColumn(disabled=True),
            },
            hide_index=True, use_container_width=True, key="pkg_overview_editor"
        )

        selected_pkg_rows = edited_pkg_overview_df[edited_pkg_overview_df['Select']]
        if st.button("Add Selected Packaging to Plan Draft", disabled=selected_pkg_rows.empty):
            for index, row in selected_pkg_rows.iterrows():
                if row['Material Name'] not in st.session_state.replenishment_plan_draft_df['MSKU'].values:
                    new_item = {
                        'MSKU': row['Material Name'], # Use Material Name as the identifier
                        'Category': 'Packaging Material', # Assign a fixed category
                        'HSN Code': '', 'Image URL': '', # No image/HSN for packaging
                        'Order Quantity': row['Suggested Order Qty'],
                        'Notes': '', 'Vendor Name': '', 'Unit Cost': 0.0, 'Currency': 'INR' # Default to INR
                    }
                    new_item_df = pd.DataFrame([new_item])
                    st.session_state.replenishment_plan_draft_df = pd.concat(
                        [st.session_state.replenishment_plan_draft_df, new_item_df],
                        ignore_index=True
                    )
            st.success(f"Added {len(selected_pkg_rows)} packaging item(s) to the plan draft below.")
            st.rerun()
    else:
        st.info("Click 'Calculate Packaging Replenishment Overview' to begin.")

    st.divider()

# --- SECTION 2: REPLENISHMENT PLAN DRAFT ---
st.header("Replenishment Plan Draft")
draft_df = st.session_state.get('replenishment_plan_draft_df')

if draft_df is not None and not draft_df.empty:
    st.info("Assign a **Vendor Name** and confirm the **Unit Cost** and **Order Quantity** for each item.")
    vendor_options = [""] + get_distinct_values(all_pos_df, 'Vendor Name')

    # --- NEW: Pre-fetch cost history to merge with the draft ---
    cost_history_df = get_last_landed_costs(all_pos_df)
    if not cost_history_df.empty:
        # Merge cost history into the draft DataFrame for easy access
        draft_df_with_costs = pd.merge(draft_df, cost_history_df, on='MSKU', how='left')
    else:
        draft_df_with_costs = draft_df.copy()
        draft_df_with_costs['last_cost'] = np.nan
        draft_df_with_costs['second_last_cost'] = np.nan
        draft_df_with_costs['last_cost_date'] = pd.NaT

    # --- NEW: Create a new column for the popover display ---
    # We will use st.data_editor's ability to render markdown
    def create_cost_history_md(row):
        last_cost = row.get('last_cost')
        second_last_cost = row.get('second_last_cost')
        
        if pd.notna(last_cost):
            # The popover will be attached to this button-like text
            return "📜"
        return "" # Return empty string if no cost history

    draft_df_with_costs['Cost History'] = draft_df_with_costs.apply(create_cost_history_md, axis=1)
    
    edited_draft_df = st.data_editor(
        draft_df_with_costs,
        column_config={
            "MSKU": st.column_config.TextColumn(disabled=True), "Category": st.column_config.TextColumn(disabled=True),
            "HSN Code": st.column_config.TextColumn(disabled=True), "Image URL": st.column_config.ImageColumn("Image"),
            "Total Sales (30d)": st.column_config.NumberColumn(format="%d", disabled=True),
            "Total Sales (60d)": st.column_config.NumberColumn(format="%d", disabled=True),
            "Order Quantity": st.column_config.NumberColumn(min_value=0, step=10, required=True),
            "Notes": st.column_config.TextColumn(width="large"),
            "Vendor Name": st.column_config.SelectboxColumn(options=vendor_options, required=False),
            # "Cost History": st.column_config.TextColumn("📜", help="View last purchase costs for this item."),
            "Unit Cost": st.column_config.NumberColumn(min_value=0.0, format="%.4f", required=True),
            "Currency": st.column_config.SelectboxColumn(options=["USD", "CNY", "INR"], required=True),
            "last_cost": None,
            "second_last_cost": None,
            "last_cost_date": None,
            
        },
        # Define the display order
        column_order=[
            "Image URL", "MSKU", "Category", "Order Quantity", "Vendor Name", 
            "Currency", "Unit Cost", "Notes"
        ],

        hide_index=True, use_container_width=True, key="draft_editor", num_rows="dynamic"
    )

    # for i, row in edited_draft_df.iterrows():
    #     if row['Cost History'] == "📜":
    #         with st.popover(f"Cost History for {row['MSKU']}"):
    #             st.markdown(f"**Last Landed Cost:** ₹{row['last_cost']:.2f} (on {pd.to_datetime(row['last_cost_date']).strftime('%d-%b-%Y') if pd.notna(row['last_cost_date']) else 'N/A'})")
    #             if pd.notna(row['second_last_cost']):
    #                 st.markdown(f"**2nd Last Landed Cost:** ₹{row['second_last_cost']:.2f}")
    #             else:
    #                 st.caption("No prior cost history found.")

    # # --- RESTORED SAVE CHANGES LOGIC ---
    # original_df_for_compare = st.session_state.replenishment_plan_draft_df.reset_index(drop=True)
    # edited_df_for_compare = edited_draft_df.reset_index(drop=True)
    # if not original_df_for_compare.equals(edited_df_for_compare):
    #     st.caption("ℹ️ Changes detected in the draft.")
    #     if st.button("Save Draft Changes", type="primary"):
    #         st.session_state.replenishment_plan_draft_df = edited_draft_df
    #         st.success("Draft updated successfully.")
    #         st.rerun()
    # # --- END RESTORED LOGIC ---

    # --- NEW: Purchase History Section ---
    st.markdown("---")
    show_history = st.toggle("📜 Show Purchase History for Draft Items", value=False)

    if show_history:
        st.subheader("Purchase History for Items in Draft")
        draft_mskust = edited_draft_df['MSKU'].unique().tolist()
        
        # Filter the main PO history DataFrame for the relevant MSKUs
        history_df = all_pos_df[all_pos_df['Msku Code'].isin(draft_mskust)].copy()
        
        if history_df.empty:
            st.info("No previous purchase history found for the items in the current draft.")
        else:
            # Calculate Landed Cost for the historical records
            cost_cols = ['INR Amt', 'Carrying Amount', 'Porter Charges', 'Packaging and Other Charges', 'Quantity']
            for col in cost_cols:
                if col in history_df.columns:
                    history_df[col] = pd.to_numeric(history_df[col], errors='coerce').fillna(0)
                else:
                    history_df[col] = 0
            
            history_df['Final Cost'] = np.where(
                history_df['Quantity'] > 0,
                (history_df['INR Amt'] + history_df['Carrying Amount'] + history_df['Porter Charges']) / history_df['Quantity'],
                0
            )
            packaging_per_piece = np.where(
                history_df['Quantity'] > 0,
                history_df['Packaging and Other Charges'] / history_df['Quantity'],
                0
            )
            history_df['Landed Cost/pcs'] = history_df['Final Cost'] + packaging_per_piece
            
            # Select and rename columns for a clean display
            history_display_cols = {
                'Order Date': 'Order Date',
                'Msku Code': 'MSKU',
                'Vendor Name': 'Vendor',
                'Forwarder': 'Forwarder',
                'Carrying Amount': 'Carrying Amount',
                'Quantity': 'Qty Ordered',
                'Final Cost': 'Final Cost',
                'Landed Cost/pcs': 'Landed Cost (INR)',
                'Status': 'PO Status'
            }
            history_display_df = history_df[list(history_display_cols.keys())].rename(columns=history_display_cols)
            
            # Sort by date to show the most recent orders first
            history_display_df.sort_values(by='Order Date', ascending=False, inplace=True)
            
            st.dataframe(
                history_display_df,
                column_config={
                    "Order Date": st.column_config.DateColumn(format="DD-MMM-YYYY"),
                    "Landed Cost (INR)": st.column_config.NumberColumn(format="₹%.2f")
                },
                use_container_width=True,
                hide_index=True
            )
    # --- END NEW SECTION ---

    st.markdown("---")
    if st.button("Send Plan to PO Workspace", type="primary"):
        draft_to_process = st.session_state.replenishment_plan_draft_df # Use the saved state
        if draft_to_process['Vendor Name'].eq('').any():
            st.error("Please assign a Vendor Name to every item in the draft before proceeding.")
        else:
            with st.spinner("Preparing PO Draft..."):
                if 'po_draft_items' not in st.session_state: st.session_state.po_draft_items = []
                items_list = []
                for i, item_row in draft_to_process.iterrows():
                    qty = item_row['Order Quantity']; price = item_row['Unit Cost']; currency = item_row['Currency']
                    total_foreign_amt = float(qty) * float(price)
                    items_list.append({
                        "MSKU": item_row['MSKU'], "Vendor Name": item_row['Vendor Name'],
                        "Forwarder": "", "Shipment Route": "Air", "Arrive by": date.today() + timedelta(days=45),
                        "Category": item_row['Category'], "Quantity": qty, "Currency": currency,
                        "per pcs price usd": price, "USD Amt": total_foreign_amt, "INR Amt": 0.0,
                        "HSN Code": item_row['HSN Code']
                    })
                st.session_state.po_draft_items.extend(items_list)
                st.success(f"Successfully sent {len(items_list)} items to the PO Workspace!")
                st.info("Navigate to the 'Create Purchase Order' page to finalize.")
                st.session_state.replenishment_plan_draft_df = pd.DataFrame(columns=draft_df.columns)
                st.rerun()

    st.subheader("Download Plan Draft")
    col_dl1, col_dl2 = st.columns(2)
    with col_dl1:
        csv_export_df = st.session_state.replenishment_plan_draft_df.copy()
        st.download_button("Download as CSV", csv_export_df.to_csv(index=False).encode('utf-8'),
                           f"replenishment_plan_draft_{date.today().strftime('%Y%m%d')}.csv", "text/csv",
                           key='download-draft-csv', use_container_width=True)
    with col_dl2:
        if st.button("Generate & Download as PDF", key='generate-draft-pdf-btn', use_container_width=True):
            with st.spinner("Generating PDF..."):
                pdf_bytes = generate_replenishment_pdf(st.session_state.replenishment_plan_draft_df)
                if pdf_bytes:
                    st.download_button(label="PDF Ready! Click to Download", data=pdf_bytes,
                                       file_name=f"replenishment_plan_draft_{date.today().strftime('%Y%m%d')}.pdf",
                                       mime="application/pdf", key='download-draft-pdf')
                else: st.error("Failed to generate PDF.")
else:
    st.info("No items have been added to the replenishment plan draft yet. Select items from the overview above.")