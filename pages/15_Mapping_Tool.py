# RMS/pages/13_Mapping_Tool.py
import streamlit as st
import pandas as pd
import os
import sys
from datetime import datetime, timedelta

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
from data_processing.sku_mapper import SKUMapper
from analytics_dashboard.data_loader import load_and_cache_analytics_data
from po_module.po_management import get_last_order_dates
from replenishment_engine.core import calculate_sales_stats
from analytics_dashboard.kpi_calculations import process_sales_data_for_analytics as get_sales_data

import logging
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Product Lookup Tool - RMS", layout="wide")

if "error" in APP_CONFIG:
    st.error(f"CRITICAL CONFIGURATION ERROR: {APP_CONFIG['error']}")
    st.stop()

st.title("🔍 Product Lookup & Enrichment Tool")
st.markdown("Upload a file or paste a list of identifiers (SKU, MSKU, or ASIN) to look up product details, inventory, and sales performance.")

# --- THIS IS THE FIX ---
# Initialize all session state keys for this page at the top.
if 'mapper_input_df' not in st.session_state:
    st.session_state.mapper_input_df = None
if 'mapper_result_df' not in st.session_state:
    st.session_state.mapper_result_df = None
# --- END FIX ---

# --- Initialize Tools & Load ALL Data ---
@st.cache_resource
def get_lookup_tools(force_refresh=False):
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        required_ids = ['sku_mapping_table_id', 'combo_sku_table_id', 'amazon_listing_table_id']
        if not all(key in APP_CONFIG['baserow'] for key in required_ids):
            st.error(f"Missing one or more required table IDs for the SKU Mapper: {required_ids}")
            return None
        sku_mapper_instance = SKUMapper(
            baserow_fetcher=fetcher,
            sku_mapping_table_id=APP_CONFIG['baserow']['sku_mapping_table_id'],
            combo_sku_table_id=APP_CONFIG['baserow']['combo_sku_table_id'],
            amazon_listing_table_id=APP_CONFIG['baserow']['amazon_listing_table_id'],
            cache_config=APP_CONFIG.get('cache', {}),
            project_root_dir=project_root,
            force_refresh_cache=force_refresh
        )
        return fetcher, sku_mapper_instance
    except Exception as e:
        st.error(f"Error initializing mapping tools: {e}")
        return None, None

fetcher, sku_mapper = get_lookup_tools()
if not fetcher or not sku_mapper:
    st.error("Could not be initialized. Check Baserow configuration and connection.")
    st.stop()

load_and_cache_analytics_data(
    fetcher, 
    APP_CONFIG['baserow'].get('processed_sales_data_table_id'),
    APP_CONFIG['baserow'].get('inventory_table_id'),
    APP_CONFIG['baserow'].get('category_table_id'),
    APP_CONFIG['baserow'].get('catalogue_table_id'),
    APP_CONFIG['baserow'].get('purchase_orders_table_id')
)
all_sales_df = st.session_state.get('analytics_sales_df')
all_inventory_df = st.session_state.get('analytics_inventory_df')
all_category_df = st.session_state.get('analytics_category_df')
all_pos_df = st.session_state.get('po_all_pos_df')

@st.cache_data
def precompute_enrichment_data(_sales_df, _pos_df):
    if _sales_df is None or _sales_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    logger.info("LOOKUP_TOOL: Pre-computing sales stats for enrichment...")
    max_date = _sales_df['Sale Date'].max()
    min_date = max_date - timedelta(days=89)
    daily_sales_for_stats = get_sales_data(_sales_df, min_date, max_date)
    sales_stats_df = calculate_sales_stats(daily_sales_for_stats, sales_history_days=30)
    last_order_dates_df = get_last_order_dates(_pos_df)
    return sales_stats_df, last_order_dates_df

sales_stats, last_orders = precompute_enrichment_data(all_sales_df, all_pos_df)

# --- Sidebar Controls ---
st.sidebar.header("Mapping Controls")
if st.sidebar.button("🔄 Refresh All Mapping Data from Baserow"):
    try:
        get_lookup_tools.clear()
        fetcher, sku_mapper = get_lookup_tools(force_refresh=True)
        if sku_mapper: st.sidebar.success("Mapping data cache has been refreshed!")
        else: st.sidebar.error("Failed to re-initialize mapper after refresh.")
    except Exception as e:
        st.sidebar.error(f"Failed to refresh cache: {e}")

# --- Main Page UI ---
st.header("1. Provide Your Data")

# --- NEW: Input Method Selector ---
input_method = st.radio(
    "Select Input Method:",
    options=["File Upload", "Manual Text Input"],
    horizontal=True,
    key="mapper_input_method"
)

input_df = None

if input_method == "File Upload":
    uploaded_file = st.file_uploader("Upload a CSV file", type="csv", key="mapper_file_uploader")
    if uploaded_file:
        try:
            input_df = pd.read_csv(uploaded_file, dtype=str).fillna('')
            st.success(f"Successfully loaded '{uploaded_file.name}' with {len(input_df)} rows.")
        except Exception as e:
            st.error(f"Error reading CSV file: {e}")

else: # Manual Text Input
    manual_input_text = st.text_area(
        "Paste your list of identifiers (one per line):",
        height=200,
        key="mapper_manual_input"
    )
    if manual_input_text:
        # Convert the multi-line string into a DataFrame
        items = [item.strip() for item in manual_input_text.strip().split('\n') if item.strip()]
        if items:
            input_df = pd.DataFrame(items, columns=['Identifier'])
            st.success(f"Loaded {len(input_df)} identifiers from text input.")

# Store the processed input_df in session state
st.session_state.mapper_input_df = input_df

if st.session_state.mapper_input_df is not None:
    st.divider()
    st.header("2. Configure Lookup")
    
    input_df = st.session_state.mapper_input_df
    column_options = input_df.columns.tolist() # No blank option needed
    
    col1, col2 = st.columns(2)
    with col1:
        map_from_type = st.selectbox("Identifier Type in your data:", options=["Platform SKU", "MSKU", "ASIN"])
    with col2:
        # If manual input, the column is always 'Identifier'. Otherwise, let user choose.
        if input_method == "Manual Text Input":
            source_column = 'Identifier'
            st.text_input("Source Column:", value=source_column, disabled=True)
        else:
            source_column = st.selectbox(f"Which column contains the {map_from_type}s?", options=column_options)

    # --- NEW: Improved UI for selecting output columns ---
    st.subheader("Select Data to Include in Results")
    with st.container(border=True):
        enrich_cols = st.columns(4)
        with enrich_cols[0]:
            st.markdown("##### Basic Mapping")
            inc_sku = st.checkbox("Platform SKU", value=True)
            inc_msku = st.checkbox("MSKU", value=True)
            inc_asin = st.checkbox("ASIN", value=True)
        with enrich_cols[1]:
            st.markdown("##### Product Info")
            inc_panel = st.checkbox("Panel", value=True)
            inc_status = st.checkbox("Listing Status", value=True)
            inc_category = st.checkbox("Category", value=True)
            inc_cogs = st.checkbox("COGS (INR)")
        with enrich_cols[2]:
            st.markdown("##### Inventory & Sales")
            inc_inventory = st.checkbox("Current Inventory", value=True)
            inc_sales_30d = st.checkbox("30-Day Avg Sales", value=True)
            inc_last_sale = st.checkbox("Last Sale Date")
        with enrich_cols[3]:
            st.markdown("##### Purchasing")
            inc_last_order = st.checkbox("Last Order Date")
            inc_vendor = st.checkbox("Vendor")
            inc_lead_time = st.checkbox("Vendor Lead Time")

    if st.button("Run Lookup & Enrich", disabled=(not source_column)):
        # ... (The mapping logic from the previous step remains exactly the same) ...
        with st.spinner("Looking up data..."):
            results_list = []
            for index, row in input_df.iterrows():
                source_value = row[source_column]
                base_details = {}
                if map_from_type == "Platform SKU":
                    details = sku_mapper.get_mapping_details_for_sku(source_value)
                    if details: base_details = details
                elif map_from_type == "MSKU":
                    details_list = sku_mapper.get_mapping_details_for_msku(source_value)
                    if details_list: base_details = details_list[0]
                elif map_from_type == "ASIN":
                    details = sku_mapper.get_mapping_details_for_asin(source_value)
                    if details: base_details = details
                result_row = {source_column: source_value}
                msku = base_details.get('msku') if base_details else None
                if inc_sku: result_row['Platform SKU'] = base_details.get('sku', 'NOT FOUND')
                if inc_msku: result_row['MSKU'] = msku if msku else 'NOT FOUND'
                if inc_asin: result_row['ASIN'] = base_details.get('asin', 'NOT FOUND')
                if inc_panel: result_row['Panel'] = base_details.get('Panel', 'N/A')
                if inc_status: result_row['Listing Status'] = base_details.get('Status', 'N/A')
                if msku:
                    if (inc_category or inc_cogs or inc_vendor or inc_lead_time) and all_category_df is not None:
                        cat_row = all_category_df[all_category_df['MSKU'] == msku]
                        if not cat_row.empty:
                            if inc_category: result_row['Category'] = cat_row.iloc[0].get('Category', 'N/A')
                            if inc_cogs: result_row['COGS (INR)'] = cat_row.iloc[0].get('Cost Inc.GST', 'N/A')
                            if inc_vendor: result_row['Vendor'] = cat_row.iloc[0].get('Supplier', 'N/A')
                            if inc_lead_time: result_row['Vendor Lead Time'] = cat_row.iloc[0].get('Vendor Lead Time (days)', 'N/A')
                    if inc_inventory and all_inventory_df is not None:
                        inv_row = all_inventory_df[all_inventory_df['MSKU'] == msku]
                        if not inv_row.empty: result_row['Current Inventory'] = inv_row.iloc[0].get('Current Inventory', 0)
                    if (inc_sales_30d or inc_last_sale) and sales_stats is not None:
                        stats_row = sales_stats[sales_stats['MSKU'] == msku]
                        if not stats_row.empty:
                            if inc_sales_30d: result_row['30-Day Avg Sales'] = stats_row.iloc[0].get('avg_daily_sales', 0)
                            if inc_last_sale: result_row['Last Sale Date'] = stats_row.iloc[0].get('last_sale_date')
                    if inc_last_order and last_orders is not None:
                        order_row = last_orders[last_orders['MSKU'] == msku]
                        if not order_row.empty: result_row['Last Order Date'] = order_row.iloc[0].get('last_order_date')
                results_list.append(result_row)
            result_df = pd.DataFrame(results_list)
            st.session_state.mapper_result_df = result_df
            st.success("Lookup complete! See the results below.")

# --- Display Results ---
if st.session_state.mapper_result_df is not None:
    st.divider()
    st.header("3. Lookup Results")
    result_df = st.session_state.mapper_result_df
    st.dataframe(result_df, use_container_width=True, hide_index=True)
    st.download_button(label="Download Results as CSV", data=result_df.to_csv(index=False).encode('utf-8'),
                       file_name="lookup_results.csv", mime="text/csv", type="primary")