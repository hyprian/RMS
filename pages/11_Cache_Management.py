# RMS/pages/08_Cache_Management.py
import streamlit as st
import os
import sys
from datetime import datetime
import pandas as pd
import json 


project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
from analytics_dashboard.data_loader import load_and_cache_analytics_data

import logging
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Cache Management - RMS", layout="wide")
st.title("🔄 Cache Management")
st.markdown("Force a refresh of the local data cache from Baserow. The app uses cached data by default to load faster. Refreshing here will pull the latest data for your next session or page load.")

# --- Initialize Tools & Config ---
@st.cache_resource
def get_cache_mgmt_fetcher():
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        return fetcher
    except Exception as e: st.error(f"Error initializing Baserow connection: {e}"); return None

fetcher = get_cache_mgmt_fetcher()
if not fetcher: st.error("Failed to initialize Baserow connection."); st.stop()

# Get all table IDs from config
sales_table_id = APP_CONFIG['baserow'].get('processed_sales_data_table_id')
inventory_table_id = APP_CONFIG['baserow'].get('inventory_table_id')
category_table_id = APP_CONFIG['baserow'].get('category_table_id')
catalogue_table_id = APP_CONFIG['baserow'].get('catalogue_table_id')
po_table_id = APP_CONFIG['baserow'].get('purchase_orders_table_id')

# --- Display Cache Status ---
st.header("Current Cache Status")

cache_dir = os.path.join(project_root, APP_CONFIG.get('cache', {}).get('directory', '.rms_cache'))
datasets_to_manage = {
    "Processed Sales Data": "processed_sales_data",
    "Inventory Data": "inventory_data",
    "Category Data": "category_data",
    "Catalogue Data": "catalogue_data",
    # We can add POs and others here too
}

status_data = []
for display_name, cache_name in datasets_to_manage.items():
    meta_path = os.path.join(cache_dir, f"{cache_name}_meta.json")
    status = "Not Cached"
    last_updated = "N/A"
    if os.path.exists(meta_path):
        try:
            with open(meta_path, 'r') as f:
                meta = json.load(f)
            last_updated_dt = datetime.fromisoformat(meta.get("last_updated"))
            last_updated = last_updated_dt.strftime('%Y-%m-%d %H:%M:%S')
            status = "Cached"
        except Exception:
            status = "Cache Invalid"
    status_data.append({"Dataset": display_name, "Status": status, "Last Updated": last_updated})

st.dataframe(pd.DataFrame(status_data), use_container_width=True)

st.divider()

# --- Refresh Buttons ---
st.header("Refresh Cache from Baserow")

col1, col2, col3, col4 = st.columns(4)

with col1:
    if st.button("Refresh Sales Data", use_container_width=True):
        with st.spinner("Forcing refresh of Sales Data..."):
            load_and_cache_analytics_data(fetcher, sales_table_id, None, None, None, force_reload=True)
            st.success("Sales Data cache updated!")
            st.rerun()

with col2:
    if st.button("Refresh Inventory Data", use_container_width=True):
        with st.spinner("Forcing refresh of Inventory Data..."):
            load_and_cache_analytics_data(fetcher, None, inventory_table_id, None, None, force_reload=True)
            st.success("Inventory Data cache updated!")
            st.rerun()

with col3:
    if st.button("Refresh Category Data", use_container_width=True):
        with st.spinner("Forcing refresh of Category Data..."):
            load_and_cache_analytics_data(fetcher, None, None, category_table_id, None, force_reload=True)
            st.success("Category Data cache updated!")
            st.rerun()

with col4:
    if st.button("Refresh Catalogue Data", use_container_width=True):
        with st.spinner("Forcing refresh of Catalogue Data..."):
            load_and_cache_analytics_data(fetcher, None, None, None, catalogue_table_id, force_reload=True)
            st.success("Catalogue Data cache updated!")
            st.rerun()

st.divider()
if st.button("🔄 REFRESH ALL DATASETS", type="primary", use_container_width=True):
    with st.spinner("Forcing refresh of ALL datasets... This may take a moment."):
        load_and_cache_analytics_data(fetcher, sales_table_id, inventory_table_id, category_table_id, catalogue_table_id, force_reload=True)
        # Also clear the PO data from session state so it reloads on its page
        if 'po_all_pos_df' in st.session_state:
            del st.session_state['po_all_pos_df']
        if 'manage_po_all_pos_df' in st.session_state:
            del st.session_state['manage_po_all_pos_df']
        st.success("All dataset caches have been updated!")
        st.rerun()