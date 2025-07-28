# RMS/analytics_dashboard/data_loader.py
import streamlit as st
import pandas as pd
import logging
import os
from datetime import datetime, timedelta

from utils.cache_manager import load_from_cache, save_to_cache
from utils.config_loader import APP_CONFIG # Import APP_CONFIG if not already

logger = logging.getLogger(__name__)

def _load_single_dataset(
    fetcher, 
    dataset_name: str, 
    session_state_key: str, 
    fetch_function,
    table_id: int, 
    cache_config: dict, 
    force_reload: bool = False
):
    """
    Generic function to load one dataset, handling session state and file cache.
    """
    if session_state_key in st.session_state and not force_reload:
        logger.debug(f"DATA_LOADER: Found '{dataset_name}' data in session state. Skipping reload.")
        return

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cache_dir = os.path.join(project_root, cache_config.get('directory', '.rms_cache'))
    cache_expiry_days = cache_config.get('expiry_days', 5)
    
    df = None
    if not force_reload:
        df = load_from_cache(dataset_name, cache_dir, cache_expiry_days)

    if df is None:
        logger.info(f"DATA_LOADER: Fetching fresh '{dataset_name}' data from Baserow (Table ID: {table_id}).")
        with st.spinner(f"Loading {dataset_name.replace('_', ' ')} from Baserow..."):
            df = fetch_function(table_id)
            if df is not None and not df.empty:
                save_to_cache(df, dataset_name, cache_dir)
            else:
                df = pd.DataFrame()
                logger.warning(f"DATA_LOADER: Fetched '{dataset_name}' data is empty or None.")
    
    # Perform one-time cleaning specific to the dataset
    if dataset_name == 'processed_sales_data' and not df.empty:
        df['Sale Date'] = pd.to_datetime(df['Sale Date'], errors='coerce').dt.date
        df['Report Period Start Date'] = pd.to_datetime(df['Report Period Start Date'], errors='coerce').dt.date
        df['Quantity Sold'] = pd.to_numeric(df['Quantity Sold'], errors='coerce').fillna(0)
        df['Net Revenue'] = pd.to_numeric(df['Net Revenue'], errors='coerce').fillna(0)
        df.dropna(subset=['Sale Date', 'Platform', 'Account Name'], inplace=True)
        df['MSKU'] = df['MSKU'].fillna('UNMAPPED').replace('', 'UNMAPPED')
    
    st.session_state[session_state_key] = df
    logger.info(f"DATA_LOADER: '{dataset_name}' data is now loaded into session state.")


def load_and_cache_analytics_data(
    fetcher, 
    sales_table_id, 
    inventory_table_id, 
    category_table_id=None, 
    catalogue_table_id=None, 
    outbound_table_id=None, # NEW
    packaging_inv_table_id=None, # NEW
    force_reload=False, 
    cache_config=None
):
    """
    Orchestrates loading of all analytics datasets, utilizing file cache and session state.
    """
    if cache_config is None:
        cache_config = APP_CONFIG.get('cache', {})

    if sales_table_id:
        _load_single_dataset(fetcher, 'processed_sales_data', 'analytics_sales_df', fetcher.get_table_data_as_dataframe, sales_table_id, cache_config, force_reload)
    
    if inventory_table_id:
        _load_single_dataset(fetcher, 'inventory_data', 'analytics_inventory_df', fetcher.get_inventory_data, inventory_table_id, cache_config, force_reload)

    if category_table_id:
        _load_single_dataset(fetcher, 'category_data', 'analytics_category_df', fetcher.get_category_data, category_table_id, cache_config, force_reload)

    if catalogue_table_id:
        _load_single_dataset(fetcher, 'catalogue_data', 'analytics_catalogue_df', fetcher.get_catalogue_data, catalogue_table_id, cache_config, force_reload)

    # --- NEW: Add logic to load packaging data ---
    if outbound_table_id:
        _load_single_dataset(fetcher, 'outbound_packaging_data', 'packaging_outbound_df', fetcher.get_outbound_packaging_data, outbound_table_id, cache_config, force_reload)

    if packaging_inv_table_id:
        _load_single_dataset(fetcher, 'packaging_inventory_data', 'packaging_inventory_df', fetcher.get_packaging_inventory, packaging_inv_table_id, cache_config, force_reload)
    # --- END NEW ---