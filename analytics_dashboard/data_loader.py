# RMS/analytics_dashboard/data_loader.py
import streamlit as st
import pandas as pd
import logging
import os
from datetime import datetime, timedelta

# Import the cache utility functions
from utils.cache_manager import load_from_cache, save_to_cache
from utils.config_loader import APP_CONFIG

logger = logging.getLogger(__name__)

# This is a new helper function to reduce code duplication
def _load_single_dataset(
    fetcher, 
    dataset_name: str, 
    session_state_key: str, 
    fetch_function, # The function to call to get data from Baserow (e.g., fetcher.get_sales_data)
    table_id: int, 
    cache_config: dict, 
    force_reload: bool = False
):
    """
    Generic function to load one dataset, handling session state and file cache.
    """
    # If data is already in session state and we are not forcing a reload, do nothing.
    if session_state_key in st.session_state and not force_reload:
        logger.debug(f"DATA_LOADER: Found '{dataset_name}' data in session state. Skipping reload.")
        return

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cache_dir = os.path.join(project_root, cache_config.get('directory', '.rms_cache'))
    cache_expiry_days = cache_config.get('expiry_days', 5)
    
    df = None
    # 1. Try to load from file cache if not forcing a reload
    if not force_reload:
        df = load_from_cache(dataset_name, cache_dir, cache_expiry_days)

    # 2. If cache is missing, stale, or we are forcing a reload, fetch from Baserow
    if df is None:
        logger.info(f"DATA_LOADER: Fetching fresh '{dataset_name}' data from Baserow (Table ID: {table_id}).")
        with st.spinner(f"Loading {dataset_name.replace('_', ' ')} from Baserow..."):
            df = fetch_function(table_id)
            if df is not None and not df.empty:
                # Save the fresh data to the file cache
                save_to_cache(df, dataset_name, cache_dir)
            else:
                # If fetch fails, create an empty DataFrame to avoid errors
                df = pd.DataFrame()
                logger.warning(f"DATA_LOADER: Fetched '{dataset_name}' data is empty or None.")
    
    # 3. Perform one-time cleaning and store in session state
    if dataset_name == 'processed_sales_data':
        if not df.empty:
            df['Sale Date'] = pd.to_datetime(df['Sale Date'], errors='coerce').dt.date
            df['Report Period Start Date'] = pd.to_datetime(df['Report Period Start Date'], errors='coerce').dt.date
            df['Quantity Sold'] = pd.to_numeric(df['Quantity Sold'], errors='coerce').fillna(0)
            df['Net Revenue'] = pd.to_numeric(df['Net Revenue'], errors='coerce').fillna(0)
            df.dropna(subset=['Sale Date', 'MSKU', 'Platform', 'Account Name'], inplace=True)
    
    # Add other specific cleaning steps for inventory, category etc. if needed
    
    st.session_state[session_state_key] = df
    logger.info(f"DATA_LOADER: '{dataset_name}' data is now loaded into session state.")


def load_and_cache_analytics_data(fetcher, sales_table_id, inventory_table_id, category_table_id=None, catalogue_table_id=None, force_reload=False, cache_config=None):
    """
    Orchestrates loading of all analytics datasets, utilizing file cache and session state.
    """
    if cache_config is None:
        cache_config = APP_CONFIG.get('cache', {}) # Get from global config if not passed

    if sales_table_id:
        _load_single_dataset(fetcher, 'processed_sales_data', 'analytics_sales_df', fetcher.get_table_data_as_dataframe, sales_table_id, cache_config, force_reload)
    
    if inventory_table_id:
        _load_single_dataset(fetcher, 'inventory_data', 'analytics_inventory_df', fetcher.get_inventory_data, inventory_table_id, cache_config, force_reload)

    if category_table_id:
        _load_single_dataset(fetcher, 'category_data', 'analytics_category_df', fetcher.get_category_data, category_table_id, cache_config, force_reload)

    if catalogue_table_id:
        _load_single_dataset(fetcher, 'catalogue_data', 'analytics_catalogue_df', fetcher.get_catalogue_data, catalogue_table_id, cache_config, force_reload)