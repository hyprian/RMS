# RMS/analytics_dashboard/data_loader.py
import streamlit as st
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# This function will be the single point of contact for loading data from Baserow for the analytics pages.
# It uses st.session_state as a cache to prevent re-fetching on every page rerun.
def load_and_cache_analytics_data(fetcher, sales_table_id, inventory_table_id, force_reload=False):
    """
    Loads sales and inventory data from Baserow into st.session_state.
    If data already exists in the session, it's not re-fetched unless force_reload is True.
    """
    if 'analytics_sales_df' not in st.session_state or force_reload:
        logger.info("DATA_LOADER: No sales data in session state or force_reload=True. Fetching from Baserow...")
        with st.spinner("Loading sales data from Baserow..."):
            sales_df = fetcher.get_table_data_as_dataframe(sales_table_id)
            if sales_df is not None and not sales_df.empty:
                # --- Perform one-time cleaning and type conversion here ---
                logger.info(f"DATA_LOADER: Sales data fetched ({len(sales_df)} rows). Cleaning and caching.")
                sales_df['Sale Date'] = pd.to_datetime(sales_df['Sale Date'], errors='coerce').dt.date
                sales_df['Report Period Start Date'] = pd.to_datetime(sales_df['Report Period Start Date'], errors='coerce').dt.date
                sales_df['Quantity Sold'] = pd.to_numeric(sales_df['Quantity Sold'], errors='coerce').fillna(0)
                sales_df['Net Revenue'] = pd.to_numeric(sales_df['Net Revenue'], errors='coerce').fillna(0)
                sales_df.dropna(subset=['Sale Date', 'MSKU', 'Platform', 'Account Name'], inplace=True)
                st.session_state.analytics_sales_df = sales_df
            else:
                st.session_state.analytics_sales_df = pd.DataFrame() # Store empty DF if fetch fails
                logger.warning("DATA_LOADER: Fetched sales data is empty or None.")
    
    if 'analytics_inventory_df' not in st.session_state or force_reload:
        logger.info("DATA_LOADER: No inventory data in session state or force_reload=True. Fetching from Baserow...")
        if inventory_table_id:
            with st.spinner("Loading inventory data from Baserow..."):
                inventory_df = fetcher.get_inventory_data(inventory_table_id) # Assumes this returns cleaned data
                if inventory_df is not None and not inventory_df.empty:
                    logger.info(f"DATA_LOADER: Inventory data fetched ({len(inventory_df)} rows). Caching.")
                    st.session_state.analytics_inventory_df = inventory_df
                else:
                    st.session_state.analytics_inventory_df = pd.DataFrame()
                    logger.warning("DATA_LOADER: Fetched inventory data is empty or None.")
        else:
             st.session_state.analytics_inventory_df = pd.DataFrame()
             logger.warning("DATA_LOADER: No inventory table ID configured.")