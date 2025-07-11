# RMS/analytics_dashboard/data_loader.py
import streamlit as st
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def load_and_cache_analytics_data(fetcher, sales_table_id, inventory_table_id, category_table_id=None, catalogue_table_id=None, force_reload=False):
    """
    Loads sales, inventory, and category data from Baserow into st.session_state.
    It will skip fetching for any table_id that is None.
    """
    # --- Sales Data Loading ---
    if sales_table_id and ('analytics_sales_df' not in st.session_state or force_reload): # ADDED CHECK
        logger.info("DATA_LOADER: Fetching sales data from Baserow...")
        with st.spinner("Loading sales data from Baserow..."):
            sales_df = fetcher.get_table_data_as_dataframe(sales_table_id)
            if sales_df is not None and not sales_df.empty:
                logger.info(f"DATA_LOADER: Sales data fetched ({len(sales_df)} rows). Cleaning and caching.")
                sales_df['Sale Date'] = pd.to_datetime(sales_df['Sale Date'], errors='coerce').dt.date
                sales_df['Report Period Start Date'] = pd.to_datetime(sales_df['Report Period Start Date'], errors='coerce').dt.date
                sales_df['Quantity Sold'] = pd.to_numeric(sales_df['Quantity Sold'], errors='coerce').fillna(0)
                sales_df['Net Revenue'] = pd.to_numeric(sales_df['Net Revenue'], errors='coerce').fillna(0)
                sales_df.dropna(subset=['Sale Date', 'MSKU', 'Platform', 'Account Name'], inplace=True)
                st.session_state.analytics_sales_df = sales_df
            else:
                st.session_state.analytics_sales_df = pd.DataFrame()
    
    # --- Inventory Data Loading ---
    if inventory_table_id and ('analytics_inventory_df' not in st.session_state or force_reload): # ADDED CHECK
        logger.info("DATA_LOADER: Fetching inventory data from Baserow...")
        with st.spinner("Loading inventory data from Baserow..."):
            inventory_df = fetcher.get_inventory_data(inventory_table_id)
            if inventory_df is not None and not inventory_df.empty:
                st.session_state.analytics_inventory_df = inventory_df
            else:
                st.session_state.analytics_inventory_df = pd.DataFrame()

    # --- Category Data Loading ---
    if category_table_id and ('analytics_category_df' not in st.session_state or force_reload): # ADDED CHECK
        logger.info("DATA_LOADER: Fetching category data from Baserow...")
        with st.spinner("Loading category data from Baserow..."):
            category_df = fetcher.get_category_data(category_table_id)
            if category_df is not None and not category_df.empty:
                st.session_state.analytics_category_df = category_df
            else:
                st.session_state.analytics_category_df = pd.DataFrame()

    if catalogue_table_id and ('analytics_catalogue_df' not in st.session_state or force_reload):
        logger.info("DATA_LOADER: No catalogue data in session state or force_reload=True. Fetching from Baserow...")
        if catalogue_table_id:
            with st.spinner("Loading product catalogue (images)..."):
                catalogue_df = fetcher.get_catalogue_data(catalogue_table_id)
                if catalogue_df is not None and not catalogue_df.empty:
                    logger.info(f"DATA_LOADER: Catalogue data fetched ({len(catalogue_df)} rows). Caching.")
                    st.session_state.analytics_catalogue_df = catalogue_df
                else:
                    st.session_state.analytics_catalogue_df = pd.DataFrame()
                    logger.warning("DATA_LOADER: Fetched catalogue data is empty or None.")
        else:
             st.session_state.analytics_catalogue_df = pd.DataFrame()
             logger.warning("DATA_LOADER: No catalogue table ID configured.")