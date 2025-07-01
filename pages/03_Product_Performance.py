# RMS/pages/03_Product_Performance.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
from analytics_dashboard.data_loader import load_and_cache_analytics_data # NEW
from analytics_dashboard.kpi_calculations import (
    process_sales_data_for_analytics as get_sales_data, # Use the new name
    calculate_total_sales_kpis,
    get_sales_trend_data
)
from analytics_dashboard.charts import create_sales_trend_chart, create_pie_chart
import logging

logger = logging.getLogger(__name__)

st.set_page_config(page_title="Product Performance - RMS", layout="wide")
st.title("📦 Product Performance Analysis")

# --- Initialize Tools & Load Data into Session State ---
@st.cache_resource
def get_analytics_fetcher_prod():
    # ... (same as before) ...
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        return fetcher
    except Exception as e:
        st.error(f"Error initializing Baserow connection: {e}")
        return None

fetcher = get_analytics_fetcher_prod()
if not fetcher: st.error("Failed to initialize Baserow connection."); st.stop()

processed_sales_table_id = APP_CONFIG['baserow'].get('processed_sales_data_table_id')
inventory_table_id = APP_CONFIG['baserow'].get('inventory_table_id')
category_table_id = APP_CONFIG['baserow'].get('category_table_id') # Get the category table ID

# Load all data once per session. This is the key performance improvement.
load_and_cache_analytics_data(fetcher, processed_sales_table_id, inventory_table_id, category_table_id)

# Now, get data from session state instead of fetching again
all_sales_df = st.session_state.get('analytics_sales_df')
all_inventory_df = st.session_state.get('analytics_inventory_df')
all_category_df = st.session_state.get('analytics_category_df') 

if all_sales_df is None or all_sales_df.empty:
    st.warning("No sales data available. Please upload sales reports on the 'Sales Data Ingestion' page.")
    st.stop()

# --- MSKU Selector ---
# Get unique MSKUs directly from the in-memory DataFrame, which is much faster
sales_mskust = set(all_sales_df['MSKU'].dropna().unique())
inventory_mskust = set()
if all_inventory_df is not None and not all_inventory_df.empty and 'MSKU' in all_inventory_df.columns:
    inventory_mskust = set(all_inventory_df['MSKU'].dropna().unique())
all_available_mskust = sorted(list(sales_mskust | inventory_mskust))

if not all_available_mskust:
    st.warning("No MSKUs found. Cannot display product performance.")
    st.stop()

# --- Sidebar Filters ---
st.sidebar.header("Product Filters")
selected_msku = st.sidebar.selectbox("Select MSKU:", options=all_available_mskust, key="product_perf_msku_select")
default_end_date_prod = date.today()
default_start_date_prod = default_end_date_prod - timedelta(days=89)
selected_start_date_prod = st.sidebar.date_input("Start Date", value=default_start_date_prod, key="product_perf_start_date")
selected_end_date_prod = st.sidebar.date_input("End Date", value=default_end_date_prod, key="product_perf_end_date")
if selected_start_date_prod > selected_end_date_prod: st.sidebar.error("Start Date cannot be after End Date."); st.stop()

# --- Display MSKU Info (from session state) ---
if selected_msku:
    st.header(f"Performance for MSKU: {selected_msku}")
    # Display Category
    if all_category_df is not None and not all_category_df.empty:
        msku_category_row = all_category_df[all_category_df['MSKU'] == selected_msku]
        if not msku_category_row.empty:
            category_name = msku_category_row['Category'].iloc[0]
            st.markdown(f"**Category:** `{category_name}`")
        else:
            st.markdown(f"**Category:** `Uncategorized`")

    if all_inventory_df is not None and not all_inventory_df.empty:
        msku_inventory_row = all_inventory_df[all_inventory_df['MSKU'] == selected_msku]
        if not msku_inventory_row.empty:
            current_inv = msku_inventory_row['Current Inventory'].iloc[0]
            st.metric("Current Inventory", f"{current_inv:,.0f} units")
        else: st.info("Inventory data not found for this MSKU.")
else: st.header("Select an MSKU from the sidebar")

st.divider()

# --- Process Data for Selected MSKU (using in-memory DataFrame) ---
if selected_msku:
    # This now just filters the already loaded DataFrame, which is very fast.
    sales_df_msku_daily = get_sales_data( # This is now the process_sales_data_for_analytics function
        all_sales_df, # Pass the full DataFrame from session state
        selected_start_date_prod,
        selected_end_date_prod,
        mskust_list=[selected_msku]
    )

    if sales_df_msku_daily is None or sales_df_msku_daily.empty:
        st.warning(f"No sales data found for MSKU '{selected_msku}' in the selected period.")
    else:
        st.success(f"Displaying analytics based on {len(sales_df_msku_daily)} daily sales records for {selected_msku}.")
        
        # --- Calculate KPIs for the MSKU ---
        kpis_msku = calculate_total_sales_kpis(sales_df_msku_daily)

        st.subheader("Key Performance Indicators (KPIs) for this MSKU")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Net Revenue", f"₹{kpis_msku['total_net_revenue']:,.2f}")
        col2.metric("Total Units Sold", f"{kpis_msku['total_units_sold']:,.0f}")
        col3.metric("Total Orders (Approx.)", f"{kpis_msku['total_orders']:,}")
        col4.metric("Avg. Selling Price (ASP)", f"₹{kpis_msku['average_selling_price']:,.2f}")
        st.divider()

        # --- Sales Trend Chart for the MSKU ---
        st.subheader("Sales Trends for this MSKU")
        trend_freq_prod = st.selectbox("Trend Granularity:", options=['D', 'W', 'M'], format_func=lambda x: {'D':'Daily', 'W':'Weekly', 'M':'Monthly'}[x], index=0, key="product_trend_freq_selector")
        
        trend_data_revenue_msku = get_sales_trend_data(sales_df_msku_daily, freq=trend_freq_prod)
        if not trend_data_revenue_msku.empty:
            fig_rev_trend_msku = create_sales_trend_chart(trend_data_revenue_msku, y_column='Net Revenue', y_column_name="Net Revenue (₹)", title=f"{ {'D':'Daily', 'W':'Weekly', 'M':'Monthly'}[trend_freq_prod]} Net Revenue Trend for {selected_msku}")
            st.plotly_chart(fig_rev_trend_msku, use_container_width=True)
        else: st.info("Not enough data for revenue trend.")

        trend_data_units_msku = get_sales_trend_data(sales_df_msku_daily, freq=trend_freq_prod)
        if not trend_data_units_msku.empty and 'Quantity Sold' in trend_data_units_msku.columns:
            fig_units_trend_msku = create_sales_trend_chart(trend_data_units_msku, y_column='Quantity Sold', y_column_name="Units Sold", title=f"{ {'D':'Daily', 'W':'Weekly', 'M':'Monthly'}[trend_freq_prod]} Units Sold Trend for {selected_msku}")
            st.plotly_chart(fig_units_trend_msku, use_container_width=True)
        else: st.info("Not enough data for units sold trend.")
        st.divider()

        # --- Sales Breakdown by Platform & Account for this MSKU ---
        st.subheader("Sales Breakdown for this MSKU")
        col_plat_prod, col_acc_prod = st.columns(2)

        with col_plat_prod:
            st.markdown("##### By Platform")
            if not sales_df_msku_daily.empty and 'Platform' in sales_df_msku_daily.columns:
                platform_sales_msku = sales_df_msku_daily.groupby('Platform', as_index=False)['Net Revenue'].sum()
                if not platform_sales_msku.empty:
                    fig_plat_pie_msku = create_pie_chart(platform_sales_msku, names_column='Platform', values_column='Net Revenue', title=f'Revenue by Platform for {selected_msku}')
                    st.plotly_chart(fig_plat_pie_msku, use_container_width=True)
                else: st.info("No platform breakdown data.")
            else: st.info("Platform data missing.")
                
        with col_acc_prod:
            st.markdown("##### By Account")
            if not sales_df_msku_daily.empty and 'Account Name' in sales_df_msku_daily.columns and 'Platform' in sales_df_msku_daily.columns:
                sales_df_msku_daily['Platform_Account_Display'] = sales_df_msku_daily['Platform'] + " - " + sales_df_msku_daily['Account Name']
                account_sales_msku = sales_df_msku_daily.groupby('Platform_Account_Display', as_index=False)['Net Revenue'].sum()
                if not account_sales_msku.empty:
                    fig_acc_pie_msku = create_pie_chart(account_sales_msku, names_column='Platform_Account_Display', values_column='Net Revenue', title=f'Revenue by Account for {selected_msku}')
                    st.plotly_chart(fig_acc_pie_msku, use_container_width=True)
                else: st.info("No account breakdown data.")
            else: st.info("Account/Platform data missing.")
else:
    st.info("Select an MSKU from the sidebar to view its performance details.")

logger.info("Product Performance page loaded/refreshed.")