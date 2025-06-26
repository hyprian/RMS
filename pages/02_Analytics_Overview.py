# RMS/pages/02_Analytics_Overview.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date 
import os
import sys

# Adjust project_root to be the actual RMS directory
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
from analytics_dashboard.kpi_calculations import (
    get_sales_data, 
    calculate_total_sales_kpis,
    get_sales_trend_data
)
from analytics_dashboard.charts import (
    create_sales_trend_chart,
    create_pie_chart,
    create_bar_chart # We might use this for breakdown too
)
import logging

logger = logging.getLogger(__name__)

st.set_page_config(page_title="Analytics Overview - RMS", layout="wide")
st.title("📈 Analytics Overview Dashboard")

# --- Initialize Tools & Config ---
@st.cache_resource
def get_analytics_fetcher():
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        return fetcher
    except Exception as e:
        st.error(f"Error initializing Baserow connection: {e}")
        return None

fetcher = get_analytics_fetcher()
if not fetcher:
    st.error("Failed to initialize Baserow connection for analytics. Check configuration.")
    st.stop()

processed_sales_table_id = APP_CONFIG['baserow'].get('processed_sales_data_table_id')
if not processed_sales_table_id:
    st.error("`processed_sales_data_table_id` is not configured in settings.yaml. Cannot display analytics.")
    st.stop()

platforms_config = APP_CONFIG.get('platforms', [])

# --- Sidebar Filters ---
st.sidebar.header("Filters")
default_end_date = date.today()
default_start_date = default_end_date - timedelta(days=29)
selected_start_date = st.sidebar.date_input("Start Date", value=default_start_date, key="analytics_start_date")
selected_end_date = st.sidebar.date_input("End Date", value=default_end_date, key="analytics_end_date")

if selected_start_date > selected_end_date:
    st.sidebar.error("Start Date cannot be after End Date."); st.stop()

platform_options = ["All Platforms"] + [p['name'] for p in platforms_config]
selected_platforms_names = st.sidebar.multiselect("Platforms", options=platform_options, default=["All Platforms"], key="analytics_platforms")

account_options = ["All Accounts"]
# Populate account options based on selected platforms
if selected_platforms_names and "All Platforms" not in selected_platforms_names:
    for p_name_selected in selected_platforms_names:
        p_conf = next((p for p in platforms_config if p['name'] == p_name_selected), None)
        if p_conf:
            for acc in p_conf.get('accounts', []):
                account_options.append(f"{p_conf['name']} - {acc['name']}")
elif not selected_platforms_names or "All Platforms" in selected_platforms_names:
    for p_conf in platforms_config:
         for acc in p_conf.get('accounts', []):
            account_options.append(f"{p_conf['name']} - {acc['name']}")
# Remove duplicates if any from account_options (though unlikely with this logic)
account_options = sorted(list(set(account_options)))


selected_accounts_display = st.sidebar.multiselect("Accounts", options=account_options, default=["All Accounts"], key="analytics_accounts")

filter_platforms = None
if selected_platforms_names and "All Platforms" not in selected_platforms_names:
    filter_platforms = selected_platforms_names

filter_accounts = None
if selected_accounts_display and "All Accounts" not in selected_accounts_display:
    filter_accounts = [acc.split(" - ")[-1].strip() for acc in selected_accounts_display] # Get actual account name


# --- Fetch and Process Data ---
with st.spinner(f"Loading analytics for {selected_start_date.strftime('%Y-%m-%d')} to {selected_end_date.strftime('%Y-%m-%d')}..."):
    sales_df_daily = get_sales_data(
        fetcher, processed_sales_table_id,
        selected_start_date, selected_end_date,
        platforms=filter_platforms, accounts=filter_accounts
    )

if sales_df_daily is None or sales_df_daily.empty:
    st.warning(f"No sales data found for the selected filters and period ({selected_start_date.strftime('%Y-%m-%d')} to {selected_end_date.strftime('%Y-%m-%d')}).")
else:
    st.success(f"Displaying analytics based on {len(sales_df_daily)} processed daily sales records.")
    
    kpis = calculate_total_sales_kpis(sales_df_daily)
    st.header("Key Performance Indicators (KPIs)")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Net Revenue", f"₹{kpis['total_net_revenue']:,.2f}")
    col2.metric("Total Units Sold", f"{kpis['total_units_sold']:,.0f}") # Format as integer
    col3.metric("Total Orders (Approx.)", f"{kpis['total_orders']:,}")
    col4.metric("Avg. Selling Price (ASP)", f"₹{kpis['average_selling_price']:,.2f}")
    st.divider()

    st.header("Sales Trends")
    trend_freq = st.selectbox("Trend Granularity:", options=['D', 'W', 'M'], format_func=lambda x: {'D':'Daily', 'W':'Weekly', 'M':'Monthly'}[x], index=0, key="trend_freq_selector")
    
    trend_data_revenue = get_sales_trend_data(sales_df_daily, freq=trend_freq)
    if not trend_data_revenue.empty:
        fig_revenue_trend = create_sales_trend_chart(trend_data_revenue, y_column='Net Revenue', y_column_name="Net Revenue (₹)", title=f"{ {'D':'Daily', 'W':'Weekly', 'M':'Monthly'}[trend_freq]} Net Revenue Trend")
        st.plotly_chart(fig_revenue_trend, use_container_width=True)
    else: st.info("Not enough data for revenue trend at selected granularity.")

    # Assuming get_sales_trend_data can also be used for units if the input df has 'Quantity Sold'
    trend_data_units = get_sales_trend_data(sales_df_daily, freq=trend_freq) 
    if not trend_data_units.empty and 'Quantity Sold' in trend_data_units.columns:
        fig_units_trend = create_sales_trend_chart(trend_data_units, y_column='Quantity Sold', y_column_name="Units Sold", title=f"{ {'D':'Daily', 'W':'Weekly', 'M':'Monthly'}[trend_freq]} Units Sold Trend")
        st.plotly_chart(fig_units_trend, use_container_width=True)
    else: st.info("Not enough data for units sold trend at selected granularity.")
    st.divider()

    # --- Sales by Platform & Account ---
    st.header("Sales Breakdown")
    col_plat, col_acc = st.columns(2)

    with col_plat:
        st.subheader("By Platform")
        if not sales_df_daily.empty and 'Platform' in sales_df_daily.columns and 'Net Revenue' in sales_df_daily.columns:
            platform_sales = sales_df_daily.groupby('Platform', as_index=False)['Net Revenue'].sum()
            platform_sales = platform_sales.sort_values(by='Net Revenue', ascending=False) # Sort for better bar chart
            if not platform_sales.empty:
                # Using Bar chart for platform for better readability if many platforms
                fig_platform_bar = create_bar_chart(
                    platform_sales, 
                    x_column='Platform', 
                    y_column='Net Revenue', 
                    y_column_name='Net Revenue (₹)',
                    title='Revenue by Platform'
                )
                st.plotly_chart(fig_platform_bar, use_container_width=True)
            else: st.info("No data for platform breakdown.")
        else: st.info("Platform or revenue data missing for breakdown.")
            
    with col_acc:
        st.subheader("By Account")
        if not sales_df_daily.empty and 'Account Name' in sales_df_daily.columns and 'Platform' in sales_df_daily.columns and 'Net Revenue' in sales_df_daily.columns:
            # Create a combined Platform - Account column for unique labeling
            sales_df_daily['Platform_Account_Display'] = sales_df_daily['Platform'] + " - " + sales_df_daily['Account Name']
            account_sales = sales_df_daily.groupby('Platform_Account_Display', as_index=False)['Net Revenue'].sum()
            account_sales = account_sales.sort_values(by='Net Revenue', ascending=False)
            if not account_sales.empty:
                fig_account_bar = create_bar_chart(
                    account_sales, 
                    x_column='Platform_Account_Display', 
                    y_column='Net Revenue',
                    x_column_name='Account', 
                    y_column_name='Net Revenue (₹)',
                    title='Revenue by Account'
                )
                st.plotly_chart(fig_account_bar, use_container_width=True)
            else: st.info("No data for account breakdown.")
        else: st.info("Account, platform, or revenue data missing for breakdown.")
    
    st.divider()

    # --- Top Products ---
    st.header("Top Performing Products")
    if not sales_df_daily.empty and 'MSKU' in sales_df_daily.columns:
        # Ensure MSKU is not None for grouping, then filter out the placeholder if needed
        top_products_df = sales_df_daily.copy()
        top_products_df['MSKU_Display'] = top_products_df['MSKU'].fillna("Unmapped/Unknown")
        
        top_n = st.slider("Number of Top Products:", min_value=3, max_value=20, value=5, key="top_n_slider_overview")

        # Top by Revenue
        if 'Net Revenue' in top_products_df.columns:
            top_by_revenue = top_products_df.groupby('MSKU_Display', as_index=False)['Net Revenue'].sum().nlargest(top_n, 'Net Revenue')
            if not top_by_revenue.empty:
                st.subheader(f"Top {len(top_by_revenue)} Products by Net Revenue") # Use len in case fewer than top_n
                # Rename MSKU_Display back to MSKU for display if preferred
                top_by_revenue.rename(columns={'MSKU_Display': 'MSKU'}, inplace=True)
                st.dataframe(top_by_revenue.style.format({"Net Revenue": "₹{:,.2f}"}), use_container_width=True)
            else: st.info("No product data for top by revenue.")
        else: st.info("Net Revenue data missing for top products.")

        # Top by Units Sold
        if 'Quantity Sold' in top_products_df.columns:
            top_by_units = top_products_df.groupby('MSKU_Display', as_index=False)['Quantity Sold'].sum().nlargest(top_n, 'Quantity Sold')
            if not top_by_units.empty:
                st.subheader(f"Top {len(top_by_units)} Products by Units Sold")
                top_by_units.rename(columns={'MSKU_Display': 'MSKU'}, inplace=True)
                st.dataframe(top_by_units.style.format({"Quantity Sold": "{:,.0f}"}), use_container_width=True)
            else: st.info("No product data for top by units.")
        else: st.info("Quantity Sold data missing for top products.")
    else:
        st.info("MSKU data not available for top products.")

logger.info("Analytics Overview page loaded/refreshed.")