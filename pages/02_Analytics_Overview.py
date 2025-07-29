# RMS/pages/02_Analytics_Overview.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date 
import os
import numpy as np
import sys

# Adjust project_root to be the actual RMS directory
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
# --- MODIFIED IMPORTS --- 
from analytics_dashboard.data_loader import load_and_cache_analytics_data # NEW
from analytics_dashboard.kpi_calculations import (
    process_sales_data_for_analytics as get_sales_data, # Using the renamed processing function
    calculate_total_sales_kpis,
    get_sales_trend_data,
    calculate_profit_data 
)
from analytics_dashboard.charts import (
    create_sales_trend_chart,
    create_pie_chart,
    create_bar_chart
)
import logging

logger = logging.getLogger(__name__)

st.set_page_config(page_title="Analytics Overview - RMS", layout="wide")
st.title("📈 Analytics Overview Dashboard")

# --- Initialize Tools & Config ---
@st.cache_resource
def get_analytics_fetcher_overview(): # Renamed to avoid conflicts
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        return fetcher
    except Exception as e:
        st.error(f"Error initializing Baserow connection: {e}")
        return None

fetcher = get_analytics_fetcher_overview()
if not fetcher:
    st.error("Failed to initialize Baserow connection for analytics. Check configuration.")
    st.stop()

processed_sales_table_id = APP_CONFIG['baserow'].get('processed_sales_data_table_id')
inventory_table_id = APP_CONFIG['baserow'].get('inventory_table_id')
category_table_id = APP_CONFIG['baserow'].get('category_table_id')
catalogue_table_id = APP_CONFIG['baserow'].get('catalogue_table_id')

if not processed_sales_table_id:
    st.error("`processed_sales_data_table_id` is not configured in settings.yaml. Cannot display analytics.")
    st.stop()

platforms_config = APP_CONFIG.get('platforms', [])

# --- CENTRAL DATA LOADING (PERFORMANCE IMPROVEMENT) ---
# Load all data once per session. This is the key performance improvement.
# You can add a button to force a reload if needed.
if 'force_reload_analytics' not in st.session_state:
    st.session_state.force_reload_analytics = False

if st.sidebar.button("Force Reload Data from Baserow"):
    st.session_state.force_reload_analytics = True

load_and_cache_analytics_data(fetcher, processed_sales_table_id, inventory_table_id, category_table_id, catalogue_table_id,force_reload=st.session_state.force_reload_analytics)
# Reset the flag after use
st.session_state.force_reload_analytics = False

# Now, get the pre-loaded data from session state
all_sales_df = st.session_state.get('analytics_sales_df')
all_inventory_df = st.session_state.get('analytics_inventory_df')
all_category_df = st.session_state.get('analytics_category_df') 

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
account_options = sorted(list(set(account_options)))

selected_accounts_display = st.sidebar.multiselect("Accounts", options=account_options, default=["All Accounts"], key="analytics_accounts")

filter_platforms = None
if selected_platforms_names and "All Platforms" not in selected_platforms_names:
    filter_platforms = selected_platforms_names

filter_accounts = None
if selected_accounts_display and "All Accounts" not in selected_accounts_display:
    filter_accounts = [acc.split(" - ")[-1].strip() for acc in selected_accounts_display]


# --- Process Data (Now in-memory, very fast) ---
if all_sales_df is None or all_sales_df.empty:
    st.warning("No sales data available. Please upload sales reports on the 'Sales Data Ingestion' page.")
else:
    sales_tab, profit_tab = st.tabs(["📈 Sales Performance", "💰 Profitability Analysis"])

    with sales_tab:

        # Filter the already-loaded data. The `get_sales_data` function is now for processing.
        sales_df_daily = get_sales_data(
            all_sales_df,
            selected_start_date,
            selected_end_date,
            platforms=filter_platforms,
            accounts=filter_accounts
        )

        # --- NEW: Merge category data into the daily sales data for analytics ---
        if all_category_df is not None and not all_category_df.empty:
            sales_df_daily = pd.merge(sales_df_daily, all_category_df, on='MSKU', how='left')
            sales_df_daily['Category'].fillna('Uncategorized', inplace=True)
        else:
            sales_df_daily['Category'] = 'N/A'

        if sales_df_daily.empty:
            st.warning(f"No sales data found for the selected filters and period ({selected_start_date.strftime('%Y-%m-%d')} to {selected_end_date.strftime('%Y-%m-%d')}).")
        else:
            st.success(f"Displaying analytics based on {len(sales_df_daily)} processed daily sales records.")
            
            kpis = calculate_total_sales_kpis(sales_df_daily)
            st.header("Key Performance Indicators (KPIs)")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Net Revenue", f"₹{kpis['total_net_revenue']:,.2f}")
            col2.metric("Total Units Sold", f"{kpis['total_units_sold']:,.0f}")
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

            trend_data_units = get_sales_trend_data(sales_df_daily, freq=trend_freq) 
            if not trend_data_units.empty and 'Quantity Sold' in trend_data_units.columns:
                fig_units_trend = create_sales_trend_chart(trend_data_units, y_column='Quantity Sold', y_column_name="Units Sold", title=f"{ {'D':'Daily', 'W':'Weekly', 'M':'Monthly'}[trend_freq]} Units Sold Trend")
                st.plotly_chart(fig_units_trend, use_container_width=True)
            else: st.info("Not enough data for units sold trend at selected granularity.")
            st.divider()

            st.header("Sales Breakdown")

            # --- NEW: Add Category Breakdown ---
            st.subheader("By Category")
            if 'Category' in sales_df_daily.columns:
                category_sales = sales_df_daily.groupby('Category', as_index=False)['Net Revenue'].sum().sort_values(by='Net Revenue', ascending=False)
                if not category_sales.empty:
                    fig_category_bar = create_bar_chart(
                        category_sales, x_column='Category', y_column='Net Revenue',
                        y_column_name='Net Revenue (₹)', title='Revenue by Product Category'
                    )
                    st.plotly_chart(fig_category_bar, use_container_width=True)
            
            st.divider()
            
            col_plat, col_acc = st.columns(2)

            with col_plat:
                st.subheader("By Platform")
                if 'Platform' in sales_df_daily.columns:
                    platform_sales = sales_df_daily.groupby('Platform', as_index=False)['Net Revenue'].sum()
                    platform_sales = platform_sales.sort_values(by='Net Revenue', ascending=False)
                    if not platform_sales.empty:
                        fig_platform_bar = create_bar_chart(platform_sales, x_column='Platform', y_column='Net Revenue', y_column_name='Net Revenue (₹)', title='Revenue by Platform')
                        st.plotly_chart(fig_platform_bar, use_container_width=True)
                    else: st.info("No data for platform breakdown.")
                else: st.info("Platform data missing.")
                    
            with col_acc:
                st.subheader("By Account")
                if 'Account Name' in sales_df_daily.columns and 'Platform' in sales_df_daily.columns:
                    sales_df_daily['Platform_Account_Display'] = sales_df_daily['Platform'] + " - " + sales_df_daily['Account Name']
                    account_sales = sales_df_daily.groupby('Platform_Account_Display', as_index=False)['Net Revenue'].sum()
                    account_sales = account_sales.sort_values(by='Net Revenue', ascending=False)
                    if not account_sales.empty:
                        fig_account_bar = create_bar_chart(account_sales, x_column='Platform_Account_Display', y_column='Net Revenue', x_column_name='Account', y_column_name='Net Revenue (₹)', title='Revenue by Account')
                        st.plotly_chart(fig_account_bar, use_container_width=True)
                    else: st.info("No data for account breakdown.")
                else: st.info("Account/Platform data missing.")
            
            st.divider()

            st.header("Top Performing Products")
            if 'MSKU' in sales_df_daily.columns:
                top_products_df = sales_df_daily.copy()
                top_products_df['MSKU_Display'] = top_products_df['MSKU'].fillna("Unmapped/Unknown")
                
                top_n = st.slider("Number of Top Products:", min_value=3, max_value=20, value=5, key="top_n_slider_overview")

                if 'Net Revenue' in top_products_df.columns:
                    top_by_revenue = top_products_df.groupby('MSKU_Display', as_index=False)['Net Revenue'].sum().nlargest(top_n, 'Net Revenue')
                    if not top_by_revenue.empty:
                        st.subheader(f"Top {len(top_by_revenue)} Products by Net Revenue")
                        top_by_revenue.rename(columns={'MSKU_Display': 'MSKU'}, inplace=True)
                        st.dataframe(top_by_revenue.style.format({"Net Revenue": "₹{:,.2f}"}), use_container_width=True)
                    else: st.info("No product data for top by revenue.")
                else: st.info("Net Revenue data missing.")

                if 'Quantity Sold' in top_products_df.columns:
                    top_by_units = top_products_df.groupby('MSKU_Display', as_index=False)['Quantity Sold'].sum().nlargest(top_n, 'Quantity Sold')
                    if not top_by_units.empty:
                        st.subheader(f"Top {len(top_by_units)} Products by Units Sold")
                        top_by_units.rename(columns={'MSKU_Display': 'MSKU'}, inplace=True)
                        st.dataframe(top_by_units.style.format({"Quantity Sold": "{:,.0f}"}), use_container_width=True)
                    else: st.info("No product data for top by units.")
                else: st.info("Quantity Sold data missing.")
            else:
                st.info("MSKU data not available for top products.")

    logger.info("Analytics Overview page loaded/refreshed.")

    with profit_tab:
        st.header("Profitability Analysis")
        # 1. Get the same daily sales data as the sales tab
        sales_df_daily_for_profit = get_sales_data(
            all_sales_df, selected_start_date, selected_end_date,
            platforms=filter_platforms, accounts=filter_accounts
        )

        if sales_df_daily_for_profit.empty:
            st.warning(f"No sales data found for the selected filters to analyze profit.")
        else:
            # 2. Calculate profit data by merging with inventory/cost data
            profit_df = calculate_profit_data(sales_df_daily_for_profit, all_inventory_df)
            
            # 3. Calculate Profit KPIs
            total_gross_profit = profit_df['Gross Profit'].sum()
            total_net_revenue = profit_df['Net Revenue'].sum()
            gross_margin = (total_gross_profit / total_net_revenue * 100) if total_net_revenue > 0 else 0
            
            st.header("Profitability KPIs")
            kpi_cols = st.columns(3)
            kpi_cols[0].metric("Total Gross Profit", f"₹{total_gross_profit:,.2f}")
            kpi_cols[1].metric("Gross Margin", f"{gross_margin:.2f}%")
            kpi_cols[2].metric("Total COGS", f"₹{profit_df['Total COGS'].sum():,.2f}")
            st.divider()

            # 4. Profit Trend Chart
            st.header("Profit Trend")
            profit_trend_data = get_sales_trend_data(profit_df, freq='D') # Use daily for now
            if not profit_trend_data.empty:
                # We need to add 'Gross Profit' to the aggregation in get_sales_trend_data
                # Quick fix here, but better to update the function
                profit_trend_data = profit_df.set_index(pd.to_datetime(profit_df['Sale Date'])).resample('D').agg({
                    'Net Revenue': 'sum',
                    'Gross Profit': 'sum'
                }).reset_index()

                fig_profit_trend = create_sales_trend_chart(profit_trend_data, y_column='Gross Profit', y_column_name="Gross Profit (₹)", title="Daily Gross Profit Trend")
                st.plotly_chart(fig_profit_trend, use_container_width=True)
            st.divider()

            # 5. Detailed Profitability Table per MSKU
            st.header("Profitability by Product (MSKU)")
            
            # Merge with product name and category for a richer table
            if all_inventory_df is not None:
                profit_df = pd.merge(profit_df, all_inventory_df[['MSKU', 'Product Name']], on='MSKU', how='left')
            if all_category_df is not None:
                profit_df = pd.merge(profit_df, all_category_df[['MSKU', 'Category']], on='MSKU', how='left')
            
            msku_profit_summary = profit_df.groupby(['MSKU', 'Product Name', 'Category'], as_index=False).agg(
                total_units_sold=('Quantity Sold', 'sum'),
                total_net_revenue=('Net Revenue', 'sum'),
                total_cogs=('Total COGS', 'sum'),
                total_gross_profit=('Gross Profit', 'sum')
            )
            msku_profit_summary['gross_margin_%'] = np.where(
                msku_profit_summary['total_net_revenue'] > 0,
                (msku_profit_summary['total_gross_profit'] / msku_profit_summary['total_net_revenue']) * 100,
                0
            )
            
            st.dataframe(
                msku_profit_summary.sort_values(by='total_gross_profit', ascending=False),
                column_config={
                    "MSKU": st.column_config.TextColumn(width="medium"),
                    "Product Name": st.column_config.TextColumn(width="large"),
                    "total_units_sold": st.column_config.NumberColumn("Units Sold", format="%d"),
                    
                    # Corrected format strings below
                    "total_net_revenue": st.column_config.NumberColumn("Net Revenue"),
                    "total_cogs": st.column_config.NumberColumn("Total COGS"),
                    "total_gross_profit": st.column_config.NumberColumn("Gross Profit"),
                    "gross_margin_%": st.column_config.NumberColumn("Gross Margin", format="%.2f%%"),
                },
                use_container_width=True,
                hide_index=True
            )