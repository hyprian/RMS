# RMS/pages/04_Platform_Performance.py
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
from analytics_dashboard.data_loader import load_and_cache_analytics_data
from analytics_dashboard.kpi_calculations import (
    process_sales_data_for_analytics as get_sales_data,
    calculate_total_sales_kpis,
    get_sales_trend_data
)
from analytics_dashboard.charts import create_bar_chart, create_sales_trend_chart
import logging

logger = logging.getLogger(__name__)

st.set_page_config(page_title="Platform Performance - RMS", layout="wide")
st.title("📢 Platform & Account Performance")

# --- Initialize Tools & Load Data into Session State ---
@st.cache_resource
def get_analytics_fetcher_platform():
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        return fetcher
    except Exception as e:
        st.error(f"Error initializing Baserow connection: {e}")
        return None

fetcher = get_analytics_fetcher_platform()
if not fetcher:
    st.error("Failed to initialize Baserow connection. Check configuration.")
    st.stop()

processed_sales_table_id = APP_CONFIG['baserow'].get('processed_sales_data_table_id')
inventory_table_id = APP_CONFIG['baserow'].get('inventory_table_id')
category_table_id = APP_CONFIG['baserow'].get('category_table_id')

if not processed_sales_table_id:
    st.error("`processed_sales_data_table_id` is not configured. Cannot display analytics.")
    st.stop()

# Central data loading for performance
load_and_cache_analytics_data(fetcher, processed_sales_table_id, inventory_table_id, category_table_id)
all_sales_df = st.session_state.get('analytics_sales_df')
all_category_df = st.session_state.get('analytics_category_df')


# --- Sidebar Filters ---
st.sidebar.header("Filters")
default_end_date = date.today()
default_start_date = default_end_date - timedelta(days=29)
selected_start_date = st.sidebar.date_input("Start Date", value=default_start_date, key="platform_start_date")
selected_end_date = st.sidebar.date_input("End Date", value=default_end_date, key="platform_end_date")

if selected_start_date > selected_end_date:
    st.sidebar.error("Start Date cannot be after End Date."); st.stop()

if all_category_df is not None and not all_category_df.empty:
    category_options = ["All Categories"] + sorted(all_category_df['Category'].unique().tolist())
    selected_category_filter = st.sidebar.multiselect("Filter by Category", options=category_options, default=["All Categories"])
else:
    selected_category_filter = []

# --- Process Data (based on date filter only, for platform comparisons) ---
if all_sales_df is None or all_sales_df.empty:
    st.warning("No sales data available. Please upload sales reports on the 'Sales Data Ingestion' page.")
else:
    # Merge category data first
    if all_category_df is not None and not all_category_df.empty:
        sales_df_with_cat = pd.merge(all_sales_df, all_category_df, on='MSKU', how='left')
        sales_df_with_cat['Category'].fillna('Uncategorized', inplace=True)
    else:
        sales_df_with_cat = all_sales_df.copy()
        sales_df_with_cat['Category'] = 'N/A'

    # Apply category filter if selected
    if selected_category_filter and "All Categories" not in selected_category_filter:
        sales_df_with_cat = sales_df_with_cat[sales_df_with_cat['Category'].isin(selected_category_filter)]

    # Now pass this potentially category-filtered DataFrame to get_sales_data
    sales_df_daily = get_sales_data(
        sales_df_with_cat, # Use the df that now includes categories
        selected_start_date,
        selected_end_date
    )

    if sales_df_daily.empty:
        st.warning(f"No sales data found for the selected period ({selected_start_date.strftime('%Y-%m-%d')} to {selected_end_date.strftime('%Y-%m-%d')}).")
    else:
        st.success(f"Displaying performance based on sales from {selected_start_date.strftime('%b %d, %Y')} to {selected_end_date.strftime('%b %d, %Y')}.")
        st.divider()

        # --- Platform Performance Comparison ---
        st.header("Performance by Platform")
        
        # Aggregate data by platform
        platform_performance_df = sales_df_daily.groupby('Platform', as_index=False).agg(
            total_net_revenue=('Net Revenue', 'sum'),
            total_units_sold=('Quantity Sold', 'sum')
        )
        
        if not platform_performance_df.empty:
            # Calculate ASP per platform
            platform_performance_df['average_selling_price'] = platform_performance_df.apply(
                lambda row: row['total_net_revenue'] / row['total_units_sold'] if row['total_units_sold'] > 0 else 0,
                axis=1
            )
            # Rename for display
            platform_performance_df.rename(columns={
                'total_net_revenue': 'Total Net Revenue',
                'total_units_sold': 'Total Units Sold',
                'average_selling_price': 'Avg. Selling Price (ASP)'
            }, inplace=True)
            
            st.dataframe(platform_performance_df.style.format({
                "Total Net Revenue": "₹{:,.2f}",
                "Total Units Sold": "{:,.0f}",
                "Avg. Selling Price (ASP)": "₹{:,.2f}"
            }), use_container_width=True)
        else:
            st.info("No data available for platform comparison.")

        st.divider()

        # --- Account Performance Comparison (within Platforms) ---
        st.header("Performance by Account")
        
        # Create a combined display column
        sales_df_daily['Platform_Account_Display'] = sales_df_daily['Platform'] + " - " + sales_df_daily['Account Name']
        
        account_performance_df = sales_df_daily.groupby('Platform_Account_Display', as_index=False).agg(
            total_net_revenue=('Net Revenue', 'sum'),
            total_units_sold=('Quantity Sold', 'sum')
        )
        
        if not account_performance_df.empty:
            account_performance_df['average_selling_price'] = account_performance_df.apply(
                lambda row: row['total_net_revenue'] / row['total_units_sold'] if row['total_units_sold'] > 0 else 0,
                axis=1
            )
            account_performance_df.rename(columns={
                'Platform_Account_Display': 'Account',
                'total_net_revenue': 'Total Net Revenue',
                'total_units_sold': 'Total Units Sold',
                'average_selling_price': 'Avg. Selling Price (ASP)'
            }, inplace=True)

            st.dataframe(account_performance_df.style.format({
                "Total Net Revenue": "₹{:,.2f}",
                "Total Units Sold": "{:,.0f}",
                "Avg. Selling Price (ASP)": "₹{:,.2f}"
            }), use_container_width=True)
        else:
            st.info("No data available for account comparison.")

        st.divider()

        # --- Platform/Account Trend Comparison ---
        st.header("Sales Trend Comparison")
        
        # Create a multiselect to choose which platforms/accounts to plot
        all_accounts = sorted(sales_df_daily['Platform_Account_Display'].unique())
        selected_accounts_for_trend = st.multiselect(
            "Select Accounts to Compare Trends:",
            options=all_accounts,
            default=all_accounts[:3], # Default to first 3 accounts
            key="platform_trend_account_select"
        )
        
        if selected_accounts_for_trend:
            trend_comparison_df = sales_df_daily[sales_df_daily['Platform_Account_Display'].isin(selected_accounts_for_trend)]
            
            # Aggregate by Date and the combined account display name
            trend_data_grouped = trend_comparison_df.groupby(['Sale Date', 'Platform_Account_Display'], as_index=False)['Net Revenue'].sum()
            
            if not trend_data_grouped.empty:
                # Use Plotly Express with the 'color' argument to create separate lines
                fig_trend_comparison = create_bar_chart(
                    trend_data_grouped,
                    x_column='Sale Date',
                    y_column='Net Revenue',
                    y_column_name='Net Revenue (₹)',
                    color_column='Platform_Account_Display',
                    barmode='group', # 'stack' is also a good option here
                    title='Daily Revenue Comparison by Account'
                )
                st.plotly_chart(fig_trend_comparison, use_container_width=True)
            else:
                st.info("No data for the selected accounts to plot trends.")
        else:
            st.info("Select one or more accounts to compare their sales trends.")

        st.divider()

        # --- Top Products for a Specific Platform/Account ---
        st.header("Top Products within a Channel")
        
        selected_account_for_top = st.selectbox(
            "View Top Products for Account:",
            options=all_accounts,
            key="top_product_account_select"
        )
        
        if selected_account_for_top:
            top_product_df_filtered = sales_df_daily[sales_df_daily['Platform_Account_Display'] == selected_account_for_top]
            
            # Group by MSKU and aggregate
            top_product_df_agg = top_product_df_filtered.groupby('MSKU', as_index=False).agg(
                total_net_revenue=('Net Revenue', 'sum'),
                total_units_sold=('Quantity Sold', 'sum')
            ).nlargest(10, 'total_net_revenue') # Get top 10 by revenue

            if not top_product_df_agg.empty:
                st.subheader(f"Top 10 Products for {selected_account_for_top}")
                st.dataframe(top_product_df_agg.style.format({
                    "total_net_revenue": "₹{:,.2f}",
                    "total_units_sold": "{:,.0f}"
                }), use_container_width=True)
            else:
                st.info(f"No product sales data found for {selected_account_for_top} in this period.")

logger.info("Platform Performance page loaded/refreshed.")