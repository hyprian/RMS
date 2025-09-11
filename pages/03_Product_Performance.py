# RMS/pages/03_Product_Performance.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
import os
import sys
import numpy as np

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
from analytics_dashboard.data_loader import load_and_cache_analytics_data
from analytics_dashboard.kpi_calculations import (
    process_sales_data_for_analytics,
    calculate_total_sales_kpis,
    get_sales_trend_data,
    calculate_profit_data,
    calculate_total_profit_kpis
)
from analytics_dashboard.charts import create_sales_trend_chart, create_pie_chart, create_bar_chart

import logging
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Product Performance - RMS", layout="wide")
st.title("📦 Product Performance Analysis")

# --- Initialize Tools & Load Data ---
@st.cache_resource
def get_analytics_fetcher_prod():
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
category_table_id = APP_CONFIG['baserow'].get('category_table_id') 
catalogue_table_id = APP_CONFIG['baserow'].get('catalogue_table_id') 

load_and_cache_analytics_data(fetcher, processed_sales_table_id, inventory_table_id, category_table_id, catalogue_table_id)

all_sales_df = st.session_state.get('analytics_sales_df')
all_inventory_df = st.session_state.get('analytics_inventory_df')
all_category_df = st.session_state.get('analytics_category_df') 

if all_sales_df is None or all_sales_df.empty:
    st.warning("No sales data available. Please upload sales reports on the 'Sales Data Ingestion' page.")
    st.stop()

# --- MSKU Selector ---
sales_mskust = set(all_sales_df['MSKU'].dropna().unique())
inventory_mskust = set()
if all_inventory_df is not None and not all_inventory_df.empty and 'MSKU' in all_inventory_df.columns:
    inventory_mskust = set(all_inventory_df['MSKU'].dropna().unique())
all_available_mskust = sorted(list(sales_mskust | inventory_mskust))

if not all_available_mskust:
    st.warning("No MSKUs found. Cannot display product performance.")
    st.stop()

# --- CREATE MAIN TABS ---
main_tab1, main_tab2 = st.tabs(["Single Product View", "📊 Product Comparison"])

# ==============================================================================
# --- SINGLE PRODUCT VIEW TAB ---
# ==============================================================================
with main_tab1:
    st.sidebar.header("Single Product Filters")
    selected_msku = st.sidebar.selectbox("Select MSKU:", options=all_available_mskust, key="product_perf_msku_select")
    default_end_date_prod = date.today()
    default_start_date_prod = default_end_date_prod - timedelta(days=89)
    selected_start_date_prod = st.sidebar.date_input("Start Date", value=default_start_date_prod, key="product_perf_start_date")
    selected_end_date_prod = st.sidebar.date_input("End Date", value=default_end_date_prod, key="product_perf_end_date")
    if selected_start_date_prod > selected_end_date_prod: st.sidebar.error("Start Date cannot be after End Date."); st.stop()

    # --- Display MSKU Info ---
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
    if selected_msku:
        sales_df_msku_daily = process_sales_data_for_analytics(
            all_sales_df, selected_start_date_prod, selected_end_date_prod, mskust_list=[selected_msku]
        )

        if sales_df_msku_daily is None or sales_df_msku_daily.empty:
            st.warning(f"No sales data found for MSKU '{selected_msku}' in the selected period.")
        else:
            profit_df_msku = calculate_profit_data(sales_df_msku_daily, all_inventory_df)
            has_profit_data = 'Cost' in profit_df_msku.columns and profit_df_msku['Cost'].sum() > 0
            
            tabs_to_show = ["📈 Performance Overview"]
            if has_profit_data:
                tabs_to_show.append("💰 Profitability Breakdown")
            
            tabs = st.tabs(tabs_to_show)
            
            with tabs[0]: # "Performance Overview" Tab
                st.subheader("Key Performance Indicators (KPIs)")
                kpis_msku = calculate_total_sales_kpis(profit_df_msku)
                profit_kpis_msku = calculate_total_profit_kpis(profit_df_msku)
                
                kpi_cols = st.columns(4)
                kpi_cols[0].metric("Total Net Revenue", f"₹{kpis_msku['total_net_revenue']:,.2f}")
                if has_profit_data:
                    kpi_cols[1].metric("Total Gross Profit", f"₹{profit_kpis_msku['total_gross_profit']:,.2f}")
                    kpi_cols[2].metric("Gross Margin", f"{profit_kpis_msku['gross_margin']:.2f}%")
                kpi_cols[3].metric("Total Units Sold", f"{kpis_msku['total_units_sold']:,.0f}")
                st.divider()

                st.subheader("Performance Trends")
                trend_freq_prod = st.selectbox("Trend Granularity:", options=['D', 'W', 'M'], format_func=lambda x: {'D':'Daily', 'W':'Weekly', 'M':'Monthly'}[x], key="product_trend_freq_selector")
                
                trend_data = profit_df_msku.set_index(pd.to_datetime(profit_df_msku['Sale Date'])).resample(trend_freq_prod).agg({
                    'Net Revenue': 'sum', 'Gross Profit': 'sum', 'Quantity Sold': 'sum'
                }).reset_index()

                if not trend_data.empty:
                    fig_revenue_trend = create_sales_trend_chart(trend_data, y_column='Net Revenue', y_column_name="Net Revenue (₹)", title=f"Net Revenue Trend")
                    st.plotly_chart(fig_revenue_trend, use_container_width=True)
                    if has_profit_data:
                        fig_profit_trend = create_sales_trend_chart(trend_data, y_column='Gross Profit', y_column_name="Gross Profit (₹)", title=f"Gross Profit Trend")
                        st.plotly_chart(fig_profit_trend, use_container_width=True)
                    fig_units_trend = create_sales_trend_chart(trend_data, y_column='Quantity Sold', y_column_name="Units Sold", title=f"Units Sold Trend")
                    st.plotly_chart(fig_units_trend, use_container_width=True)
                else:
                    st.info("Not enough data to display trends.")
                
                # --- RESTORED: Sales Breakdown by Platform & Account ---
                st.divider()
                st.subheader("Sales Breakdown by Channel")
                col_plat_prod, col_acc_prod = st.columns(2)

                with col_plat_prod:
                    st.markdown("##### By Platform")
                    platform_sales_msku = sales_df_msku_daily.groupby('Platform', as_index=False)['Net Revenue'].sum()
                    if not platform_sales_msku.empty:
                        fig_plat_pie_msku = create_pie_chart(platform_sales_msku, names_column='Platform', values_column='Net Revenue', title=f'Revenue by Platform')
                        st.plotly_chart(fig_plat_pie_msku, use_container_width=True)
                    else: st.info("No platform breakdown data.")
                        
                with col_acc_prod:
                    st.markdown("##### By Account")
                    sales_df_msku_daily['Platform_Account_Display'] = sales_df_msku_daily['Platform'] + " - " + sales_df_msku_daily['Account Name']
                    account_sales_msku = sales_df_msku_daily.groupby('Platform_Account_Display', as_index=False)['Net Revenue'].sum()
                    if not account_sales_msku.empty:
                        fig_acc_pie_msku = create_pie_chart(account_sales_msku, names_column='Platform_Account_Display', values_column='Net Revenue', title=f'Revenue by Account')
                        st.plotly_chart(fig_acc_pie_msku, use_container_width=True)
                    else: st.info("No account breakdown data.")
                # --- END RESTORED SECTION ---

            # Conditionally create the second tab only if there is profit data
            if has_profit_data:
                with tabs[1]: # "Profitability Breakdown" Tab
                    st.subheader("Profitability Breakdown by Platform & Account")
                    
                    # We already calculated platform_profit_summary in the previous version
                    platform_profit_summary = profit_df_msku.groupby(['Platform', 'Account Name'], as_index=False).agg(
                        total_units_sold=('Quantity Sold', 'sum'),
                        total_net_revenue=('Net Revenue', 'sum'),
                        total_cogs=('Total COGS', 'sum'),
                        total_gross_profit=('Gross Profit', 'sum')
                    )
                    platform_profit_summary['gross_margin_%'] = np.where(
                        platform_profit_summary['total_net_revenue'] > 0,
                        (platform_profit_summary['total_gross_profit'] / platform_profit_summary['total_net_revenue']) * 100, 0
                    )
                    
                    st.dataframe(
                        platform_profit_summary.sort_values(by='total_gross_profit', ascending=False),
                        column_config={
                            "total_units_sold": st.column_config.NumberColumn("Units Sold", format="%d"),
                            "total_net_revenue": st.column_config.NumberColumn("Net Revenue", format="₹%,.2f"),
                            "total_cogs": st.column_config.NumberColumn("Total COGS", format="₹%,.2f"),
                            "total_gross_profit": st.column_config.NumberColumn("Gross Profit", format="₹%,.2f"),
                            "gross_margin_%": st.column_config.NumberColumn("Gross Margin", format="%.2f%%"),
                        },
                        use_container_width=True, hide_index=True
                    )

                    # --- NEW: Profit Breakdown Charts ---
                    st.divider()
                    profit_chart_col1, profit_chart_col2 = st.columns(2)
                    with profit_chart_col1:
                        # Chart showing which platform contributes the most profit
                        platform_profit_chart_data = platform_profit_summary.groupby('Platform', as_index=False)['total_gross_profit'].sum()
                        fig_plat_profit_pie = create_pie_chart(
                            platform_profit_chart_data, 
                            names_column='Platform', 
                            values_column='total_gross_profit', 
                            title='Gross Profit by Platform'
                        )
                        st.plotly_chart(fig_plat_profit_pie, use_container_width=True)
                    
                    with profit_chart_col2:
                        # Chart showing which platform has the best margin
                        # Calculate weighted average margin per platform
                        platform_margin_data = platform_profit_summary.groupby('Platform', as_index=False).agg(
                            total_net_revenue=('total_net_revenue', 'sum'),
                            total_gross_profit=('total_gross_profit', 'sum')
                        )
                        platform_margin_data['gross_margin_%'] = np.where(
                            platform_margin_data['total_net_revenue'] > 0,
                            (platform_margin_data['total_gross_profit'] / platform_margin_data['total_net_revenue']) * 100, 0
                        )
                        fig_plat_margin_bar = create_bar_chart(
                            platform_margin_data,
                            x_column='Platform',
                            y_column='gross_margin_%',
                            y_column_name='Gross Margin %',
                            title='Gross Margin % by Platform'
                        )
                        st.plotly_chart(fig_plat_margin_bar, use_container_width=True)
    else:
        st.info("Select an MSKU from the sidebar to view its performance details.")

# ==============================================================================
# --- PRODUCT COMPARISON TAB ---
# ==============================================================================
with main_tab2:
    st.header("Compare Product Performance")
    
    # --- Sidebar Filters for Comparison ---
    # Note: These filters will appear when this tab is active.
    # We can reuse the date filters from the other tab if we want them to be linked,
    # but separate keys allow for independent date ranges.
    st.sidebar.header("Comparison Filters")
    selected_mskust_to_compare = st.sidebar.multiselect(
        "Select up to 4 MSKUs to compare:",
        options=all_available_mskust,
        max_selections=4,
        key="product_compare_multiselect"
    )
    
    comp_end_date = st.sidebar.date_input("End Date", value=date.today(), key="product_compare_end_date")
    comp_start_date = st.sidebar.date_input("Start Date", value=comp_end_date - timedelta(days=29), key="product_compare_start_date")
    if comp_start_date > comp_end_date: st.sidebar.error("Start Date cannot be after End Date."); st.stop()

    if not selected_mskust_to_compare:
        st.info("Select 2 to 4 MSKUs from the sidebar to start a comparison.")
    else:
        # Fetch data for ALL selected MSKUs at once
        comparison_sales_df = process_sales_data_for_analytics(
            all_sales_df, comp_start_date, comp_end_date, mskust_list=selected_mskust_to_compare
        )
        
        if comparison_sales_df.empty:
            st.warning("No sales data found for the selected MSKUs in this period.")
        else:
            # --- Display KPIs Side-by-Side ---
            st.subheader("Key Performance Indicators (KPIs)")
            kpi_cols = st.columns(len(selected_mskust_to_compare))
            
            for i, msku in enumerate(selected_mskust_to_compare):
                with kpi_cols[i]:
                    st.markdown(f"##### {msku}")
                    # Filter the already fetched data for this specific MSKU
                    msku_data = comparison_sales_df[comparison_sales_df['MSKU'] == msku]
                    
                    if msku_data.empty:
                        st.text("No sales in period.")
                        continue
                    
                    kpis = calculate_total_sales_kpis(msku_data)
                    st.metric("Net Revenue", f"₹{kpis['total_net_revenue']:,.2f}")
                    st.metric("Units Sold", f"{kpis['total_units_sold']:,.0f}")
                    st.metric("Avg. Selling Price", f"₹{kpis['average_selling_price']:,.2f}")

            st.divider()

            # --- Display Combined Trend Chart ---
            st.subheader("Sales Trend Comparison")
            
            # Group by both Date and MSKU to get trends for each
            trend_data_grouped = comparison_sales_df.groupby(['Sale Date', 'MSKU'], as_index=False)['Net Revenue'].sum()
            
            if not trend_data_grouped.empty:
                # Use Plotly Express with the 'color' argument to create separate lines for each MSKU
                fig_comparison_trend = create_sales_trend_chart(
                    trend_data_grouped,
                    y_column='Net Revenue',
                    y_column_name="Net Revenue (₹)",
                    title="Net Revenue Trend Comparison",
                    color_column='MSKU' # This is the key to plotting multiple lines
                )
                st.plotly_chart(fig_comparison_trend, use_container_width=True)
            else:
                st.info("Not enough data to plot trends.")
                
            st.divider()

            # --- NEW: Sales Breakdown by Channel Section ---
            st.subheader("Sales Breakdown by Channel")
            
            # 1. Breakdown by Platform
            platform_breakdown_df = comparison_sales_df.groupby(['MSKU', 'Platform'], as_index=False)['Net Revenue'].sum()
            if not platform_breakdown_df.empty:
                fig_platform_breakdown = create_bar_chart(
                    platform_breakdown_df,
                    x_column='MSKU',
                    y_column='Net Revenue',
                    y_column_name='Net Revenue (₹)',
                    color_column='Platform', # Use Platform to create grouped bars
                    barmode='group', # 'group' for side-by-side, 'stack' for stacked
                    title='Revenue by Platform for Selected MSKUs'
                )
                st.plotly_chart(fig_platform_breakdown, use_container_width=True)
            else:
                st.info("Not enough data for platform breakdown.")

            # 2. Breakdown by Account
            comparison_sales_df['Platform_Account_Display'] = comparison_sales_df['Platform'] + " - " + comparison_sales_df['Account Name']
            account_breakdown_df = comparison_sales_df.groupby(['MSKU', 'Platform_Account_Display'], as_index=False)['Net Revenue'].sum()
            if not account_breakdown_df.empty:
                fig_account_breakdown = create_bar_chart(
                    account_breakdown_df,
                    x_column='MSKU',
                    y_column='Net Revenue',
                    y_column_name='Net Revenue (₹)',
                    color_column='Platform_Account_Display', # Use Account to create grouped bars
                    barmode='group',
                    title='Revenue by Account for Selected MSKUs'
                )
                st.plotly_chart(fig_account_breakdown, use_container_width=True)
            else:
                st.info("Not enough data for account breakdown.")
            # --- END NEW SECTION ---

logger.info("Product Performance page loaded/refreshed.")