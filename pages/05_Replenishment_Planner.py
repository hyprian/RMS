# RMS/pages/06_Replenishment_Planner.py
import streamlit as st
import pandas as pd
from datetime import date
import os
import sys
import numpy as np # Ensure numpy is imported for np.select, etc.
from datetime import datetime, timedelta # Ensure timedelta is imported

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
from analytics_dashboard.data_loader import load_and_cache_analytics_data
from analytics_dashboard.kpi_calculations import (
    process_sales_data_for_analytics as get_sales_data,
    get_current_inventory,
    calculate_sales_velocity
)
from replenishment.replenishment_logic import calculate_replenishment_data

import logging
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Replenishment Planner - RMS", layout="wide")
st.title("🚚 Replenishment Planner")
st.markdown("Analyze sales velocity and inventory to get smart replenishment recommendations.")

# --- Initialize Tools & Load Data into Session State ---
@st.cache_resource
def get_analytics_fetcher_replen():
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        return fetcher
    except Exception as e: st.error(f"Error initializing Baserow connection: {e}"); return None

fetcher = get_analytics_fetcher_replen()
if not fetcher: st.error("Failed to initialize Baserow connection."); st.stop()

processed_sales_table_id = APP_CONFIG['baserow'].get('processed_sales_data_table_id')
inventory_table_id = APP_CONFIG['baserow'].get('inventory_table_id')
category_table_id = APP_CONFIG['baserow'].get('category_table_id')

if not all([processed_sales_table_id, inventory_table_id, category_table_id]):
    st.error("`processed_sales_data_table_id`, `inventory_table_id`, and `category_table_id` must be configured in settings.yaml.")
    st.stop()

load_and_cache_analytics_data(fetcher, processed_sales_table_id, inventory_table_id, category_table_id)
all_sales_df = st.session_state.get('analytics_sales_df')
all_inventory_df = st.session_state.get('analytics_inventory_df')
all_category_df = st.session_state.get('analytics_category_df')

# --- Sidebar Controls ---
st.sidebar.header("Replenishment Parameters")
velocity_period = st.sidebar.selectbox(
    "Calculate Sales Velocity based on last:",
    options=[7, 14, 30, 60, 90],
    index=2,
    format_func=lambda x: f"{x} days"
)
st.sidebar.subheader("Default Parameters")
default_lead_time = st.sidebar.number_input("Supplier Lead Time (days)", min_value=1, value=30)
default_stock_cover = st.sidebar.number_input("Desired Stock Cover (days)", min_value=0, value=15)
default_order_cycle = st.sidebar.number_input("Order Cycle (days)", min_value=1, value=30)
default_moq = st.sidebar.number_input("Minimum Order Quantity (MOQ)", min_value=0, value=0)

# --- Main Page Logic ---
if all_sales_df is None or all_sales_df.empty or all_inventory_df is None or all_inventory_df.empty:
    st.warning("Sales or Inventory data is not available. Please upload sales reports and ensure inventory is synced.")
else:
    st.info(f"Using sales data up to **{all_sales_df['Sale Date'].max()}** and inventory data for **{len(all_inventory_df)} MSKUs**.")
    
    if st.button("Calculate Replenishment Plan", type="primary"):
        with st.spinner("Calculating sales velocity and replenishment needs..."):
            end_date = all_sales_df['Sale Date'].max()
            start_date = end_date - timedelta(days=velocity_period - 1)
            sales_df_for_velocity = get_sales_data(all_sales_df, start_date, end_date)
            sales_velocity = calculate_sales_velocity(sales_df_for_velocity, days_period=velocity_period)
            current_inventory = get_current_inventory(all_inventory_df)
            
            lead_times = {'default': default_lead_time}
            stock_cover_days = {'default': default_stock_cover}
            order_cycle_days = {'default': default_order_cycle}
            moqs = {'default': default_moq}
            
            replenishment_plan_df = calculate_replenishment_data(
                current_inventory, sales_velocity,
                lead_times, stock_cover_days,
                order_cycle_days, moqs
            )
            
            if all_category_df is not None and not all_category_df.empty:
                replenishment_plan_df = pd.merge(
                    replenishment_plan_df, 
                    all_category_df, 
                    on='MSKU', 
                    how='left'
                )
                replenishment_plan_df['Category'].fillna('Uncategorized', inplace=True)
            else:
                replenishment_plan_df['Category'] = 'N/A'
            
            st.session_state.replenishment_plan_df = replenishment_plan_df

# --- Display Replenishment Table ---
if 'replenishment_plan_df' in st.session_state and st.session_state.replenishment_plan_df is not None:
    st.header("Replenishment Plan")
    
    display_df = st.session_state.replenishment_plan_df
    
    if not display_df.empty:
        # --- Filter Section ---
        col_filter1, col_filter2 = st.columns(2)
        
        with col_filter1:
            all_categories = ['All Categories'] + sorted(display_df['Category'].unique().tolist())
            selected_category = st.selectbox("Filter by Category:", options=all_categories)
        
        with col_filter2:
            # --- NEW: Status Filter ---
            all_statuses = ['All Statuses'] + sorted(display_df['Status'].unique().tolist())
            selected_status = st.selectbox("Filter by Status:", options=all_statuses)

        # Apply filters
        if selected_category != 'All Categories':
            display_df = display_df[display_df['Category'] == selected_category]
        if selected_status != 'All Statuses':
            display_df = display_df[display_df['Status'] == selected_status]

        display_cols = [
            'MSKU', 'Category', 'Status', 'Current Inventory', 'Avg Daily Sales', 'DOS', 
            'Reorder Point', 'Suggested Order Qty', 'Lead Time (days)', 
            'Stock Cover (days)', 'MOQ', 'Target Stock Level'
        ]
        display_df_final = display_df[[col for col in display_cols if col in display_df.columns]]
        display_df_final['Order Qty Override'] = display_df_final['Suggested Order Qty']

        st.info("You can edit the 'Order Qty Override' column. The table is sorted by status to show items needing attention first.")

        edited_df = st.data_editor(
            display_df_final.sort_values(by='Status', key=lambda col: col.map({
                '🚨 Order Now': 0, 
                '⚠️ Reorder Soon': 1, 
                '✅ OK': 2, 
                '📈 Overstocked': 3
            })), # Custom sort order
            column_config={
                "MSKU": st.column_config.TextColumn("MSKU", width="medium", disabled=True),
                # --- THIS IS THE FIX ---
                "Category": st.column_config.TextColumn("Category", width="medium", disabled=True),
                # --- END FIX ---
                "Status": st.column_config.TextColumn("Status", width="small", disabled=True),
                "Current Inventory": st.column_config.NumberColumn("Current Inv.", format="%d", disabled=True),
                "Avg Daily Sales": st.column_config.NumberColumn("Avg Daily Sales", format="%.2f", disabled=True),
                "DOS": st.column_config.NumberColumn("Days of Stock", format="%.1f", disabled=True),
                "Reorder Point": st.column_config.NumberColumn(format="%d", disabled=True),
                "Suggested Order Qty": st.column_config.NumberColumn(format="%d", disabled=True),
                "Order Qty Override": st.column_config.NumberColumn("Final Order Qty", min_value=0, step=10),
                "Lead Time (days)": st.column_config.NumberColumn(format="%d", disabled=True),
                "Stock Cover (days)": st.column_config.NumberColumn(format="%d", disabled=True),
                "MOQ": st.column_config.NumberColumn(format="%d", disabled=True),
                "Target Stock Level": st.column_config.NumberColumn(format="%d", disabled=True),
            },
            hide_index=True,
            use_container_width=True,
            key="replenishment_editor_final"
        )
        
        st.download_button(
            "Download Plan as CSV",
            edited_df.to_csv(index=False).encode('utf-8'),
            f"replenishment_plan_{date.today().strftime('%Y%m%d')}.csv",
            "text/csv",
            key='download-replen-plan'
        )
    else:
        st.success("Calculation complete. No items currently require replenishment based on the parameters.")