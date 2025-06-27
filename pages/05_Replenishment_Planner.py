# RMS/pages/06_Replenishment_Planner.py
import streamlit as st
import pandas as pd
from datetime import date , timedelta
import os
import sys

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
if not processed_sales_table_id or not inventory_table_id:
    st.error("`processed_sales_data_table_id` and `inventory_table_id` must be configured in settings.yaml.")
    st.stop()

# Load all data once per session
load_and_cache_analytics_data(fetcher, processed_sales_table_id, inventory_table_id)
all_sales_df = st.session_state.get('analytics_sales_df')
all_inventory_df = st.session_state.get('analytics_inventory_df')

# --- Sidebar Controls ---
st.sidebar.header("Replenishment Parameters")
velocity_period = st.sidebar.selectbox(
    "Calculate Sales Velocity based on last:",
    options=[7, 14, 30, 60, 90],
    index=2, # Default to 30 days
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
            # 1. Get daily sales data for the velocity period (to handle Amazon distribution)
            # We need to go back far enough to cover the velocity period from the last sale date
            end_date = all_sales_df['Sale Date'].max()
            start_date = end_date - timedelta(days=velocity_period - 1)
            
            sales_df_for_velocity = get_sales_data(all_sales_df, start_date, end_date)
            
            # 2. Calculate Sales Velocity
            sales_velocity = calculate_sales_velocity(sales_df_for_velocity, days_period=velocity_period)
            
            # 3. Get Current Inventory
            current_inventory = get_current_inventory(all_inventory_df)
            
            # 4. Prepare Parameter Maps (using defaults for now)
            # In a future version, these could be loaded from a Baserow table or CSV
            lead_times = {'default': default_lead_time}
            stock_cover_days = {'default': default_stock_cover}
            order_cycle_days = {'default': default_order_cycle}
            moqs = {'default': default_moq}
            
            # 5. Calculate Replenishment
            replenishment_plan_df = calculate_replenishment_data(
                current_inventory,
                sales_velocity,
                lead_times,
                stock_cover_days,
                order_cycle_days,
                moqs
            )
            
            # Store in session state for display and editing
            st.session_state.replenishment_plan_df = replenishment_plan_df

# --- Display Replenishment Table ---
if 'replenishment_plan_df' in st.session_state and st.session_state.replenishment_plan_df is not None:
    st.header("Replenishment Plan")
    
    display_df = st.session_state.replenishment_plan_df
    
    if not display_df.empty:
        # For display, let's select and reorder columns
        display_cols = [
            'MSKU', 'Status', 'Current Inventory', 'Avg Daily Sales', 'DOS', 
            'Reorder Point', 'Suggested Order Qty', 'Lead Time (days)', 
            'Stock Cover (days)', 'MOQ', 'Target Stock Level'
        ]
        # Ensure all display columns exist
        display_df_final = display_df[[col for col in display_cols if col in display_df.columns]]

        # Use st.data_editor to make it interactive
        # We can add a column for manual override
        display_df_final['Order Qty Override'] = display_df_final['Suggested Order Qty']

        st.info("You can edit the 'Order Qty Override' column. The table is sorted by status to show items needing attention first.")

        edited_df = st.data_editor(
            display_df_final.sort_values(by='Status'), # Sort to bring "Order Now" to top
            column_config={
                "Status": st.column_config.TextColumn("Status", width="small"),
                "Current Inventory": st.column_config.NumberColumn("Current Inv.", format="%d"),
                "Avg Daily Sales": st.column_config.NumberColumn("Avg Daily Sales", format="%.2f"),
                "DOS": st.column_config.NumberColumn("Days of Stock", format="%.1f"),
                "Reorder Point": st.column_config.NumberColumn(format="%d"),
                "Suggested Order Qty": st.column_config.NumberColumn(format="%d", disabled=True),
                "Order Qty Override": st.column_config.NumberColumn("Final Order Qty", min_value=0, step=10),
            },
            hide_index=True,
            use_container_width=True,
            key="replenishment_editor_final"
        )
        
        # Note: To save edits, you would update the session state with `edited_df`
        # st.session_state.replenishment_plan_df = edited_df # Or a merged version
        
        st.download_button(
            "Download Plan as CSV",
            edited_df.to_csv(index=False).encode('utf-8'),
            f"replenishment_plan_{date.today().strftime('%Y%m%d')}.csv",
            "text/csv",
            key='download-replen-plan'
        )
    else:
        st.success("Calculation complete. No items currently require replenishment based on the parameters.")