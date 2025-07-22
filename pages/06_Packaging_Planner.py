# RMS/pages/11_Packaging_Planner.py
import streamlit as st
import pandas as pd
import os
import sys
from datetime import date

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
from packaging_module.packaging_logic import (
    process_outbound_to_daily_consumption,
    calculate_packaging_velocity,
    calculate_packaging_replenishment
)

import logging
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Packaging Planner - RMS", layout="wide")
st.title("📦 Packaging Material Replenishment Planner")
st.markdown("Analyze packaging material consumption to get smart replenishment recommendations.")

# --- Initialize Tools & Config ---
@st.cache_resource
def get_packaging_tools():
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        return fetcher
    except Exception as e:
        st.error(f"Error initializing Baserow connection: {e}"); return None

fetcher = get_packaging_tools()
if not fetcher:
    st.error("Failed to initialize Baserow connection."); st.stop()

outbound_table_id = APP_CONFIG['baserow'].get('automated_outbound_table_id')
inventory_table_id = APP_CONFIG['baserow'].get('packaging_inventory_table_id')

# st.warning(f"DEBUG: The app is using the following Table IDs for this page:")
# st.code(f"""
# Automated Outbound Table ID: {outbound_table_id} (Type: {type(outbound_table_id)})
# Packaging Inventory Table ID: {inventory_table_id} (Type: {type(inventory_table_id)})
# """)

if not outbound_table_id or not inventory_table_id:
    st.error("`automated_outbound_table_id` and `packaging_inventory_table_id` must be configured in settings.yaml.")
    st.stop()

# --- Data Loading and Caching ---
@st.cache_data(ttl=3600) # Cache for 1 hour
def load_packaging_data(_fetcher, _outbound_id, _inventory_id):
    with st.spinner("Loading packaging consumption and inventory data..."):
        
        # --- TEMPORARY DEBUGGING BLOCK ---
        # st.subheader("Debug: Raw Data Fetch")
        
        # Step 1: Fetch raw outbound data
        outbound_df = _fetcher.get_outbound_packaging_data(_outbound_id)
        # st.write("1. Raw Outbound Data (`outbound_df`):")
        # if outbound_df is None:
        #     st.error("`outbound_df` is None.")
        # elif outbound_df.empty:
        #     # --- CORRECTED WARNING TEXT ---
        #     st.warning("`outbound_df` is empty. Check `get_outbound_packaging_data` function in fetcher, Baserow table name/permissions, and required column names ('Date', 'Packing material').")
        # else:
        #     st.success(f"Fetched {len(outbound_df)} raw outbound records.")
        #     st.dataframe(outbound_df.head())

        # Step 2: Fetch raw inventory data
        inventory_df = _fetcher.get_packaging_inventory(_inventory_id)
        # st.write("2. Raw Inventory Data (`inventory_df`):")
        # if inventory_df is None:
        #     st.error("`inventory_df` is None.")
        # elif inventory_df.empty:
        #     st.warning("`inventory_df` is empty. Check `get_packaging_inventory` function in fetcher, Baserow table name/permissions, and required column names ('material', 'Current Inventory').")
        # else:
        #     st.success(f"Fetched {len(inventory_df)} raw inventory records.")
        #     st.dataframe(inventory_df.head())

        # Step 3: Process outbound to daily consumption
        daily_consumption_df = process_outbound_to_daily_consumption(outbound_df)
        # st.write("3. Processed Daily Consumption (`daily_consumption_df`):")
        # if daily_consumption_df is None:
        #     st.error("`daily_consumption_df` is None.")
        # elif daily_consumption_df.empty:
        #     st.warning("`daily_consumption_df` is empty. This means `process_outbound_to_daily_consumption` failed or the input `outbound_df` was empty.")
        # else:
        #     st.success(f"Processed into {len(daily_consumption_df)} daily consumption records.")
        #     st.dataframe(daily_consumption_df.head())
        
        st.divider()
        # # --- END TEMPORARY DEBUGGING BLOCK ---
        
        return daily_consumption_df, inventory_df

# Load the data
daily_consumption_df, inventory_df = load_packaging_data(fetcher, outbound_table_id, inventory_table_id)
# --- Sidebar Controls ---
st.sidebar.header("Replenishment Parameters")
if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

velocity_period = st.sidebar.selectbox(
    "Calculate Usage Velocity based on last:",
    options=[7, 14, 30, 60, 90],
    index=2, # Default to 30 days
    format_func=lambda x: f"{x} days"
)
st.sidebar.subheader("Default Parameters")
default_lead_time = st.sidebar.number_input("Supplier Lead Time (days)", min_value=1, value=15)
default_stock_cover = st.sidebar.number_input("Desired Stock Cover (days)", min_value=0, value=7)

# --- Main Page Logic ---
if daily_consumption_df.empty or inventory_df.empty:
    st.warning("Consumption or Inventory data for packaging is not available. The `Automated Outbound Daily` table might be empty or lack recent data.")
else:
    st.info(f"Using consumption data up to **{daily_consumption_df['Date'].max()}** and inventory for **{len(inventory_df)} materials**.")
    
    # --- KPI Cards ---
    st.header("Quick Insights")
    velocity_for_kpi = calculate_packaging_velocity(daily_consumption_df, days_period=velocity_period)
    
    if not velocity_for_kpi.empty:
        top_3_used = velocity_for_kpi.nlargest(3)
        
        kpi_cols = st.columns(3)
        for i, (material, usage) in enumerate(top_3_used.items()):
            with kpi_cols[i]:
                st.metric(f"Top Used: {material}", f"{usage:.2f} / day")
    
    # --- Calculate and Display Replenishment Plan ---
    if st.button("Calculate Replenishment Plan", type="primary"):
        with st.spinner("Calculating replenishment needs..."):
            # 1. Calculate Sales Velocity
            packaging_velocity = calculate_packaging_velocity(daily_consumption_df, days_period=velocity_period)
            
            # 2. Calculate Replenishment
            replenishment_plan_df = calculate_packaging_replenishment(
                inventory_df,
                packaging_velocity,
                default_lead_time,
                default_stock_cover
            )
            
            st.session_state.packaging_replenishment_plan_df = replenishment_plan_df

# --- Display Replenishment Table ---
if 'packaging_replenishment_plan_df' in st.session_state and st.session_state.packaging_replenishment_plan_df is not None:
    st.header("Packaging Replenishment Plan")
    
    display_df = st.session_state.packaging_replenishment_plan_df
    
    if not display_df.empty:
        # Define columns and their order for display
        display_cols = [
            'Material Name', 'Status', 'Current Inventory', 'Avg Daily Usage', 
            'DOS', 'Reorder Point', 'Suggested Order Qty'
        ]
        display_df_final = display_df[[col for col in display_cols if col in display_df.columns]]
        display_df_final['Order Qty Override'] = display_df_final['Suggested Order Qty']

        st.info("You can edit the 'Order Qty Override' column. The table is sorted by urgency.")

        edited_df = st.data_editor(
            display_df_final.sort_values(by='Status', key=lambda col: col.map({
                '🚨 Order Now': 0, 
                '⚠️ Reorder Soon': 1, 
                '✅ OK': 2, 
                '📈 Overstocked': 3
            })),
            column_config={
                "Material Name": st.column_config.TextColumn("Material", width="large", disabled=True),
                "Status": st.column_config.TextColumn(width="small", disabled=True),
                "Current Inventory": st.column_config.NumberColumn("Current Stock", format="%d", disabled=True),
                "Avg Daily Usage": st.column_config.NumberColumn(format="%.2f", disabled=True),
                "DOS": st.column_config.NumberColumn("Days of Stock", format="%.1f", disabled=True),
                "Reorder Point": st.column_config.NumberColumn(format="%d", disabled=True),
                "Suggested Order Qty": st.column_config.NumberColumn(format="%d", disabled=True),
                "Order Qty Override": st.column_config.NumberColumn("Final Order Qty", min_value=0, step=50),
            },
            hide_index=True,
            use_container_width=True,
            key="packaging_replenishment_editor"
        )
        
        st.download_button(
            "Download Plan as CSV",
            edited_df.to_csv(index=False).encode('utf-8'),
            f"packaging_replenishment_plan_{date.today().strftime('%Y%m%d')}.csv",
            "text/csv",
            key='download-packaging-plan'
        )
    else:
        st.success("Calculation complete. No packaging materials currently require replenishment.")