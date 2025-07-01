# RMS/replenishment/replenishment_logic.py
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import logging

logger = logging.getLogger(__name__)

def calculate_replenishment_data(
    inventory_df: pd.DataFrame, 
    sales_velocity_df: pd.Series, 
    lead_times_map: dict, 
    stock_cover_days_map: dict,
    order_cycle_days_map: dict,
    moq_map: dict
) -> pd.DataFrame:
    logger.info("REPLEN_LOGIC: Starting replenishment calculation...")
    
    if inventory_df is None or inventory_df.empty:
        logger.warning("REPLEN_LOGIC: Inventory data is empty. Cannot calculate replenishment.")
        return pd.DataFrame()

    replen_df = inventory_df.set_index('MSKU').copy()
    
    replen_df['Avg Daily Sales'] = replen_df.index.map(sales_velocity_df)
    
    # --- FIX: Reassign the result of fillna ---
    replen_df['Avg Daily Sales'] = replen_df['Avg Daily Sales'].fillna(0)

    sales_only_mskust = sales_velocity_df.index.difference(replen_df.index)
    for msku in sales_only_mskust:
        replen_df.loc[msku, 'Current Inventory'] = 0
        replen_df.loc[msku, 'Avg Daily Sales'] = sales_velocity_df.loc[msku]
    
    # --- FIX: Reassign the result of fillna ---
    replen_df['Current Inventory'] = replen_df['Current Inventory'].fillna(0)

    # ... (Mapping parameters remains the same) ...
    default_lead_time = lead_times_map.get('default', 30)
    default_stock_cover_days = stock_cover_days_map.get('default', 15)
    default_order_cycle_days = order_cycle_days_map.get('default', 30)
    default_moq = moq_map.get('default', 0)
    replen_df['Lead Time (days)'] = replen_df.index.map(lambda x: lead_times_map.get(x, default_lead_time))
    replen_df['Stock Cover (days)'] = replen_df.index.map(lambda x: stock_cover_days_map.get(x, default_stock_cover_days))
    replen_df['Order Cycle (days)'] = replen_df.index.map(lambda x: order_cycle_days_map.get(x, default_order_cycle_days))
    replen_df['MOQ'] = replen_df.index.map(lambda x: moq_map.get(x, default_moq))

    # --- Core Replenishment Calculations (remain the same) ---
    replen_df['DOS'] = np.where(replen_df['Avg Daily Sales'] > 0, replen_df['Current Inventory'] / replen_df['Avg Daily Sales'], np.inf)
    replen_df['Safety Stock (Units)'] = replen_df['Avg Daily Sales'] * replen_df['Stock Cover (days)']
    replen_df['Reorder Point'] = (replen_df['Avg Daily Sales'] * replen_df['Lead Time (days)']) + replen_df['Safety Stock (Units)']
    replen_df['Target Stock Level'] = (replen_df['Avg Daily Sales'] * (replen_df['Lead Time (days)'] + replen_df['Order Cycle (days)'])) + replen_df['Safety Stock (Units)']
    inbound_stock = 0 
    replen_df['Suggested Order Qty'] = np.where(replen_df['Current Inventory'] <= replen_df['Reorder Point'], replen_df['Target Stock Level'] - replen_df['Current Inventory'] - inbound_stock, 0)
    replen_df['Suggested Order Qty'] = replen_df['Suggested Order Qty'].clip(lower=0)
    replen_df['Suggested Order Qty'] = np.where((replen_df['Suggested Order Qty'] > 0) & (replen_df['Suggested Order Qty'] < replen_df['MOQ']), replen_df['MOQ'], replen_df['Suggested Order Qty'])

    # --- FIX: Handle potential non-finite values before converting to int ---
    unit_cols_to_round = ['Safety Stock (Units)', 'Reorder Point', 'Target Stock Level', 'Suggested Order Qty']
    for col in unit_cols_to_round:
        # Replace any remaining non-finite values (inf, -inf, NaN) with 0 before casting
        replen_df[col] = np.nan_to_num(replen_df[col], nan=0.0, posinf=0.0, neginf=0.0)
        replen_df[col] = np.ceil(replen_df[col]).astype(int)

    # Determine Replenishment Status
    conditions = [
        replen_df['Current Inventory'] <= replen_df['Reorder Point'],
        replen_df['DOS'] <= (replen_df['Lead Time (days)'] + replen_df['Stock Cover (days)']),
        replen_df['DOS'] > 90 # Example for overstocked
    ]
    choices = ['🚨 Order Now', '⚠️ Reorder Soon', '📈 Overstocked']
    replen_df['Status'] = np.select(conditions, choices, default='✅ OK')

    replen_df.reset_index(inplace=True)
    
    logger.info(f"REPLEN_LOGIC: Replenishment calculation complete for {len(replen_df)} MSKUs.")
    return replen_df