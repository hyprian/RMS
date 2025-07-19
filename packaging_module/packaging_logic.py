# RMS/packaging_module/packaging_logic.py
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import logging

logger = logging.getLogger(__name__)

def process_outbound_to_daily_consumption(outbound_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw outbound data into a daily consumption log for each packaging material.
    Handles 'Packaging List' being either a string or a list of strings.
    """
    if outbound_df is None or outbound_df.empty or 'Packaging List' not in outbound_df.columns:
        return pd.DataFrame(columns=['Date', 'Material Name', 'Quantity Used'])

    # --- ROBUST PROCESSING LOGIC ---
    # Create a new column to hold the list of materials, regardless of input type.
    
    def to_list(entry):
        if isinstance(entry, list):
            return entry # It's already a list
        if isinstance(entry, str):
            # Handle both comma-separated and space-separated strings just in case
            if ',' in entry:
                return [item.strip() for item in entry.split(',')]
            else:
                return entry.split() # Splits by whitespace
        return [] # Return empty list for other types (like NaN)

    # Apply this robust function to create the 'Materials' column
    outbound_df['Materials'] = outbound_df['Packaging List'].apply(to_list)
    # --- END ROBUST PROCESSING LOGIC ---

    # Explode the DataFrame so each material in the list gets its own row
    daily_consumption_df = outbound_df.explode('Materials')
    
    # Clean up the material names
    daily_consumption_df['Material Name'] = daily_consumption_df['Materials'].astype(str).str.strip()
    
    # Drop any rows that might have become empty
    daily_consumption_df.dropna(subset=['Material Name'], inplace=True)
    daily_consumption_df = daily_consumption_df[daily_consumption_df['Material Name'] != '']
    
    # Now, group by Date and the cleaned Material Name and count the occurrences
    daily_counts = daily_consumption_df.groupby(['Date', 'Material Name']).size().reset_index(name='Quantity Used')
    
    logger.info(f"Processed outbound data into {len(daily_counts)} daily consumption records.")
    return daily_counts

def calculate_packaging_velocity(daily_consumption_df: pd.DataFrame, days_period: int) -> pd.Series:
    """
    Calculates average daily usage velocity per packaging material.
    """
    if daily_consumption_df is None or daily_consumption_df.empty:
        return pd.Series(dtype=float)

    # Ensure Date is a date object for comparison
    daily_consumption_df['Date'] = pd.to_datetime(daily_consumption_df['Date']).dt.date
        
    most_recent_date = daily_consumption_df['Date'].max()
    if pd.isna(most_recent_date):
        logger.warning("PACKAGING_LOGIC: No valid dates found in consumption data.")
        return pd.Series(dtype=float)

    velocity_start_date = most_recent_date - timedelta(days=days_period - 1)

    velocity_df = daily_consumption_df[
        (daily_consumption_df['Date'] >= velocity_start_date) & 
        (daily_consumption_df['Date'] <= most_recent_date)
    ].copy()

    if velocity_df.empty:
        logger.warning(f"PACKAGING_LOGIC: No consumption data in the last {days_period} days.")
        return pd.Series(dtype=float)

    total_usage_per_material = velocity_df.groupby('Material Name')['Quantity Used'].sum()
    avg_daily_usage = total_usage_per_material / days_period
    
    logger.info(f"Calculated packaging velocity for {len(avg_daily_usage)} materials over {days_period} days.")
    return avg_daily_usage

def calculate_packaging_replenishment(
    inventory_df: pd.DataFrame, 
    velocity_series: pd.Series, 
    lead_time: int, 
    stock_cover_days: int
) -> pd.DataFrame:
    """
    Calculates replenishment needs for packaging materials.
    """
    if inventory_df is None or inventory_df.empty:
        logger.warning("PACKAGING_LOGIC: Inventory data is empty. Cannot calculate replenishment.")
        return pd.DataFrame()

    replen_df = inventory_df.set_index('Material Name')
    replen_df['Avg Daily Usage'] = replen_df.index.map(velocity_series).fillna(0)

    # Add materials that have usage but no inventory record
    usage_only_materials = velocity_series.index.difference(replen_df.index)
    for material in usage_only_materials:
        replen_df.loc[material, 'Current Inventory'] = 0
        replen_df.loc[material, 'Avg Daily Usage'] = velocity_series.loc[material]
    
    replen_df['Current Inventory'].fillna(0, inplace=True)

    # Core Calculations
    replen_df['DOS'] = np.where(
        replen_df['Avg Daily Usage'] > 0,
        replen_df['Current Inventory'] / replen_df['Avg Daily Usage'],
        np.inf
    )
    replen_df['Safety Stock'] = replen_df['Avg Daily Usage'] * stock_cover_days
    replen_df['Reorder Point'] = (replen_df['Avg Daily Usage'] * lead_time) + replen_df['Safety Stock']
    replen_df['Target Stock Level'] = replen_df['Reorder Point'] + (replen_df['Avg Daily Usage'] * 30) # e.g., reorder point + 30 days of stock
    
    replen_df['Suggested Order Qty'] = np.where(
        replen_df['Current Inventory'] <= replen_df['Reorder Point'],
        replen_df['Target Stock Level'] - replen_df['Current Inventory'],
        0
    ).clip(lower=0)

    # Round up unit values
    for col in ['Safety Stock', 'Reorder Point', 'Target Stock Level', 'Suggested Order Qty']:
        replen_df[col] = np.ceil(replen_df[col]).astype(int)

    # Determine Status
    conditions = [
        replen_df['Current Inventory'] <= replen_df['Reorder Point'],
        replen_df['DOS'] <= (lead_time + stock_cover_days),
        replen_df['DOS'] > 120 # Example for overstocked
    ]
    choices = ['🚨 Order Now', '⚠️ Reorder Soon', '📈 Overstocked']
    replen_df['Status'] = np.select(conditions, choices, default='✅ OK')

    replen_df.reset_index(inplace=True)
    logger.info(f"Packaging replenishment calculation complete for {len(replen_df)} materials.")
    return replen_df