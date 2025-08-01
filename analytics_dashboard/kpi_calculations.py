# RMS/analytics_dashboard/kpi_calculations.py
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def process_sales_data_for_analytics(
    all_sales_df: pd.DataFrame,
    filter_start_date: datetime.date, 
    filter_end_date: datetime.date, 
    platforms: list = None, 
    accounts: list = None, 
    mskust_list: list = None) -> pd.DataFrame:
    """
    Processes a pre-loaded, daily-granularity sales DataFrame for analytics.
    This function is now much simpler: it just filters the data based on the user's selections.
    The complex "explosion" logic for Amazon is no longer needed.
    """
    logger.info(f"KPI_CALC: Filtering sales data for period {filter_start_date} to {filter_end_date}")

    if all_sales_df is None or all_sales_df.empty:
        logger.warning("KPI_CALC: Provided sales DataFrame is empty.")
        return pd.DataFrame()

    # --- Apply All User Filters ---
    # Start with the full DataFrame from session state
    filtered_df = all_sales_df.copy()
    
    # Filter by date first - this is the most significant filter
    # The 'Sale Date' column should already be a date object from the data_loader
    if 'Sale Date' in filtered_df.columns:
        filtered_df = filtered_df[
            (filtered_df['Sale Date'] >= filter_start_date) & 
            (filtered_df['Sale Date'] <= filter_end_date)
        ]
    else:
        logger.error("KPI_CALC: 'Sale Date' column not found in the provided DataFrame.")
        return pd.DataFrame()

    # Apply optional filters
    if platforms:
        filtered_df = filtered_df[filtered_df['Platform'].isin(platforms)]
    if accounts:
        filtered_df = filtered_df[filtered_df['Account Name'].isin(accounts)]
    if mskust_list:
        filtered_df = filtered_df[filtered_df['MSKU'].isin(mskust_list)]
    
    if filtered_df.empty:
        logger.info("KPI_CALC: No data remains after applying all filters.")
    else:
        logger.info(f"KPI_CALC: Returning {len(filtered_df)} filtered daily records.")
    
    return filtered_df


def calculate_total_sales_kpis(daily_sales_df: pd.DataFrame):
    """Calculates high-level KPIs from a daily sales DataFrame."""
    if daily_sales_df is None or daily_sales_df.empty:
        return {
            'total_net_revenue': 0, 'total_units_sold': 0,
            'total_orders': 0, 'average_selling_price': 0
        }

    total_net_revenue = daily_sales_df['Net Revenue'].sum()
    total_units_sold = daily_sales_df['Quantity Sold'].sum()
    
    total_orders = 0
    if 'Order ID' in daily_sales_df.columns and daily_sales_df['Order ID'].notna().any():
        total_orders = daily_sales_df['Order ID'].nunique()
    else:
        total_orders = len(daily_sales_df) # Fallback proxy
        logger.warning("KPI_CALC: 'Order ID' not reliably found for order count; using row count as proxy.")

    average_selling_price = (total_net_revenue / total_units_sold) if total_units_sold > 0 else 0

    return {
        'total_net_revenue': total_net_revenue,
        'total_units_sold': total_units_sold,
        'total_orders': total_orders,
        'average_selling_price': average_selling_price
    }

def get_sales_trend_data(daily_sales_df: pd.DataFrame, freq='D'):
    """Aggregates daily sales data to a specified frequency (D, W, M)."""
    if daily_sales_df is None or daily_sales_df.empty:
        return pd.DataFrame()
    if 'Sale Date' not in daily_sales_df.columns:
        logger.error("KPI_CALC: 'Sale Date' column missing for trend calculation.")
        return pd.DataFrame()

    trend_df = daily_sales_df.copy()
    trend_df['Sale Date'] = pd.to_datetime(trend_df['Sale Date'])
    trend_df = trend_df.set_index('Sale Date')
    
    aggregated_trend = trend_df.resample(freq).agg({
        'Net Revenue': 'sum',
        'Quantity Sold': 'sum'
    }).reset_index()
    
    logger.info(f"KPI_CALC: Generated sales trend data with frequency '{freq}'.")
    return aggregated_trend


def get_current_inventory(inventory_df: pd.DataFrame, msku_list: list = None) -> pd.DataFrame:
    """Filters the main inventory DataFrame for a list of MSKUs."""
    if inventory_df is None or inventory_df.empty:
        logger.warning("KPI_CALC: get_current_inventory called with empty inventory_df.")
        return pd.DataFrame(columns=['MSKU', 'Current Inventory'])
    
    if 'MSKU' not in inventory_df.columns or 'Current Inventory' not in inventory_df.columns:
        logger.error("KPI_CALC: Inventory DataFrame missing 'MSKU' or 'Current Inventory' columns.")
        return pd.DataFrame(columns=['MSKU', 'Current Inventory'])

    if msku_list:
        inventory_df = inventory_df[inventory_df['MSKU'].isin(msku_list)]
    
    return inventory_df[['MSKU', 'Current Inventory']].copy()


def calculate_sales_velocity(daily_sales_df: pd.DataFrame, days_period: int) -> pd.Series:
    """Calculates average daily sales velocity per MSKU over a given period."""
    if daily_sales_df is None or daily_sales_df.empty:
        logger.warning("KPI_CALC: calculate_sales_velocity called with empty daily_sales_df.")
        return pd.Series(dtype=float)

    daily_sales_df['Sale Date'] = pd.to_datetime(daily_sales_df['Sale Date']).dt.date

    if daily_sales_df.empty:
        return pd.Series(dtype=float)
        
    most_recent_sale_date = daily_sales_df['Sale Date'].max()
    velocity_start_date = most_recent_sale_date - timedelta(days=days_period - 1)

    velocity_df = daily_sales_df[
        (daily_sales_df['Sale Date'] >= velocity_start_date) & 
        (daily_sales_df['Sale Date'] <= most_recent_sale_date)
    ].copy()

    if velocity_df.empty:
        logger.warning(f"KPI_CALC: No sales data found in the last {days_period} days for velocity calculation.")
        return pd.Series(dtype=float)

    total_sales_per_msku = velocity_df.groupby('MSKU')['Quantity Sold'].sum()
    avg_daily_sales = total_sales_per_msku / days_period
    
    logger.info(f"KPI_CALC: Calculated sales velocity for {len(avg_daily_sales)} MSKUs over {days_period} days.")
    return avg_daily_sales

def calculate_profit_data(daily_sales_df: pd.DataFrame, inventory_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges sales data with inventory (cost) data and calculates profit metrics.
    
    Returns:
        pd.DataFrame: The daily sales DataFrame with added 'Cost', 'Total COGS', and 'Gross Profit' columns.
    """
    if daily_sales_df is None or daily_sales_df.empty:
        return pd.DataFrame()
    if inventory_df is None or inventory_df.empty or 'MSKU' not in inventory_df.columns or 'Cost' not in inventory_df.columns:
        logger.warning("KPI_CALC: Profit calculation skipped. Inventory data or 'Cost' column is missing.")
        # Return the original df with blank profit columns
        daily_sales_df['Total COGS'] = 0
        daily_sales_df['Gross Profit'] = daily_sales_df['Net Revenue']
        return daily_sales_df

    # Select only the necessary columns from inventory to merge
    cost_data = inventory_df[['MSKU', 'Cost']].copy()
    
    # Merge sales data with cost data
    profit_df = pd.merge(daily_sales_df, cost_data, on='MSKU', how='left')
    profit_df['Cost'].fillna(0, inplace=True) # If an MSKU has sales but no cost in inventory, assume 0 cost

    # Calculate profit metrics
    profit_df['Total COGS'] = profit_df['Quantity Sold'] * profit_df['Cost']
    profit_df['Gross Profit'] = profit_df['Net Revenue'] - profit_df['Total COGS']
    
    logger.info(f"KPI_CALC: Successfully calculated profit data for {len(profit_df)} records.")
    return 


def calculate_profit_data(daily_sales_df: pd.DataFrame, inventory_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges sales data with inventory (cost) data and calculates profit metrics.
    Returns the original DataFrame with profit columns added, even if inventory is missing.
    """
    # Start with a copy to avoid modifying the original DataFrame
    profit_df = daily_sales_df.copy()

    if inventory_df is None or inventory_df.empty or 'MSKU' not in inventory_df.columns or 'Cost' not in inventory_df.columns:
        logger.warning("KPI_CALC: Profit calculation running without cost data. Gross Profit will equal Net Revenue.")
        profit_df['Cost'] = 0
        profit_df['Total COGS'] = 0
        profit_df['Gross Profit'] = profit_df['Net Revenue']
        return profit_df

    cost_data = inventory_df[['MSKU', 'Cost']].copy()
    
    # Merge sales data with cost data
    profit_df = pd.merge(profit_df, cost_data, on='MSKU', how='left')
    profit_df['Cost'].fillna(0, inplace=True)

    profit_df['Total COGS'] = profit_df['Quantity Sold'] * profit_df['Cost']
    profit_df['Gross Profit'] = profit_df['Net Revenue'] - profit_df['Total COGS']
    
    logger.info(f"KPI_CALC: Successfully calculated profit data for {len(profit_df)} records.")
    return profit_df

# --- ADD THIS NEW FUNCTION ---
def calculate_total_profit_kpis(profit_df: pd.DataFrame):
    """Calculates high-level profit KPIs from a profit-enabled DataFrame."""
    if profit_df is None or profit_df.empty or 'Gross Profit' not in profit_df.columns:
        return {
            'total_gross_profit': 0,
            'total_cogs': 0,
            'gross_margin': 0
        }

    total_gross_profit = profit_df['Gross Profit'].sum()
    total_net_revenue = profit_df['Net Revenue'].sum()
    total_cogs = profit_df['Total COGS'].sum()
    
    gross_margin = (total_gross_profit / total_net_revenue * 100) if total_net_revenue > 0 else 0

    return {
        'total_gross_profit': total_gross_profit,
        'total_cogs': total_cogs,
        'gross_margin': gross_margin
    }