# RMS/replenishment_engine/core.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def calculate_sales_stats(daily_sales_df: pd.DataFrame, days_for_stats: int = 30, days_for_velocity: int = 90) -> pd.DataFrame:
    """
    Calculates key sales statistics for each MSKU from daily sales data.
    """
    if daily_sales_df is None or daily_sales_df.empty:
        return pd.DataFrame()

    logger.info(f"ENGINE: Calculating sales stats for last {days_for_stats} and {days_for_velocity} days.")
    
    daily_sales_df['Sale Date'] = pd.to_datetime(daily_sales_df['Sale Date']).dt.date
    most_recent_date = daily_sales_df['Sale Date'].max()
    if pd.isna(most_recent_date):
        return pd.DataFrame()

    # --- 30-Day Stats ---
    start_date_30d = most_recent_date - timedelta(days=days_for_stats - 1)
    sales_30d = daily_sales_df[daily_sales_df['Sale Date'] >= start_date_30d]
    all_mskust = sales_30d['MSKU'].unique()
    date_range_30d = pd.date_range(start=start_date_30d, end=most_recent_date, freq='D').date
    multi_index = pd.MultiIndex.from_product([all_mskust, date_range_30d], names=['MSKU', 'Sale Date'])
    sales_30d_full = sales_30d.groupby(['MSKU', 'Sale Date'])['Quantity Sold'].sum().reindex(multi_index, fill_value=0).reset_index()
    stats_30d = sales_30d_full.groupby('MSKU').agg(
        avg_daily_sales_30d=('Quantity Sold', 'mean'),
        std_dev_daily_sales=('Quantity Sold', 'std')
    ).reset_index()
    stats_30d['std_dev_daily_sales'] = stats_30d['std_dev_daily_sales'].fillna(0)

    # --- 90-Day Stats ---
    start_date_90d = most_recent_date - timedelta(days=days_for_velocity - 1)
    sales_90d = daily_sales_df[daily_sales_df['Sale Date'] >= start_date_90d]
    total_sales_90d = sales_90d.groupby('MSKU')['Quantity Sold'].sum().reset_index()
    total_sales_90d = total_sales_90d.rename(columns={'Quantity Sold': 'total_sales_90d'})

    # --- Last Sale Date ---
    last_sale_dates = daily_sales_df[daily_sales_df['Quantity Sold'] > 0].groupby('MSKU')['Sale Date'].max().reset_index()
    last_sale_dates = last_sale_dates.rename(columns={'Sale Date': 'last_sale_date'})

    # --- Merge and Clean ---
    final_stats = pd.merge(stats_30d, total_sales_90d, on='MSKU', how='left')
    final_stats = pd.merge(final_stats, last_sale_dates, on='MSKU', how='left')
    
    numeric_cols_to_fill = ['avg_daily_sales_30d', 'std_dev_daily_sales', 'total_sales_90d']
    for col in numeric_cols_to_fill:
        if col in final_stats.columns:
            final_stats[col] = final_stats[col].fillna(0)

    final_stats['last_sale_date'] = pd.to_datetime(final_stats['last_sale_date'], errors='coerce')
    most_recent_datetime = datetime.combine(most_recent_date, datetime.min.time())
    final_stats['days_since_last_sale'] = (most_recent_datetime - final_stats['last_sale_date']).dt.days
    final_stats['days_since_last_sale'] = final_stats['days_since_last_sale'].fillna(999)

    # --- Final Calculated Metrics ---
    final_stats['coefficient_of_variation'] = np.where(final_stats['avg_daily_sales_30d'] > 0, final_stats['std_dev_daily_sales'] / final_stats['avg_daily_sales_30d'], 0)
    monthly_sales_current = final_stats['avg_daily_sales_30d'] * 30
    monthly_sales_average_90d = final_stats['total_sales_90d'] / (days_for_velocity / 30)
    final_stats['velocity_score'] = np.where(monthly_sales_average_90d > 0, monthly_sales_current / monthly_sales_average_90d, 0)
    
    return final_stats


def run_replenishment_engine(
    products_df: pd.DataFrame, 
    sales_stats_df: pd.DataFrame, 
    open_po_df: pd.DataFrame,
    last_order_dates_df: pd.DataFrame, 
    parameters: dict
) -> pd.DataFrame:
    logger.info("ENGINE: Running main replenishment engine...")
    
    # 1. Prepare Master DataFrame
    df = products_df.copy()
    df = pd.merge(df, sales_stats_df, on='MSKU', how='left')
    if open_po_df is not None and not open_po_df.empty:
        df = pd.merge(df, open_po_df, on='MSKU', how='left')
    if last_order_dates_df is not None and not last_order_dates_df.empty:
        df = pd.merge(df, last_order_dates_df, on='MSKU', how='left')
    
    # Define column groups and fill NaNs with appropriate defaults
    numeric_cols = ['Current Inventory', 'Vendor Lead Time (days)', 'avg_daily_sales_30d', 'std_dev_daily_sales', 'coefficient_of_variation', 'velocity_score', 'days_since_last_sale', 'on_order_qty_air', 'on_order_qty_sea']
    for col in numeric_cols:
        if col not in df.columns: df[col] = 0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    if 'Product Type' not in df.columns: df['Product Type'] = 'NON-FOCUSED'
    df['Product Type'] = df['Product Type'].fillna('NON-FOCUSED').str.upper()
    if 'sea_order_eta' not in df.columns: df['sea_order_eta'] = pd.NaT
    df['sea_order_eta'] = pd.to_datetime(df['sea_order_eta'], errors='coerce')
    if 'last_order_date' not in df.columns: df['last_order_date'] = pd.NaT
    df['last_order_date'] = pd.to_datetime(df['last_order_date'], errors='coerce')

    # 2. Intermediate Calculations (Applied to all rows)
    is_peak = parameters.get('is_peak_season', False)
    df['adjusted_daily_sales'] = df['avg_daily_sales_30d'] * parameters.get('peak_season_multiplier', 1.0) if is_peak else df['avg_daily_sales_30d']
    df['air_lead_time_adjusted'] = df['Vendor Lead Time (days)'] + parameters.get('air_transit_days', 5) + parameters.get('admin_days', 2) + (parameters.get('holiday_buffer_days', 5) if is_peak else 0)
    df['sea_lead_time_adjusted'] = df['Vendor Lead Time (days)'] + parameters.get('sea_transit_days', 30) + parameters.get('admin_days', 2) + (parameters.get('holiday_buffer_days', 5) if is_peak else 0)
    df['dynamic_safety_factor'] = (parameters.get('base_safety_buffer_pct', 10) / 100 + df['coefficient_of_variation']).clip(upper=(parameters.get('max_safety_buffer_pct', 30) / 100))
    df['current_days_coverage'] = np.where(df['avg_daily_sales_30d'] > 0, df['Current Inventory'] / df['avg_daily_sales_30d'], 999)
    df['danger_days_adjusted'] = parameters.get('danger_threshold_days', 7) * (1 + df['coefficient_of_variation'])

    today_dt = datetime.now()
    days_since_last_review = (today_dt - df['last_order_date']).dt.days
    df['review_due'] = days_since_last_review >= parameters.get('review_period_days', 15)
    df['review_due'].fillna(True, inplace=True)

    # 3. Calculate Potential Order Quantities for EACH strategy
    # FOCUSED - SEA
    sea_demand_lead = df['adjusted_daily_sales'] * df['sea_lead_time_adjusted']
    sea_demand_review = df['adjusted_daily_sales'] * parameters.get('review_period_days', 15)
    sea_safety_buffer = (sea_demand_lead + sea_demand_review) * df['dynamic_safety_factor']
    df['sea_target_stock'] = sea_demand_lead + sea_demand_review + sea_safety_buffer
    df['sea_order_quantity_calc'] = (df['sea_target_stock'] - df['Current Inventory'] - df['on_order_qty_sea']).clip(lower=0)

    # FOCUSED - AIR (Bridge or Urgent)
    has_open_sea_order = pd.notna(df['sea_order_eta'])
    days_until_arrival = (df['sea_order_eta'] - datetime.now()).dt.days.fillna(999).clip(lower=0)
    demand_until_arrival = df['adjusted_daily_sales'] * days_until_arrival
    volatility_buffer = df['std_dev_daily_sales'] * np.sqrt(days_until_arrival) * 1.65
    projected_stock = df['Current Inventory'] - demand_until_arrival - volatility_buffer
    gap_qty = np.where(projected_stock < 0, abs(projected_stock), 0)
    air_bridge_qty_calc = gap_qty * (1 + parameters.get('air_bridge_buffer_pct', 5) / 100)
    
    is_urgent = df['current_days_coverage'] < df['danger_days_adjusted']
    air_demand_lead = df['adjusted_daily_sales'] * df['air_lead_time_adjusted']
    air_safety_urgent = air_demand_lead * df['dynamic_safety_factor']
    air_urgent_qty_calc = (air_demand_lead + air_safety_urgent - df['Current Inventory'] - df['on_order_qty_air']).clip(lower=0)
    
    df['air_focused_qty_calc'] = np.where(has_open_sea_order, air_bridge_qty_calc, np.where(is_urgent, air_urgent_qty_calc, 0))

    # NON-FOCUSED - AIR
    air_demand_lead_nf = df['adjusted_daily_sales'] * df['air_lead_time_adjusted']
    air_demand_review_nf = df['adjusted_daily_sales'] * parameters.get('review_period_days', 15)
    air_safety_nf = (air_demand_lead_nf + air_demand_review_nf) * df['dynamic_safety_factor'] * parameters.get('non_focused_multiplier', 1.5)
    df['air_target_stock_nf'] = air_demand_lead_nf + air_demand_review_nf + air_safety_nf
    df['air_nonfocused_qty_calc'] = (df['air_target_stock_nf'] - df['Current Inventory'] - df['on_order_qty_air']).clip(lower=0)

    # 4. Final Assignment based on Product Type
    df['sea_order_quantity'] = np.where(df['Product Type'] == 'FOCUSED', df['sea_order_quantity_calc'], 0)
    df['air_order_quantity'] = np.where(df['Product Type'] == 'FOCUSED', df['air_focused_qty_calc'], df['air_nonfocused_qty_calc'])

    # 5. Status Classifications & Order Reason (Full Implementation)
    df['total_in_transit'] = df['on_order_qty_air'] + df['on_order_qty_sea']
    df['effective_days_coverage'] = np.where(df['avg_daily_sales_30d'] > 0, (df['Current Inventory'] + df['total_in_transit']) / df['avg_daily_sales_30d'], 999)
    
    # Replenishment Status
    cond_f_urgent = df['current_days_coverage'] < df['danger_days_adjusted']
    cond_f_low = df['current_days_coverage'] < df['air_lead_time_adjusted']
    cond_f_plan_sea = (df['effective_days_coverage'] < df['sea_lead_time_adjusted']) & (~has_open_sea_order)
    cond_f_overstock = df['effective_days_coverage'] > (df['sea_lead_time_adjusted'] + parameters.get('overstock_days', 60))
    choices_f = ["🚨 Order NOW (AIR)", "⚠️ Low Stock (Order AIR)", "📋 Plan SEA Order", "📦 Overstocked"]
    replen_status_f = np.select([cond_f_urgent, cond_f_low, cond_f_plan_sea, cond_f_overstock], choices_f, default="✅ Stock OK")
    
    cond_nf_urgent = df['current_days_coverage'] < df['danger_days_adjusted']
    cond_nf_low = df['current_days_coverage'] < (df['air_lead_time_adjusted'] + parameters.get('danger_threshold_days', 7))
    cond_nf_overstock = df['current_days_coverage'] > parameters.get('overstock_days', 60)
    choices_nf = ["🚨 Order NOW (AIR)", "⚠️ Low Stock", "📦 Overstocked"]
    replen_status_nf = np.select([cond_nf_urgent, cond_nf_low, cond_nf_overstock], choices_nf, default="✅ Stock OK")
    
    df['replen_status'] = np.where(df['Product Type'] == 'FOCUSED', replen_status_f, replen_status_nf)

    # Movement Status
    cond_move_fast = df['velocity_score'] > parameters.get('fast_moving_threshold', 1.5)
    cond_move_normal = df['velocity_score'] > parameters.get('slow_moving_threshold', 0.3)
    cond_move_slow = (df['days_since_last_sale'] < parameters.get('dead_stock_days', 60)) & (df['velocity_score'] > 0)
    cond_move_dead = df['days_since_last_sale'] >= parameters.get('dead_stock_days', 60)
    choices_move = ["🔥 Fast Moving", "📈 Normal", "🐌 Slow Moving", "💀 Dead Stock"]
    df['movement_status'] = np.select([cond_move_fast, cond_move_normal, cond_move_slow, cond_move_dead], choices_move, default="🏷️ Liquidate")

    # Volatility Status
    df['volatility_status'] = pd.cut(df['coefficient_of_variation'], bins=[-np.inf, 0.25, 0.50, 0.75, np.inf], labels=["🟢 Stable", "🟡 Moderate", "🟠 High", "🔴 Extreme"], right=False)

    # Order Reason
    df['order_reason'] = '' # Start with a blank slate
    
    # FOCUSED Product Reasons
    focused_mask = df['Product Type'] == 'FOCUSED'

    urgent_air_mask = focused_mask & is_urgent & ~has_open_sea_order
    bridge_air_mask = focused_mask & has_open_sea_order & (air_bridge_qty_calc > 0)
    regular_sea_mask = focused_mask & (df['sea_order_quantity'] > 0)
    
    # --- ADD THIS DEBUG BLOCK ---
    logger.debug(f"Order Reason Debug: Total FOCUSED products = {focused_mask.sum()}")
    logger.debug(f"Order Reason Debug: URGENT AIR mask matches = {urgent_air_mask.sum()} rows")
    logger.debug(f"Order Reason Debug: BRIDGE AIR mask matches = {bridge_air_mask.sum()} rows")
    logger.debug(f"Order Reason Debug: REGULAR SEA mask matches = {regular_sea_mask.sum()} rows")
    # --- END DEBUG BLOCK ---

    
    # Reason 1: URGENT (highest priority for FOCUSED)
    urgent_air_mask = focused_mask & (df['replen_status'] == "🚨 Order NOW (AIR)") & (df['air_order_quantity'] > 0)
    df.loc[urgent_air_mask, 'order_reason'] = 'URGENT'
    logger.debug(f"Order Reason Debug: After URGENT assignment, non-empty reasons = {len(df[df['order_reason'] != ''])}")
    
    # Reason 2: BRIDGE (second priority)
    bridge_air_mask = focused_mask & has_open_sea_order & (df['air_order_quantity'] > 0)
    # Only apply BRIDGE if it's not already marked as URGENT
    df.loc[bridge_air_mask & (df['order_reason'] == ''), 'order_reason'] = 'BRIDGE'
    logger.debug(f"Order Reason Debug: After BRIDGE assignment, non-empty reasons = {len(df[df['order_reason'] != ''])}")

    # Reason 3: REGULAR SEA (only if review is due)
    regular_sea_mask = focused_mask & (df['sea_order_quantity'] > 0) & (df['review_due'])
    
    # Add "REGULAR SEA" to the reason. If a reason already exists (like BRIDGE), append it.
    df.loc[regular_sea_mask, 'order_reason'] = np.where(
        df.loc[regular_sea_mask, 'order_reason'] != '', 
        df.loc[regular_sea_mask, 'order_reason'] + ' + REGULAR SEA', 
        'REGULAR SEA'
    )
    logger.debug(f"Order Reason Debug: After REGULAR SEA assignment, non-empty reasons = {len(df[df['order_reason'] != ''])}")

    # NON-FOCUSED Product Reason
    non_focused_mask = df['Product Type'] == 'NON-FOCUSED'
    regular_air_mask = non_focused_mask & (df['air_order_quantity'] > 0)
    df.loc[regular_air_mask, 'order_reason'] = 'REGULAR AIR'
    logger.debug(f"Order Reason Debug: After REGULAR AIR assignment, non-empty reasons = {len(df[df['order_reason'] != ''])}")
    # --- END CORRECTED LOGIC ---

    # Final rounding
    df['sea_order_quantity'] = np.ceil(df['sea_order_quantity']).astype(int)
    df['air_order_quantity'] = np.ceil(df['air_order_quantity']).astype(int)

    logger.debug("Order Reason Debug: Final state of key columns before returning DataFrame:")
    debug_cols = ['MSKU', 'Product Type', 'replen_status', 'air_order_quantity', 'sea_order_quantity', 'review_due', 'order_reason']
    logger.debug(f"\n{df[debug_cols].head(15).to_string()}")

    return df