# RMS/analytics_dashboard/kpi_calculations.py
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def get_sales_data(fetcher, processed_sales_table_id: str, 
                   filter_start_date: datetime.date, 
                   filter_end_date: datetime.date, 
                   platforms: list = None, 
                   accounts: list = None, 
                   mskust_list: list = None) -> pd.DataFrame:
    """
    Fetches sales data from Baserow and processes it for analytics.
    - Filters data based on provided date range, platforms, accounts, and MSKUs.
    - For Amazon data (which is period-aggregated), it distributes the sales
      evenly across the days within its reported period that also fall into
      the user's filter_start_date and filter_end_date.
    - Returns a DataFrame with daily sales data.
    """
    logger.info(f"KPI_CALC: Fetching sales data for period {filter_start_date} to {filter_end_date}")
    if not processed_sales_table_id:
        logger.error("KPI_CALC: Processed sales table ID not provided.")
        return pd.DataFrame()

    # --- Construct Baserow Filters ---
    # This part is complex due to Baserow's API filtering capabilities.
    # A robust solution would involve building precise filter strings.
    # For now, we'll fetch a slightly broader range and filter more in Pandas,
    # especially if complex OR conditions for platforms/accounts are needed.

    # Fetch all data for now and filter in pandas.
    # In a production system with large data, optimize this to fetch less from Baserow.
    # For date filtering, Baserow API is good, but for multiple platforms/accounts/MSKUs (OR conditions),
    # it can be tricky to construct a single API filter string.
    
    all_raw_sales_df = fetcher.get_table_data_as_dataframe(processed_sales_table_id)

    if all_raw_sales_df is None or all_raw_sales_df.empty:
        logger.warning("KPI_CALC: No sales data fetched from Baserow.")
        return pd.DataFrame()

    # --- Initial Data Cleaning and Type Conversion ---
    required_cols = ['Sale Date', 'MSKU', 'Platform', 'Account Name', 
                     'Quantity Sold', 'Net Revenue', 'Report Period Start Date']
    missing_cols = [col for col in required_cols if col not in all_raw_sales_df.columns]
    if missing_cols:
        logger.error(f"KPI_CALC: Raw sales data from Baserow is missing required columns: {missing_cols}")
        return pd.DataFrame()

    # Convert data types
    all_raw_sales_df['Sale Date'] = pd.to_datetime(all_raw_sales_df['Sale Date'], errors='coerce').dt.date
    all_raw_sales_df['Report Period Start Date'] = pd.to_datetime(all_raw_sales_df['Report Period Start Date'], errors='coerce').dt.date
    all_raw_sales_df['Quantity Sold'] = pd.to_numeric(all_raw_sales_df['Quantity Sold'], errors='coerce').fillna(0)
    all_raw_sales_df['Net Revenue'] = pd.to_numeric(all_raw_sales_df['Net Revenue'], errors='coerce').fillna(0)
    
    # Drop rows where critical dates couldn't be parsed
    all_raw_sales_df.dropna(subset=['Sale Date'], inplace=True) # Sale Date is essential

    # --- Apply User Filters (Platform, Account, MSKU) ---
    filtered_df = all_raw_sales_df.copy()
    if platforms:
        filtered_df = filtered_df[filtered_df['Platform'].isin(platforms)]
    if accounts:
        filtered_df = filtered_df[filtered_df['Account Name'].isin(accounts)]
    if mskust_list:
        filtered_df = filtered_df[filtered_df['MSKU'].isin(mskust_list)]
    
    if filtered_df.empty:
        logger.info("KPI_CALC: No data after applying platform/account/MSKU filters.")
        return pd.DataFrame()

    # --- Process and "Explode" Amazon Data, Combine with Daily Data ---
    daily_sales_records = []

    for index, row in filtered_df.iterrows():
        platform = row['Platform']
        sale_date = row['Sale Date'] # For F/M this is the actual sale date; for Amz, it's period end.
        
        if platform.lower() == "amazon" and pd.notna(row['Report Period Start Date']):
            # This is an Amazon aggregated record
            amazon_period_start = row['Report Period Start Date']
            amazon_period_end = sale_date # Which is the period end for Amazon records
            
            if amazon_period_start > amazon_period_end:
                logger.warning(f"KPI_CALC: Amazon record for MSKU {row['MSKU']} has start date after end date. Skipping. Start: {amazon_period_start}, End: {amazon_period_end}")
                continue

            num_days_in_amazon_report = (amazon_period_end - amazon_period_start).days + 1
            if num_days_in_amazon_report <= 0: num_days_in_amazon_report = 1 # Should not happen if dates are correct

            avg_daily_qty = row['Quantity Sold'] / num_days_in_amazon_report
            avg_daily_revenue = row['Net Revenue'] / num_days_in_amazon_report

            # Distribute this average across the days of the Amazon report period
            # that also fall within the user's global filter_start_date and filter_end_date
            current_day_in_amazon_period = amazon_period_start
            while current_day_in_amazon_period <= amazon_period_end:
                if filter_start_date <= current_day_in_amazon_period <= filter_end_date:
                    # This day is relevant for the user's selected filter range
                    daily_sales_records.append({
                        'Sale Date': current_day_in_amazon_period,
                        'MSKU': row['MSKU'],
                        'Platform': platform,
                        'Account Name': row['Account Name'],
                        'Quantity Sold': avg_daily_qty,
                        'Net Revenue': avg_daily_revenue,
                        # Add other relevant fields if needed for analytics, e.g., Platform SKU
                        'Platform SKU': row.get('Platform SKU'),
                        'Order ID': row.get('Order ID') 
                    })
                current_day_in_amazon_period += timedelta(days=1)
        
        elif platform.lower() in ["flipkart", "meesho"]: # Or any other platform with daily data
            # This is already daily data, check if it falls within the filter range
            if filter_start_date <= sale_date <= filter_end_date:
                daily_sales_records.append({
                    'Sale Date': sale_date,
                    'MSKU': row['MSKU'],
                    'Platform': platform,
                    'Account Name': row['Account Name'],
                    'Quantity Sold': row['Quantity Sold'],
                    'Net Revenue': row['Net Revenue'],
                    'Platform SKU': row.get('Platform SKU'),
                    'Order ID': row.get('Order ID')
                })
        else:
            # Handle other platforms or records without 'Report Period Start Date' if necessary
            # For now, assume they are daily if not Amazon with period dates
            if filter_start_date <= sale_date <= filter_end_date:
                 daily_sales_records.append({
                    'Sale Date': sale_date,
                    'MSKU': row['MSKU'],
                    'Platform': platform,
                    'Account Name': row['Account Name'],
                    'Quantity Sold': row['Quantity Sold'],
                    'Net Revenue': row['Net Revenue'],
                    'Platform SKU': row.get('Platform SKU'),
                    'Order ID': row.get('Order ID')
                })


    if not daily_sales_records:
        logger.info("KPI_CALC: No daily sales records generated after processing and date filtering.")
        return pd.DataFrame()

    final_daily_df = pd.DataFrame(daily_sales_records)
    
    # Final aggregation: In case the "explosion" of Amazon data or other processing
    # created multiple entries for the same MSKU/Platform/Account on the same *actual* Sale Date,
    # we should sum them up.
    if not final_daily_df.empty:
        # MSKU can be None if unmapped, fill for groupby
        final_daily_df['MSKU'] = final_daily_df['MSKU'].fillna('__UNMAPPED__')
        
        final_daily_df = final_daily_df.groupby(
            ['Sale Date', 'MSKU', 'Platform', 'Account Name'], 
            as_index=False
        ).agg({
            'Quantity Sold': 'sum',
            'Net Revenue': 'sum',
            'Platform SKU': 'first', # Take one example if multiple
            'Order ID': 'first'      # Take one example if multiple
        })
        final_daily_df['MSKU'] = final_daily_df['MSKU'].replace('__UNMAPPED__', None)
        logger.info(f"KPI_CALC: Generated {len(final_daily_df)} final daily aggregated sales records.")
    
    return final_daily_df

# --- Placeholder for other KPI calculation functions ---
def calculate_total_sales_kpis(daily_sales_df: pd.DataFrame):
    if daily_sales_df is None or daily_sales_df.empty:
        return {
            'total_net_revenue': 0,
            'total_units_sold': 0,
            'total_orders': 0, # Approximation
            'average_selling_price': 0
        }

    total_net_revenue = daily_sales_df['Net Revenue'].sum()
    total_units_sold = daily_sales_df['Quantity Sold'].sum()
    
    # Approximating total orders:
    # If 'Order ID' is present and somewhat reliable, count distinct non-null Order IDs.
    # Otherwise, if each row is a daily aggregate per MSKU, this isn't a true order count.
    # For now, let's count rows as a proxy if Order ID isn't well-populated.
    # This needs refinement based on how 'Order ID' is populated.
    total_orders = 0
    if 'Order ID' in daily_sales_df.columns and daily_sales_df['Order ID'].notna().any():
        total_orders = daily_sales_df['Order ID'].nunique() # Counts distinct non-null Order IDs
    else:
        # If no reliable Order ID, this is not a true order count.
        # Could be number of MSKU-Day combinations with sales.
        total_orders = len(daily_sales_df) # Placeholder logic
        logger.warning("KPI_CALC: 'Order ID' not reliably found for order count; using row count as proxy.")


    average_selling_price = (total_net_revenue / total_units_sold) if total_units_sold > 0 else 0

    return {
        'total_net_revenue': total_net_revenue,
        'total_units_sold': total_units_sold,
        'total_orders': total_orders,
        'average_selling_price': average_selling_price
    }

def get_sales_trend_data(daily_sales_df: pd.DataFrame, freq='D'):
    """
    Aggregates daily sales data to a specified frequency (D, W, M).
    Input DataFrame must have a 'Sale Date' (datetime.date or convertible) 
    and 'Net Revenue', 'Quantity Sold'.
    """
    if daily_sales_df is None or daily_sales_df.empty:
        return pd.DataFrame()
    if 'Sale Date' not in daily_sales_df.columns:
        logger.error("KPI_CALC: 'Sale Date' column missing for trend calculation.")
        return pd.DataFrame()

    trend_df = daily_sales_df.copy()
    trend_df['Sale Date'] = pd.to_datetime(trend_df['Sale Date']) # Ensure it's datetime for resampling
    
    # Set Sale Date as index for resampling
    trend_df = trend_df.set_index('Sale Date')
    
    # Resample and aggregate
    # MSKU can be None, handle it. We want to sum numerics.
    # If MSKU is part of the aggregation, it should be in groupby before resample or after.
    # For an overall trend, we resample directly.
    aggregated_trend = trend_df.resample(freq).agg({
        'Net Revenue': 'sum',
        'Quantity Sold': 'sum'
    }).reset_index()
    
    logger.info(f"KPI_CALC: Generated sales trend data with frequency '{freq}'.")
    return aggregated_trend