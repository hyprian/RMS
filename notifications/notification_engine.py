# RMS/notifications/notification_engine.py
import pandas as pd
from datetime import datetime, timedelta
import logging

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
# --- MODIFIED: Import the correct engine functions ---
from replenishment_engine.core import calculate_sales_stats, run_replenishment_engine
from po_module.po_management import get_all_pos, get_open_po_data, get_last_order_dates
from analytics_dashboard.kpi_calculations import process_sales_data_for_analytics as get_sales_data, calculate_total_sales_kpis

from .waha_sender import WahaSender
from .message_generator import generate_low_stock_alert, generate_stale_data_alert, generate_weekly_summary

logger = logging.getLogger(__name__)

def run_all_checks():
    logger.info("NOTIFICATION_ENGINE: Starting all checks...")
    # ... (Initialization logic remains the same) ...
    try:
        waha_config = APP_CONFIG.get('notifications', {})
        sender = WahaSender(base_url=waha_config.get('waha_base_url'), session_name=waha_config.get('waha_session'))
        if sender.check_session_status() != "WORKING":
            logger.error("NOTIFICATION_ENGINE: WAHA session is not 'WORKING'. Aborting checks.")
            return
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
    except Exception as e:
        logger.error(f"NOTIFICATION_ENGINE: Failed to initialize tools. Error: {e}", exc_info=True)
        return

    # --- 2. Load All Necessary Data (remains the same) ---
    logger.info("NOTIFICATION_ENGINE: Loading all required data from Baserow...")
    try:
        all_sales_df = fetcher.get_table_data_as_dataframe(APP_CONFIG['baserow']['processed_sales_data_table_id'])
        all_inventory_df = fetcher.get_inventory_data(APP_CONFIG['baserow']['inventory_table_id'])
        all_category_df = fetcher.get_category_data(APP_CONFIG['baserow']['category_table_id'])
        all_pos_df = get_all_pos(fetcher, APP_CONFIG['baserow']['purchase_orders_table_id'])
        if all_sales_df.empty or all_inventory_df.empty or all_category_df.empty:
            logger.warning("NOTIFICATION_ENGINE: One or more essential data tables are empty.")
    except Exception as e:
        logger.error(f"NOTIFICATION_ENGINE: Failed to load data. Error: {e}", exc_info=True)
        return

    # --- 3. Prepare Inputs and Run Replenishment Engine ---
    messages_to_send = []
    overview_df = pd.DataFrame()

    try:
        logger.info("NOTIFICATION_ENGINE: Preparing data and running replenishment engine...")
        
        # Clean sales data
        all_sales_df['Sale Date'] = pd.to_datetime(all_sales_df['Sale Date'].astype(str).str.strip(), errors='coerce').dt.date
        all_sales_df['Quantity Sold'] = pd.to_numeric(all_sales_df['Quantity Sold'], errors='coerce').fillna(0)
        all_sales_df.dropna(subset=['Sale Date'], inplace=True)
        
        # Prepare all inputs for the engine
        max_date = all_sales_df['Sale Date'].max()
        min_date = max_date - timedelta(days=90)
        daily_sales_for_stats = get_sales_data(all_sales_df, min_date, max_date)
        
        # --- FIX 1: Call calculate_sales_stats with the correct argument ---
        sales_stats = calculate_sales_stats(daily_sales_for_stats, sales_history_days=60)
        
        open_po_data = get_open_po_data(all_pos_df)
        last_order_dates = get_last_order_dates(all_pos_df)
        
        products_df = pd.merge(all_inventory_df, all_category_df, on='MSKU', how='outer')
        if 'Product Type' in products_df.columns:
            products_df = products_df[products_df['Product Type'] != 'DISCONTINUED']

        replen_params = APP_CONFIG.get('replenishment_parameters', {})
        
        overview_df = run_replenishment_engine(products_df, sales_stats, open_po_data, last_order_dates, replen_params)
        
    except Exception as e:
        logger.error(f"NOTIFICATION_ENGINE: Error during replenishment engine execution: {e}", exc_info=True)

    # --- 4. Run Checks Based on Engine Output ---
    if not overview_df.empty:
        try:
            logger.info("NOTIFICATION_ENGINE: Running low stock check...")
            
            # --- THIS IS THE FIX ---
            # Select ALL items that are not "Stock OK" to pass to the generator
            low_stock_products = overview_df[overview_df['order_status'] != '✅ Stock OK']
            
            if not low_stock_products.empty:
                # Adapt the DataFrame to what the message generator expects
                alert_df = low_stock_products[['MSKU', 'Product Type', 'order_status', 'Current Inventory', 'days_of_stock', 'avg_daily_sales']].copy()
                alert_df.rename(columns={'order_status': 'replen_status', 'days_of_stock': 'current_days_coverage'}, inplace=True)
                
                low_stock_message = generate_low_stock_alert(alert_df)
                if low_stock_message:
                    messages_to_send.append(low_stock_message)
            # --- END FIX ---
        except Exception as e:
            logger.error(f"NOTIFICATION_ENGINE: Error during low stock check: {e}", exc_info=True)

    # Check 2: Stale Data Alert
    try:
        logger.info("NOTIFICATION_ENGINE: Running stale data check...")
        waha_config = APP_CONFIG.get('notifications', {})
        threshold_days = waha_config.get('stale_data_threshold_days', 3)
        
        # The 'Sale Date' column is now a proper date object from the cleaning step
        last_sale_dates_by_account = all_sales_df.groupby(['Platform', 'Account Name'])['Sale Date'].max().reset_index()
        
        # --- THIS IS THE FIX for the AttributeError ---
        # Ensure the column is datetime before using .dt accessor
        last_sale_dates_by_account['Sale Date'] = pd.to_datetime(last_sale_dates_by_account['Sale Date'])
        last_sale_dates_by_account['days_since_last_sale'] = (datetime.now() - last_sale_dates_by_account['Sale Date']).dt.days
        # --- END FIX ---
        
        stale_accounts = last_sale_dates_by_account[last_sale_dates_by_account['days_since_last_sale'] > threshold_days]
        
        if not stale_accounts.empty:
            stale_data_message = generate_stale_data_alert(stale_accounts)
            if stale_data_message:
                messages_to_send.append(stale_data_message)
    except Exception as e:
        logger.error(f"NOTIFICATION_ENGINE: Error during stale data check: {e}", exc_info=True)

    # Check 3: Weekly Summary (run only on a specific day, e.g., Monday)
    if datetime.now().weekday() == 0:
        try:
            logger.info("NOTIFICATION_ENGINE: Running weekly summary check...")
            today = datetime.now().date()
            seven_days_ago = today - timedelta(days=6)
            weekly_sales_df = get_sales_data(all_sales_df, seven_days_ago, today)
            weekly_kpis = calculate_total_sales_kpis(weekly_sales_df)
            
            all_pos_df['Order Date'] = pd.to_datetime(all_pos_df['Order Date'], errors='coerce')
            new_pos_count = len(all_pos_df[all_pos_df['Order Date'] >= (datetime.now() - timedelta(days=7))]['Po No.'].unique())
            
            low_stock_count = len(low_stock_products) if 'low_stock_products' in locals() else 0
            
            weekly_message = generate_weekly_summary(weekly_kpis, low_stock_count, new_pos_count)
            if weekly_message:
                messages_to_send.append(weekly_message)
        except Exception as e:
            logger.error(f"NOTIFICATION_ENGINE: Error during weekly summary check: {e}", exc_info=True)

    # --- 4. Send All Generated Messages ---
    if not messages_to_send:
        logger.info("NOTIFICATION_ENGINE: All checks complete. No notifications to send.")
        return

    final_message = "\n\n---\n\n".join(messages_to_send)
    recipients = waha_config.get('recipients', [])
    for recipient in recipients:
        group_id = recipient.get('group_id')
        if group_id:
            sender.send_message_to_group(group_id, final_message)
        else:
            logger.warning(f"Recipient '{recipient.get('name')}' has no group_id configured.")
            
    logger.info("NOTIFICATION_ENGINE: All notifications sent.")