# RMS/notifications/message_generator.py
import pandas as pd
from datetime import datetime

def generate_low_stock_alert(low_stock_df: pd.DataFrame) -> str | None:
    """
    Generates a structured WhatsApp message for low stock products,
    separating them by FOCUSED, NON-FOCUSED, and ZERO SALES.
    """
    if low_stock_df is None or low_stock_df.empty:
        return None

    # --- FIX: Use the correct column name from the complex engine ---
    # The engine provides 'avg_daily_sales_30d', not 'avg_daily_sales'
    avg_sales_col = 'avg_daily_sales'
    
    # --- Data Segmentation ---
    selling_products = low_stock_df[low_stock_df[avg_sales_col] > 0].copy()
    focused_low_stock = selling_products[selling_products['Product Type'] == 'FOCUSED']
    non_focused_low_stock = selling_products[selling_products['Product Type'] == 'NON-FOCUSED']
    zero_sales_low_stock = low_stock_df[low_stock_df[avg_sales_col] == 0].copy()

    # --- Message Construction ---
    message_parts = []
    message_parts.append("*RMS Low Stock Alert!* 🚨")
    has_content = False

    # Section 1: FOCUSED Products
    if not focused_low_stock.empty:
        has_content = True
        message_parts.append("\n\n*🔥 FOCUSED Products - Action Required:*")
        for index, row in focused_low_stock.iterrows():
            msku = row.get('MSKU', 'N/A')
            status = row.get('replen_status', '').replace("🚨", "").replace("⚠️", "").strip()
            current_inv = int(row.get('Current Inventory', 0))
            # --- FIX: Use the correct column name ---
            days_coverage = row.get('current_days_coverage', 0)
            days_coverage_str = f"{days_coverage:.1f} days"
            line = f"- *{msku}*\n  _{status}_ | Stock: *{current_inv}* units ({days_coverage_str})"
            message_parts.append(line)

    # Section 2: NON-FOCUSED Products
    if not non_focused_low_stock.empty:
        has_content = True
        message_parts.append("\n\n*📈 NON-FOCUSED Products - Action Required:*")
        for index, row in non_focused_low_stock.iterrows():
            msku = row.get('MSKU', 'N/A')
            status = row.get('replen_status', '').replace("🚨", "").replace("⚠️", "").strip()
            current_inv = int(row.get('Current Inventory', 0))
            # --- FIX: Use the correct column name ---
            days_coverage = row.get('current_days_coverage', 0)
            days_coverage_str = f"{days_coverage:.1f} days"
            line = f"- *{msku}*\n  _{status}_ | Stock: *{current_inv}* units ({days_coverage_str})"
            message_parts.append(line)

    # Section 3: Zero Sales Products
    if not zero_sales_low_stock.empty:
        has_content = True
        message_parts.append("\n\n*❓ Low Stock & ZERO Sales - Please Investigate:*")
        message_parts.append("_(These items have low inventory but no sales in the last 30 days. Check for listing issues or if they should be marked 'Discontinued'.)_")
        for index, row in zero_sales_low_stock.iterrows():
            msku = row.get('MSKU', 'N/A')
            current_inv = int(row.get('Current Inventory', 0))
            line = f"- *{msku}* | Stock: *{current_inv}* units"
            message_parts.append(line)

    if not has_content:
        return None

    message_parts.append("\n\nPlease review in the RMS dashboard.")
    return "\n".join(message_parts)


def generate_stale_data_alert(stale_accounts_df: pd.DataFrame) -> str | None:
    """
    Generates a formatted WhatsApp message for accounts with stale sales data.
    """
    if stale_accounts_df is None or stale_accounts_df.empty:
        return None

    message_parts = []
    message_parts.append("*RMS Data Alert: Stale Data* ⚠️")
    message_parts.append("\nThe following accounts have not had sales data uploaded recently:\n")

    for index, row in stale_accounts_df.iterrows():
        platform = row.get('Platform', 'N/A')
        account = row.get('Account Name', 'N/A')
        days_since_update = int(row.get('days_since_last_sale', 0))
        
        line = f"- *{platform} - {account}*: Last sale data is *{days_since_update} days* old."
        message_parts.append(line)

    message_parts.append("\nProjections and replenishment plans may be inaccurate. Please upload new sales reports.")
    
    return "\n".join(message_parts)


def generate_weekly_summary(kpi_data: dict, low_stock_count: int, new_pos_count: int) -> str | None:
    """
    Generates a formatted weekly summary message.
    """
    if not kpi_data:
        return None

    # Format numbers with commas
    total_revenue = f"₹{kpi_data.get('total_net_revenue', 0):,.2f}"
    total_units = f"{kpi_data.get('total_units_sold', 0):,.0f}"
    
    message_parts = []
    message_parts.append(f"*RMS Weekly Summary: {datetime.now().strftime('%d-%b-%Y')}* 📊\n")
    message_parts.append(f"- *Total Sales (Last 7d):* {total_revenue}")
    message_parts.append(f"- *Total Units Sold (Last 7d):* {total_units}")
    message_parts.append(f"- *Products Requiring Orders:* {low_stock_count}")
    message_parts.append(f"- *New POs Created (Last 7d):* {new_pos_count}")
    message_parts.append("\nHave a great week!")
    
    return "\n".join(message_parts)