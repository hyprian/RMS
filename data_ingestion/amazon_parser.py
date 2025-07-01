# RMS/data_ingestion/amazon_parser.py
import pandas as pd
from datetime import datetime
from .utils import clean_numeric_value, clean_integer_value
import logging
import os

logger = logging.getLogger(__name__)

class AmazonSalesParser:
    def __init__(self, platform_name, account_config, sku_mapper):
        self.platform_name = platform_name
        self.account_config = account_config
        self.sku_mapper = sku_mapper
        self.account_slug = account_config.get('slug', 'unknown_amazon_account')

    def _map_sku(self, platform_sku_in):
        platform_sku_str = str(platform_sku_in).strip() if pd.notna(platform_sku_in) else ""
        if not platform_sku_str:
            logger.debug("AMZ_PARSER: Empty or NaN platform SKU received in _map_sku.")
            return "", [None]
        msku_result = self.sku_mapper.map_sku_to_msku(platform_sku_str)
        return platform_sku_str, ([msku_result] if isinstance(msku_result, str) else msku_result) if msku_result is not None else [None]

    def _get_sku_column_name(self):
        return self.account_config.get('sku_column', 'Sku')

    def _get_quantity_column_names(self):
        qty_col = self.account_config.get('quantity_column', 'Quantity')
        return [qty_col]

    def parse(self, file_path, report_start_date_obj: datetime.date, report_end_date_obj: datetime.date) -> pd.DataFrame:
        logger.info(f"AMZ_PARSER: Starting NEW daily Amazon parsing for file: {file_path}")
        sku_col = self._get_sku_column_name()
        logger.debug(f"AMZ_PARSER: Using SKU column: '{sku_col}'")

        try:
            # Use a more robust CSV reading, especially if some rows have missing fields
            df = pd.read_csv(file_path, dtype={sku_col: str}, low_memory=False)
        except Exception as e:
            logger.error(f"AMZ_PARSER: Error reading Amazon file {file_path}: {e}", exc_info=True)
            return pd.DataFrame()

        if df.empty:
            logger.warning(f"AMZ_PARSER: Amazon file {file_path} is empty.")
            return pd.DataFrame()

        # --- Define columns for the NEW report format ---
        qty_cols = self._get_quantity_column_names()
        date_col = "Invoice Date"
        transaction_type_col = "Transaction Type"
        revenue_col = "Tax Exclusive Gross"
        order_id_col = "Order Id"

        required_cols = [sku_col, date_col, transaction_type_col, revenue_col, order_id_col] + qty_cols
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"AMZ_PARSER: Missing required columns {missing_cols} in new Amazon report format. Available: {df.columns.tolist()}")
            return pd.DataFrame()

        # --- Filter out Refunds and Cancels ---
        invalid_transaction_types = ["Refund", "Cancel"]
        df_filtered = df[~df[transaction_type_col].isin(invalid_transaction_types)].copy()
        logger.info(f"AMZ_PARSER: Original rows: {len(df)}, Rows after filtering out Refunds/Cancels: {len(df_filtered)}")

        if df_filtered.empty:
            logger.warning(f"AMZ_PARSER: No valid sales transactions found after filtering in {file_path}")
            return pd.DataFrame()

        # --- Filter by Date (with more robust parsing) ---
        # Let Pandas infer the date format by removing the `format` argument.
        # This is much more flexible.
        df_filtered[date_col] = pd.to_datetime(df_filtered[date_col], errors='coerce')
        
        # Log how many dates failed to parse
        num_failed_dates = df_filtered[date_col].isnull().sum()
        if num_failed_dates > 0:
            logger.warning(f"AMZ_PARSER: Could not parse {num_failed_dates} dates in the '{date_col}' column.")
        
        df_filtered.dropna(subset=[date_col], inplace=True)
        
        # Now filter by the date range selected by the user
        df_filtered = df_filtered[
            (df_filtered[date_col].dt.date >= report_start_date_obj) & 
            (df_filtered[date_col].dt.date <= report_end_date_obj)
        ].copy()

        if df_filtered.empty:
            logger.warning(f"AMZ_PARSER: No data found for the selected date range: {report_start_date_obj} to {report_end_date_obj} after date parsing and filtering.")
            return pd.DataFrame()

        # --- The rest of the processing and aggregation logic remains the same ---
        temp_data_for_aggregation = []
        for index, row in df_filtered.iterrows():
            platform_sku_from_file = row[sku_col]
            original_platform_sku, msku_list = self._map_sku(platform_sku_from_file)
            
            sale_date_dt = row[date_col]
            quantity_sold = sum(clean_integer_value(row.get(qc, 0)) for qc in qty_cols)
            net_revenue = clean_numeric_value(row.get(revenue_col, 0))
            order_id = row.get(order_id_col)

            for msku_item in msku_list:
                temp_data_for_aggregation.append({
                    'Sale Date': sale_date_dt.date(),
                    'MSKU': msku_item,
                    'Platform': self.platform_name,
                    'Account Name': self.account_config.get('name', self.account_slug),
                    'Platform SKU': original_platform_sku,
                    'Order ID': order_id,
                    'Quantity Sold': quantity_sold,
                    'Net Revenue': net_revenue,
                    'Report Source File': os.path.basename(file_path)
                })

        if not temp_data_for_aggregation:
            logger.warning(f"AMZ_PARSER: No records to aggregate after processing rows in {file_path}")
            return pd.DataFrame()

        agg_df = pd.DataFrame(temp_data_for_aggregation)
        agg_df['MSKU_agg'] = agg_df['MSKU'].fillna('__UNMAPPED__')

        agg_functions = {
            'Quantity Sold': 'sum',
            'Net Revenue': 'sum',
            'Platform SKU': 'first',
            'Order ID': lambda x: ', '.join(x.unique()),
            'Report Source File': 'first'
        }
        
        grouped_df = agg_df.groupby(
            ['Sale Date', 'MSKU_agg', 'Platform', 'Account Name'], 
            as_index=False
        ).agg(agg_functions)
        
        grouped_df.rename(columns={'MSKU_agg': 'MSKU'}, inplace=True)
        grouped_df['MSKU'] = grouped_df['MSKU'].replace('__UNMAPPED__', None)
        grouped_df['Sale Date'] = grouped_df['Sale Date'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)

        grouped_df['Gross Revenue'] = grouped_df['Net Revenue']
        grouped_df['Discounts'] = 0.0
        grouped_df['Platform Fees'] = 0.0
        
        logger.info(f"AMZ_PARSER: Successfully parsed and aggregated {len(grouped_df)} daily records from Amazon file: {file_path}")
        return grouped_df