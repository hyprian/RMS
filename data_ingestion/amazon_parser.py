# RMS/data_ingestion/amazon_parser.py
import pandas as pd
from datetime import datetime # Ensure datetime is imported if not already
from .utils import clean_numeric_value, clean_integer_value
import logging
import os # For os.path.basename

logger = logging.getLogger(__name__)

# from .base_parser import BaseSalesParser # If you use a base class

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
        # The first item returned is the original platform SKU string
        return platform_sku_str, ([msku_result] if isinstance(msku_result, str) else msku_result) if msku_result is not None else [None]

    def _get_sku_column_name(self):
        return self.account_config.get('sku_column', 'SKU')

    def _get_quantity_column_names(self):
        qty_cols = self.account_config.get('quantity_columns')
        if qty_cols and isinstance(qty_cols, list): return qty_cols
        # Fallback if only 'quantity_column' (singular) is defined, or default
        qty_col = self.account_config.get('quantity_column')
        if qty_col and isinstance(qty_col, str): return [qty_col]
        return ['Units Ordered', 'Units Ordered - B2B'] # Default for Amazon

    def parse(self, file_path, report_start_date_obj: datetime.date, report_end_date_obj: datetime.date) -> pd.DataFrame:
        logger.info(f"AMZ_PARSER: Starting Amazon parsing for file: {file_path}, Period: {report_start_date_obj} to {report_end_date_obj}")
        sku_col = self._get_sku_column_name()
        logger.debug(f"AMZ_PARSER: Using SKU column: '{sku_col}'")

        try:
            df = pd.read_csv(file_path, dtype={sku_col: str})
        except Exception as e:
            logger.error(f"AMZ_PARSER: Error reading Amazon file {file_path}: {e}", exc_info=True)
            return pd.DataFrame()

        if df.empty:
            logger.warning(f"AMZ_PARSER: Amazon file {file_path} is empty.")
            return pd.DataFrame()

        processed_records = []
        qty_cols = self._get_quantity_column_names()
        
        revenue_col_consumer = "Ordered Product Sales" # As per your sample
        revenue_col_b2b = "Ordered Product Sales - B2B" # As per your sample

        # Check if essential columns exist
        required_cols_from_file = [sku_col] + qty_cols
        if revenue_col_consumer not in df.columns and revenue_col_consumer not in qty_cols : required_cols_from_file.append(revenue_col_consumer)
        if revenue_col_b2b not in df.columns and revenue_col_b2b not in qty_cols : required_cols_from_file.append(revenue_col_b2b)
        
        missing_cols = [col for col in required_cols_from_file if col not in df.columns]
        if missing_cols:
            logger.error(f"AMZ_PARSER: Missing required columns {missing_cols} in Amazon report {file_path}. Available: {df.columns.tolist()}")
            return pd.DataFrame()

        for index, row in df.iterrows():
            platform_sku_from_file = row.get(sku_col)
            original_platform_sku, msku_list = self._map_sku(platform_sku_from_file)

            total_quantity_period = sum(clean_integer_value(row.get(qc, 0)) for qc in qty_cols)
            gross_revenue_period = clean_numeric_value(row.get(revenue_col_consumer, "0")) + \
                                   clean_numeric_value(row.get(revenue_col_b2b, "0"))

            # Amazon Business Reports are aggregated for the period.
            # We store one record representing the total for that period.
            # 'Sale Date' will store the end of the report period.
            # 'Report Period Start Date' will store the start.
            
            for msku_item in msku_list: # msku_item can be None
                record = {
                    'Sale Date': report_end_date_obj.strftime('%Y-%m-%d'), # End of the period
                    'Report Period Start Date': report_start_date_obj.strftime('%Y-%m-%d'), # NEW
                    'MSKU': msku_item, # Could be None if unmapped
                    'Platform': self.platform_name,
                    'Account Name': self.account_config.get('name', self.account_slug),
                    'Platform SKU': original_platform_sku,
                    'Order ID': None, 
                    'Quantity Sold': total_quantity_period,
                    'Gross Revenue': gross_revenue_period,
                    'Discounts': 0.0, # Default, not in this report type
                    'Platform Fees': 0.0, # Default, not in this report type
                    'Net Revenue': gross_revenue_period, # Assuming Gross = Net for this report type
                    'Report Source File': os.path.basename(file_path)
                    # 'Upload Batch ID', 'Data Processed Timestamp', 'COGS per Unit' will be added later
                }
                processed_records.append(record)
        
        if not processed_records:
            logger.warning(f"AMZ_PARSER: No records processed from Amazon file {file_path}.")
            return pd.DataFrame()

        result_df = pd.DataFrame(processed_records)
        logger.info(f"AMZ_PARSER: Successfully parsed {len(result_df)} summary records from Amazon file: {file_path}")
        return result_df