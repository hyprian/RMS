# RMS/data_ingestion/amazon_parser.py
import pandas as pd
from datetime import datetime, timedelta
from .utils import clean_numeric_value, clean_integer_value
import os
# from .base_parser import BaseSalesParser # Uncomment if using base class

# class AmazonSalesParser(BaseSalesParser): # Uncomment if using base class
class AmazonSalesParser: # Standalone version
    def __init__(self, platform_name, account_config, sku_mapper):
        # super().__init__(platform_name, account_config, sku_mapper) # Uncomment if using base class
        self.platform_name = platform_name
        self.account_config = account_config
        self.sku_mapper = sku_mapper
        self.account_slug = account_config.get('slug', 'unknown_amazon_account')


    def _map_sku(self, platform_sku): # Duplicated from Base for standalone
        if not platform_sku or pd.isna(platform_sku): return str(platform_sku), [None]
        msku_result = self.sku_mapper.map_sku_to_msku(str(platform_sku))
        if msku_result is None: return str(platform_sku), [None]
        elif isinstance(msku_result, str): return str(platform_sku), [msku_result]
        elif isinstance(msku_result, list):
            valid_mskus = [m for m in msku_result if m and not pd.isna(m)]
            return str(platform_sku), valid_mskus if valid_mskus else [None]
        return str(platform_sku), [None]

    def _get_sku_column_name(self): # Duplicated from Base for standalone
        return self.account_config.get('sku_column', 'SKU')

    def _get_quantity_column_names(self): # Duplicated from Base for standalone
        qty_cols = self.account_config.get('quantity_columns')
        if qty_cols and isinstance(qty_cols, list): return qty_cols
        qty_col = self.account_config.get('quantity_column')
        if qty_col and isinstance(qty_col, str): return [qty_col]
        return ['Units Ordered', 'Units Ordered - B2B'] # Default for Amazon

    def parse(self, file_path, report_start_date, report_end_date) -> pd.DataFrame:
        try:
            df = pd.read_csv(file_path, dtype={self._get_sku_column_name(): str})
        except Exception as e:
            print(f"Error reading Amazon file {file_path}: {e}") # Replace with logger
            return pd.DataFrame()

        processed_records = []
        sku_col = self._get_sku_column_name()
        qty_cols = self._get_quantity_column_names() # e.g., ["Units Ordered", "Units Ordered - B2B"]
        
        # Revenue columns specific to Amazon sample
        # These should ideally come from config if they vary, or be robustly identified
        revenue_col_consumer = "Ordered Product Sales"
        revenue_col_b2b = "Ordered Product Sales - B2B"

        # Check if essential columns exist
        required_cols_for_processing = [sku_col] + qty_cols
        if revenue_col_consumer not in df.columns: required_cols_for_processing.append(revenue_col_consumer) # only add if not already in qty_cols
        if revenue_col_b2b not in df.columns: required_cols_for_processing.append(revenue_col_b2b)

        for col in required_cols_for_processing:
            if col not in df.columns:
                print(f"Missing required column '{col}' in Amazon report {file_path}") # Replace with logger
                return pd.DataFrame()

        num_days = (report_end_date - report_start_date).days + 1
        if num_days <= 0: num_days = 1 # Avoid division by zero if dates are same

        for index, row in df.iterrows():
            platform_sku_original = row[sku_col]
            original_sku_for_mapping, msku_list = self._map_sku(platform_sku_original)

            total_quantity_period = sum(clean_integer_value(row.get(qc, 0)) for qc in qty_cols)
            
            # For Amazon, revenue is often reported as a string with currency
            gross_revenue_period = clean_numeric_value(row.get(revenue_col_consumer, "0")) + \
                                   clean_numeric_value(row.get(revenue_col_b2b, "0"))

            # Distribute sales evenly across the report period for daily records
            # Or create one record for the period with report_end_date as Sale Date
            # Decision: Create ONE record per SKU for the entire period, Sale Date = report_end_date
            
            for msku in msku_list:
                if msku is None: # Handle unmapped SKUs if needed, or skip
                    # For now, we'll create a record with MSKU as None if unmapped
                    # This allows it to be flagged in Baserow or analytics
                    pass # MSKU is already None

                processed_records.append({
                    'Sale Date': report_end_date.strftime('%Y-%m-%d'), # Single date for the period
                    'MSKU': msku,
                    'Platform': self.platform_name,
                    'Account Name': self.account_config.get('name', self.account_slug),
                    'Platform SKU': original_sku_for_mapping,
                    'Order ID': None, # Amazon business report doesn't give order IDs per SKU row
                    'Quantity Sold': total_quantity_period, # Total for the period
                    'Gross Revenue': gross_revenue_period,  # Total for the period
                    'Discounts': 0, # Not directly in this Amazon report sample per SKU
                    'Platform Fees': 0, # Not directly in this report sample
                    'Net Revenue': gross_revenue_period, # Assuming gross = net for now from this report
                    'Report Source File': os.path.basename(file_path)
                    # 'Upload Batch ID' and 'Data Processed Timestamp' will be added later
                })
        
        return pd.DataFrame(processed_records)