# RMS/data_ingestion/base_parser.py
from abc import ABC, abstractmethod
import pandas as pd

class BaseSalesParser(ABC):
    def __init__(self, platform_name, account_config, sku_mapper):
        self.platform_name = platform_name
        self.account_config = account_config # Contains sku_column, quantity_column(s) etc.
        self.sku_mapper = sku_mapper

    @abstractmethod
    def parse(self, file_path, report_start_date, report_end_date) -> pd.DataFrame:
        """
        Parses the given sales report file and returns a standardized DataFrame.

        The DataFrame should have columns matching the target Baserow table:
        'Sale Date', 'MSKU', 'Platform', 'Account Name', 'Platform SKU', 
        'Order ID', 'Quantity Sold', 'Gross Revenue', 'Discounts', 
        'Platform Fees', 'Net Revenue', 'Report Source File', 
        'Data Processed Timestamp' (added later), 'Upload Batch ID' (added later)
        """
        pass

    def _map_sku(self, platform_sku):
        if not platform_sku or pd.isna(platform_sku):
            return None, [None] # Return original and list of MSKUs (even if single)
        
        msku_result = self.sku_mapper.map_sku_to_msku(str(platform_sku))
        
        if msku_result is None:
            return str(platform_sku), [None]
        elif isinstance(msku_result, str):
            return str(platform_sku), [msku_result]
        elif isinstance(msku_result, list):
            # For sales data, if a combo SKU maps to multiple MSKUs,
            # we need to decide how to attribute the sale.
            # Option 1: Attribute the full sale to each component (can overstate component sales).
            # Option 2: Attribute to the first component MSKU.
            # Option 3: Skip or flag combo sales that need manual attribution.
            # For now, let's take the first valid MSKU from the list for simplicity in sales aggregation.
            # This might need refinement based on business rules for combo sales.
            valid_mskus = [m for m in msku_result if m and not pd.isna(m)]
            return str(platform_sku), valid_mskus if valid_mskus else [None]
        return str(platform_sku), [None]

    def _get_sku_column_name(self):
        return self.account_config.get('sku_column', 'SKU') # Default to 'SKU'

    def _get_quantity_column_names(self):
        qty_cols = self.account_config.get('quantity_columns')
        if qty_cols and isinstance(qty_cols, list):
            return qty_cols
        qty_col = self.account_config.get('quantity_column')
        if qty_col and isinstance(qty_col, str):
            return [qty_col]
        return ['Quantity'] # Default