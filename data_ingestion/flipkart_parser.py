# RMS/data_ingestion/flipkart_parser.py
import pandas as pd
from datetime import datetime
from .utils import clean_numeric_value, clean_integer_value
import logging # Import logging
import os

logger = logging.getLogger(__name__) # Use logger

# from .base_parser import BaseSalesParser # If you implement the base class

# class FlipkartSalesParser(BaseSalesParser):
class FlipkartSalesParser:
    def __init__(self, platform_name, account_config, sku_mapper):
        # super().__init__(platform_name, account_config, sku_mapper)
        self.platform_name = platform_name
        self.account_config = account_config
        self.sku_mapper = sku_mapper
        self.account_slug = account_config.get('slug', 'unknown_flipkart_account')

    def _map_sku(self, platform_sku_in): # Renamed input to avoid confusion
        platform_sku_str = str(platform_sku_in).strip() if pd.notna(platform_sku_in) else ""
        if not platform_sku_str: 
            logger.debug("Empty or NaN platform SKU received in _map_sku")
            return "", [None] # Return empty string for original if it was empty/NaN

        msku_result = self.sku_mapper.map_sku_to_msku(platform_sku_str) # map_sku_to_msku handles normalization
        
        # The map_sku_to_msku in SKUMapper already logs warnings for unmapped SKUs.
        # The first item returned by _map_sku should always be the platform_sku_str itself (or a cleaned version if _map_sku did that)
        
        if msku_result is None:
            return platform_sku_str, [None]
        elif isinstance(msku_result, str):
            return platform_sku_str, [msku_result]
        elif isinstance(msku_result, list):
            valid_mskus = [m for m in msku_result if m and not pd.isna(m)]
            return platform_sku_str, valid_mskus if valid_mskus else [None]
        return platform_sku_str, [None]


    def _get_sku_column_name(self):
        return self.account_config.get('sku_column', 'SKU ID')

    def _get_quantity_column_names(self):
        qty_col = self.account_config.get('quantity_column', 'Final Sale Units')
        return [qty_col]

    def parse(self, file_path, report_start_date, report_end_date) -> pd.DataFrame:
        logger.info(f"Starting Flipkart parsing for file: {file_path}")
        sku_col = self._get_sku_column_name()
        logger.debug(f"Flipkart parser using SKU column: '{sku_col}'")

        try:
            df = pd.read_excel(file_path, dtype={sku_col: str}, engine='openpyxl')
        except Exception as e:
            logger.error(f"Error reading Flipkart file {file_path}: {e}", exc_info=True)
            return pd.DataFrame()

        if df.empty:
            logger.warning(f"Flipkart file {file_path} is empty.")
            return pd.DataFrame()

        qty_cols = self._get_quantity_column_names()
        date_col = self.account_config.get('report_settings', {}).get('date_column', 'Order Date')
        revenue_col = self.account_config.get('report_settings', {}).get('revenue_column', 'Final Sale Amount')

        required_cols_for_processing = [sku_col, date_col] + qty_cols + [revenue_col]
        missing_cols = [col for col in required_cols_for_processing if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns {missing_cols} in Flipkart report {file_path}. Available: {df.columns.tolist()}")
            return pd.DataFrame()

        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df.dropna(subset=[date_col], inplace=True)
        df_filtered = df[(df[date_col].dt.date >= report_start_date) & (df[date_col].dt.date <= report_end_date)].copy() # Use .copy()

        if df_filtered.empty:
            logger.warning(f"No data in Flipkart report {file_path} for the selected date range: {report_start_date} to {report_end_date}")
            return pd.DataFrame()

        temp_data_for_aggregation = []
        for index, row in df_filtered.iterrows():
            platform_sku_from_file = row[sku_col]
            # original_platform_sku will be the string version of platform_sku_from_file,
            # msku_list will contain the mapped MSKU(s) or [None]
            original_platform_sku, msku_list = self._map_sku(platform_sku_from_file)
            
            sale_date_dt = row[date_col]
            quantity_sold = sum(clean_integer_value(row.get(qc, 0)) for qc in qty_cols)
            net_revenue = clean_numeric_value(row.get(revenue_col, 0))

            for msku_item in msku_list: # msku_item can be None if unmapped
                temp_data_for_aggregation.append({
                    'Sale Date': sale_date_dt.date(), # Store as date object for now, convert to string later
                    'MSKU': msku_item, # This will be None if unmapped
                    'Platform': self.platform_name,
                    'Account Name': self.account_config.get('name', self.account_slug),
                    'Platform SKU': original_platform_sku, # This is the crucial part
                    'Quantity Sold': quantity_sold,
                    'Net Revenue': net_revenue,
                    'Report Source File': os.path.basename(file_path)
                })
        
        if not temp_data_for_aggregation:
            logger.warning(f"No records to aggregate after processing rows in {file_path}")
            return pd.DataFrame()

        # Create DataFrame for aggregation
        agg_df = pd.DataFrame(temp_data_for_aggregation)
        
        # Handle potential None MSKUs before groupby by filling with a placeholder, then reverting
        agg_df['MSKU_agg'] = agg_df['MSKU'].fillna('__UNMAPPED__')

        # Define aggregation functions
        agg_functions = {
            'Quantity Sold': 'sum',
            'Net Revenue': 'sum',
            'Platform SKU': 'first', # Takes the first platform SKU encountered for that group
            'Report Source File': 'first' 
        }
        
        # Group by the keys that define a unique daily sales record
        grouped_df = agg_df.groupby(
            ['Sale Date', 'MSKU_agg', 'Platform', 'Account Name'], 
            as_index=False
        ).agg(agg_functions)
        
        # Revert placeholder for MSKU
        grouped_df.rename(columns={'MSKU_agg': 'MSKU'}, inplace=True)
        grouped_df['MSKU'] = grouped_df['MSKU'].replace('__UNMAPPED__', None)

        # Convert Sale Date back to string YYYY-MM-DD
        grouped_df['Sale Date'] = grouped_df['Sale Date'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)

        # Add missing standard columns that are not directly from the report or aggregation
        grouped_df['Order ID'] = None 
        grouped_df['Gross Revenue'] = grouped_df['Net Revenue'] # Assuming Net = Gross for Flipkart
        grouped_df['Discounts'] = 0
        grouped_df['Platform Fees'] = 0
        
        logger.info(f"Successfully parsed and aggregated {len(grouped_df)} records from Flipkart file: {file_path}")
        return grouped_df