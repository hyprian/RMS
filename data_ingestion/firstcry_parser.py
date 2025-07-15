# RMS/data_ingestion/firstcry_parser.py
import pandas as pd
from datetime import datetime
from .utils import clean_numeric_value, clean_integer_value
import logging
import os

logger = logging.getLogger(__name__)

class FirstCrySalesParser:
    def __init__(self, platform_name, account_config, sku_mapper):
        self.platform_name = platform_name
        self.account_config = account_config
        self.sku_mapper = sku_mapper
        self.account_slug = account_config.get('slug', 'unknown_firstcry_account')

    def _map_sku(self, platform_sku_in):
        platform_sku_str = str(platform_sku_in).strip() if pd.notna(platform_sku_in) else ""
        if not platform_sku_str:
            return "", [None]
        msku_result = self.sku_mapper.map_sku_to_msku(platform_sku_str)
        return platform_sku_str, ([msku_result] if isinstance(msku_result, str) else msku_result) if msku_result is not None else [None]

    def _get_sku_column_name(self):
        return self.account_config.get('sku_column', 'VendorStyleCode')

    def _get_quantity_column_names(self):
        qty_col = self.account_config.get('quantity_column', 'Quantity')
        return [qty_col]

    def parse(self, file_path, report_start_date, report_end_date) -> pd.DataFrame:
        logger.info(f"FIRSTCRY_PARSER: Starting FirstCry parsing for file: {file_path}")
        sku_col = self._get_sku_column_name()
        
        try:
            # FirstCry report is XLSX
            df = pd.read_excel(file_path, dtype={sku_col: str}, engine='openpyxl')
        except Exception as e:
            logger.error(f"FIRSTCRY_PARSER: Error reading FirstCry file {file_path}: {e}", exc_info=True)
            return pd.DataFrame()

        if df.empty:
            logger.warning(f"FIRSTCRY_PARSER: FirstCry file {file_path} is empty.")
            return pd.DataFrame()

        qty_cols = self._get_quantity_column_names()
        report_settings = self.account_config.get('report_settings', {})
        date_col = report_settings.get('date_column', 'OrderDate')
        revenue_col = report_settings.get('revenue_column', 'MRP Sales')
        order_id_col = "POID" # From your sample

        required_cols = [sku_col, date_col, revenue_col, order_id_col] + qty_cols
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"FIRSTCRY_PARSER: Missing required columns {missing_cols} in FirstCry report {file_path}.")
            return pd.DataFrame()

        # Handle FirstCry's date format: 2025-06-16 12:33:00
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df.dropna(subset=[date_col], inplace=True)
        
        # Filter by the user-selected date range
        df_filtered = df[(df[date_col].dt.date >= report_start_date) & (df[date_col].dt.date <= report_end_date)].copy()

        if df_filtered.empty:
            logger.warning(f"FIRSTCRY_PARSER: No data in report for the selected date range.")
            return pd.DataFrame()

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
            return pd.DataFrame()

        agg_df = pd.DataFrame(temp_data_for_aggregation)
        agg_df['MSKU_agg'] = agg_df['MSKU'].fillna('__UNMAPPED__')
        
        agg_functions = {'Quantity Sold': 'sum', 'Net Revenue': 'sum', 'Platform SKU': 'first', 'Order ID': 'first', 'Report Source File': 'first'}
        
        grouped_df = agg_df.groupby(['Sale Date', 'MSKU_agg', 'Platform', 'Account Name'], as_index=False).agg(agg_functions)
        
        grouped_df.rename(columns={'MSKU_agg': 'MSKU'}, inplace=True)
        grouped_df['MSKU'] = grouped_df['MSKU'].replace('__UNMAPPED__', None)
        grouped_df['Sale Date'] = grouped_df['Sale Date'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)

        # Add missing standard columns
        grouped_df['Gross Revenue'] = grouped_df['Net Revenue'] # Assuming 'MRP Sales' is net for now
        grouped_df['Discounts'] = 0
        grouped_df['Platform Fees'] = 0
        
        logger.info(f"FIRSTCRY_PARSER: Successfully parsed and aggregated {len(grouped_df)} records from FirstCry file.")
        return grouped_df