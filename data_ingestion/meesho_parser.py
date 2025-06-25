# RMS/data_ingestion/meesho_parser.py
import pandas as pd
from datetime import datetime
from .utils import clean_numeric_value, clean_integer_value
import logging
import os

logger = logging.getLogger(__name__)

# from .base_parser import BaseSalesParser # If you implement the base class

# class MeeshoSalesParser(BaseSalesParser):
class MeeshoSalesParser:
    def __init__(self, platform_name, account_config, sku_mapper):
        # super().__init__(platform_name, account_config, sku_mapper)
        self.platform_name = platform_name
        self.account_config = account_config
        self.sku_mapper = sku_mapper
        self.account_slug = account_config.get('slug', 'unknown_meesho_account')

    def _map_sku(self, platform_sku_in):
        platform_sku_str = str(platform_sku_in).strip() if pd.notna(platform_sku_in) else ""
        if not platform_sku_str:
            logger.debug("Empty or NaN platform SKU received in _map_sku for Meesho.")
            return "", [None]

        msku_result = self.sku_mapper.map_sku_to_msku(platform_sku_str)
        
        return platform_sku_str, ([msku_result] if isinstance(msku_result, str) else msku_result) if msku_result is not None else [None]


    def _get_sku_column_name(self):
        return self.account_config.get('sku_column', 'SKU')

    def _get_quantity_column_names(self):
        qty_col = self.account_config.get('quantity_column', 'Quantity')
        return [qty_col]

    def parse(self, file_path, report_start_date, report_end_date) -> pd.DataFrame:
        logger.info(f"Starting Meesho parsing for file: {file_path}")
        sku_col = self._get_sku_column_name()
        logger.debug(f"Meesho parser using SKU column: '{sku_col}'")

        try:
            # Specify encoding if necessary, Meesho often uses utf-8
            df = pd.read_csv(file_path, dtype={sku_col: str}, encoding='utf-8')
        except Exception as e:
            logger.error(f"Error reading Meesho file {file_path}: {e}", exc_info=True)
            return pd.DataFrame()

        if df.empty:
            logger.warning(f"Meesho file {file_path} is empty.")
            return pd.DataFrame()

        qty_cols = self._get_quantity_column_names()
        
        # Column names from Meesho sample - make these configurable via report_settings if they vary
        status_col = self.account_config.get('report_settings', {}).get('status_column', "Reason for Credit Entry")
        valid_statuses = self.account_config.get('report_settings', {}).get('valid_sale_statuses', ["SHIPPED"])
        date_col = self.account_config.get('report_settings', {}).get('date_column', "Order Date")
        revenue_col = self.account_config.get('report_settings', {}).get('revenue_column', "Supplier Discounted Price (Incl GST and Commision)")
        order_id_col = "Sub Order No" # From sample

        required_cols_for_processing = [sku_col, date_col, status_col, revenue_col, order_id_col] + qty_cols
        missing_cols = [col for col in required_cols_for_processing if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns {missing_cols} in Meesho report {file_path}. Available: {df.columns.tolist()}")
            return pd.DataFrame()

        # Filter by status
        df_status_filtered = df[df[status_col].astype(str).str.upper().isin([s.upper() for s in valid_statuses])].copy()
        if df_status_filtered.empty:
            logger.warning(f"No valid sales entries after status filter in Meesho report {file_path}")
            return pd.DataFrame()

        # Filter by report_start_date and report_end_date
        df_status_filtered[date_col] = pd.to_datetime(df_status_filtered[date_col], errors='coerce')
        df_status_filtered.dropna(subset=[date_col], inplace=True)
        df_filtered = df_status_filtered[(df_status_filtered[date_col].dt.date >= report_start_date) & (df_status_filtered[date_col].dt.date <= report_end_date)].copy()

        if df_filtered.empty:
            logger.warning(f"No data in Meesho report {file_path} for the selected date range: {report_start_date} to {report_end_date}")
            return pd.DataFrame()

        temp_data_for_aggregation = []
        for index, row in df_filtered.iterrows():
            platform_sku_from_file = row[sku_col]
            original_platform_sku, msku_list = self._map_sku(platform_sku_from_file)
            
            logger.debug(f"Meesho row: SKU='{platform_sku_from_file}', Mapped Original='{original_platform_sku}', Mapped MSKUs='{msku_list}'")


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
                    'Platform SKU': original_platform_sku, # Crucial line
                    'Order ID': order_id,
                    'Quantity Sold': quantity_sold,
                    'Net Revenue': net_revenue,
                    'Report Source File': os.path.basename(file_path)
                })
        
        if not temp_data_for_aggregation:
            logger.warning(f"No records to aggregate after processing rows in {file_path}")
            return pd.DataFrame()

        agg_df = pd.DataFrame(temp_data_for_aggregation)
        logger.debug(f"Meesho agg_df head before grouping:\n{agg_df.head().to_string()}")
        if 'Platform SKU' not in agg_df.columns:
            logger.error("CRITICAL: 'Platform SKU' column is missing before aggregation in Meesho parser!")
        elif agg_df['Platform SKU'].isnull().all():
             logger.warning("WARNING: All 'Platform SKU' values are null before aggregation in Meesho parser!")


        agg_df['MSKU_agg'] = agg_df['MSKU'].fillna('__UNMAPPED__')
        
        agg_functions = {
            'Quantity Sold': 'sum',
            'Net Revenue': 'sum',
            'Platform SKU': 'first', 
            'Order ID': 'first', 
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
        grouped_df['Discounts'] = 0
        grouped_df['Platform Fees'] = 0
        
        logger.info(f"Successfully parsed and aggregated {len(grouped_df)} records from Meesho file: {file_path}")
        logger.debug(f"Meesho grouped_df head after aggregation:\n{grouped_df.head().to_string()}")
        if 'Platform SKU' in grouped_df.columns and grouped_df['Platform SKU'].isnull().all():
            logger.warning("WARNING: All 'Platform SKU' values are null AFTER aggregation in Meesho parser!")


        return grouped_df