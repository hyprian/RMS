# RMS/data_ingestion/shopify_parser.py
import pandas as pd
from datetime import datetime
from .utils import clean_numeric_value, clean_integer_value
import logging
import os

logger = logging.getLogger(__name__)

class ShopifySalesParser:
    def __init__(self, platform_name, account_config, sku_mapper):
        self.platform_name = platform_name
        self.account_config = account_config
        self.sku_mapper = sku_mapper
        self.account_slug = account_config.get('slug', 'unknown_shopify_account')

    def _map_sku(self, platform_sku_in):
        platform_sku_str = str(platform_sku_in).strip() if pd.notna(platform_sku_in) else ""
        if not platform_sku_str:
            return "", [None]
        msku_result = self.sku_mapper.map_sku_to_msku(platform_sku_str)
        return platform_sku_str, ([msku_result] if isinstance(msku_result, str) else msku_result) if msku_result is not None else [None]

    def _get_sku_column_name(self):
        return self.account_config.get('sku_column', 'Lineitem sku')

    def _get_quantity_column_names(self):
        qty_col = self.account_config.get('quantity_column', 'Lineitem quantity')
        return [qty_col]

    def parse(self, file_path, report_start_date_obj: datetime.date, report_end_date_obj: datetime.date):
        logger.info(f"SHOPIFY_PARSER: Starting Shopify parsing for file: {file_path}")
        sku_col = self._get_sku_column_name()
        unmapped_skus = []
        empty_df = pd.DataFrame()

        try:
            df = pd.read_csv(file_path, dtype={sku_col: str}, encoding='utf-8')
        except Exception as e:
            logger.error(f"SHOPIFY_PARSER: Error reading Shopify file {file_path}: {e}", exc_info=True)
            return empty_df, unmapped_skus

        if df.empty:
            logger.warning(f"SHOPIFY_PARSER: Shopify file {file_path} is empty.")
            return empty_df, unmapped_skus

        qty_cols = self._get_quantity_column_names()
        report_settings = self.account_config.get('report_settings', {})
        date_col = report_settings.get('date_column', 'Created at')
        revenue_col = report_settings.get('revenue_column', 'Total')

        required_cols = [sku_col, date_col, revenue_col] + qty_cols
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"SHOPIFY_PARSER: Missing required columns {missing_cols} in Shopify report. Available: {df.columns.tolist()}")
            return empty_df, unmapped_skus

        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df.dropna(subset=[date_col], inplace=True)

        df_filtered = df[
            (df[date_col].dt.date >= report_start_date_obj) &
            (df[date_col].dt.date <= report_end_date_obj)
        ].copy()

        if df_filtered.empty:
            logger.warning(f"SHOPIFY_PARSER: No data in report for the selected date range.")
            return empty_df, unmapped_skus

        temp_data_for_aggregation = []
        for index, row in df_filtered.iterrows():
            platform_sku_from_file = row[sku_col]
            original_platform_sku, msku_list = self._map_sku(platform_sku_from_file)

            if msku_list == [None]:
                unmapped_skus.append({
                    "Platform SKU": original_platform_sku,
                    "Platform": self.platform_name,
                    "Account": self.account_config.get('name'),
                    "Source File": os.path.basename(file_path)
                })
                continue

            sale_date_dt = row[date_col]
            quantity_sold = sum(clean_integer_value(row.get(qc, 0)) for qc in qty_cols)
            net_revenue = clean_numeric_value(row.get(revenue_col, 0))

            for msku_item in msku_list:
                temp_data_for_aggregation.append({
                    'Sale Date': sale_date_dt.date(),
                    'MSKU': msku_item,
                    'Platform': self.platform_name,
                    'Account Name': self.account_config.get('name', self.account_slug),
                    'Platform SKU': original_platform_sku,
                    'Order ID': None,
                    'Quantity Sold': quantity_sold,
                    'Net Revenue': net_revenue,
                    'Report Source File': os.path.basename(file_path)
                })

        if not temp_data_for_aggregation:
            logger.warning(f"SHOPIFY_PARSER: No records to aggregate after processing rows in {file_path}")
            return empty_df, unmapped_skus

        agg_df = pd.DataFrame(temp_data_for_aggregation)
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
        grouped_df['Discounts'] = 0.0
        grouped_df['Platform Fees'] = 0.0

        logger.info(f"SHOPIFY_PARSER: Successfully parsed and aggregated {len(grouped_df)} records from Shopify file.")
        logger.info(f"SHOPIFY_PARSER: Found {len(unmapped_skus)} unmapped SKUs in this file.")
        return grouped_df, unmapped_skus
