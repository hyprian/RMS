# RMS/data_ingestion/meesho_parser.py
import pandas as pd
from datetime import datetime
from .utils import clean_numeric_value, clean_integer_value
import os
# from .base_parser import BaseSalesParser

# class MeeshoSalesParser(BaseSalesParser):
class MeeshoSalesParser:
    def __init__(self, platform_name, account_config, sku_mapper):
        # super().__init__(platform_name, account_config, sku_mapper)
        self.platform_name = platform_name
        self.account_config = account_config
        self.sku_mapper = sku_mapper
        self.account_slug = account_config.get('slug', 'unknown_meesho_account')

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
        return self.account_config.get('sku_column', 'SKU') # Default for Meesho

    def _get_quantity_column_names(self): # Duplicated from Base for standalone
        qty_col = self.account_config.get('quantity_column', 'Quantity')
        return [qty_col]

    def parse(self, file_path, report_start_date, report_end_date) -> pd.DataFrame:
        try:
            df = pd.read_csv(file_path, dtype={self._get_sku_column_name(): str})
        except Exception as e:
            print(f"Error reading Meesho file {file_path}: {e}")
            return pd.DataFrame()

        sku_col = self._get_sku_column_name()
        qty_cols = self._get_quantity_column_names() # Should be a list with one item
        
        # Column names from Meesho sample
        status_col = self.account_config.get('report_settings', {}).get('status_column', "Reason for Credit Entry")
        valid_statuses = self.account_config.get('report_settings', {}).get('valid_sale_statuses', ["SHIPPED"]) # Case-insensitive later
        date_col = self.account_config.get('report_settings', {}).get('date_column', "Order Date")
        revenue_col = self.account_config.get('report_settings', {}).get('revenue_column', "Supplier Discounted Price (Incl GST and Commision)")
        order_id_col = "Sub Order No"

        required_cols_for_processing = [sku_col, date_col, status_col, revenue_col] + qty_cols
        if order_id_col not in df.columns: required_cols_for_processing.append(order_id_col)

        for col in required_cols_for_processing:
            if col not in df.columns:
                print(f"Missing required column '{col}' in Meesho report {file_path}")
                return pd.DataFrame()

        # Filter by status
        df = df[df[status_col].astype(str).str.upper().isin([s.upper() for s in valid_statuses])]
        if df.empty:
            print(f"No valid sales entries after status filter in Meesho report {file_path}")
            return pd.DataFrame()

        # Filter by report_start_date and report_end_date
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df.dropna(subset=[date_col], inplace=True)
        df_filtered = df[(df[date_col].dt.date >= report_start_date) & (df[date_col].dt.date <= report_end_date)]

        temp_aggregated_data = []
        for index, row in df_filtered.iterrows():
            platform_sku_original = row[sku_col]
            original_sku_for_mapping, msku_list = self._map_sku(platform_sku_original)
            
            sale_date = row[date_col].strftime('%Y-%m-%d')
            quantity_sold = sum(clean_integer_value(row.get(qc, 0)) for qc in qty_cols)
            net_revenue = clean_numeric_value(row.get(revenue_col, 0))
            order_id = row.get(order_id_col)

            for msku in msku_list:
                temp_aggregated_data.append({
                    'Sale Date': sale_date,
                    'MSKU': msku,
                    'Platform': self.platform_name,
                    'Account Name': self.account_config.get('name', self.account_slug),
                    'Platform SKU': original_sku_for_mapping,
                    'Order ID': order_id,
                    'Quantity Sold': quantity_sold,
                    'Net Revenue': net_revenue, # Assuming this is net
                    'Report Source File': os.path.basename(file_path)
                })
        
        if not temp_aggregated_data:
            return pd.DataFrame()

        final_df = pd.DataFrame(temp_aggregated_data)
        if final_df.empty: return pd.DataFrame()
        
        final_df['MSKU'] = final_df['MSKU'].astype(str).fillna('UNMAPPED')
        agg_functions = {
            'Quantity Sold': 'sum',
            'Net Revenue': 'sum',
            'Platform SKU': 'first', 
            'Order ID': 'first', # Or lambda x: ', '.join(x.unique()) if multiple orders for same MSKU/day
            'Report Source File': 'first'
        }
        grouped_df = final_df.groupby(['Sale Date', 'MSKU', 'Platform', 'Account Name'], as_index=False).agg(agg_functions)
        
        grouped_df.rename(columns={'Platform SKU': 'Platform SKU Sample', 'Order ID': 'Order ID Sample'}, inplace=True)
        grouped_df['MSKU'] = grouped_df['MSKU'].replace('UNMAPPED', None)

        grouped_df['Gross Revenue'] = grouped_df['Net Revenue'] # Assuming Net = Gross for Meesho from this report
        grouped_df['Discounts'] = 0
        grouped_df['Platform Fees'] = 0
        
        return grouped_df