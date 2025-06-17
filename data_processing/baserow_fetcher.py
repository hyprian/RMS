# RMS/data_processing/baserow_fetcher.py
import requests
import pandas as pd
import logging
import json # Make sure json is imported

logger = logging.getLogger(__name__)

class BaserowFetcher:
    def __init__(self, api_token, base_url="https://api.baserow.io"):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Authorization": f"Token {api_token}",
            "Content-Type": "application/json",
        }
        if not api_token:
            logger.error("Baserow API token is not provided.")
            raise ValueError("Baserow API token is required.")

    def _get_all_rows(self, table_id):
        all_rows = []
        page = 1
        size = 100
        while True:
            url = f"{self.base_url}/api/database/rows/table/{table_id}/?user_field_names=true&page={page}&size={size}"
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                results = data.get("results", [])
                all_rows.extend(results)
                if data.get("next") is None or not results:
                    break
                page += 1
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data from Baserow table {table_id}, page {page}: {e}")
                raise
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON response from Baserow table {table_id}, page {page}: {e}. Response text: {response.text[:500]}")
                raise
        return all_rows

    def get_table_data_as_dataframe(self, table_id, required_columns=None, column_mapping=None):
        """
        Fetches all data from a Baserow table and returns it as a Pandas DataFrame.
        Optionally checks for required columns (original names before mapping)
        and applies a column mapping.
        """
        logger.info(f"Fetching data for Baserow table ID: {table_id}")
        try:
            rows = self._get_all_rows(table_id)
            if not rows:
                logger.warning(f"No data found in Baserow table {table_id}.")
                # If column_mapping is provided, use target names for empty df, else original required_columns
                final_cols_for_empty_df = list(column_mapping.values()) if column_mapping else required_columns
                return pd.DataFrame(columns=final_cols_for_empty_df) if final_cols_for_empty_df else pd.DataFrame()


            df = pd.DataFrame(rows)

            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, dict) and 'value' in x).any():
                    df[col] = df[col].apply(lambda x: x['value'] if isinstance(x, dict) and 'value' in x else x)
                elif df[col].apply(lambda x: isinstance(x, list) and x and isinstance(x[0], dict) and 'value' in x[0]).any():
                    df[col] = df[col].apply(lambda x: [item['value'] for item in x if isinstance(item, dict) and 'value' in item] if isinstance(x, list) else x)

            if required_columns:
                missing_cols = [col for col in required_columns if col not in df.columns]
                if missing_cols:
                    err_msg = f"Table {table_id} is missing required original columns: {missing_cols}. Available columns: {df.columns.tolist()}"
                    logger.error(err_msg)
                    raise ValueError(err_msg)
            
            # Apply column mapping if provided
            if column_mapping:
                # Check if all source columns in mapping exist in df
                source_cols_in_mapping = list(column_mapping.keys())
                missing_source_cols = [col for col in source_cols_in_mapping if col not in df.columns]
                if missing_source_cols:
                    logger.warning(f"Source columns for mapping not found in table {table_id}: {missing_source_cols}. These mappings will be skipped.")
                
                # Only select columns that are sources in the mapping AND exist in df
                cols_to_rename = {k: v for k, v in column_mapping.items() if k in df.columns}
                df = df[list(cols_to_rename.keys())].rename(columns=cols_to_rename)


            logger.info(f"Successfully fetched and processed {len(df)} rows from table {table_id}.")
            return df
        except Exception as e:
            logger.error(f"Failed to get data for table {table_id}: {e}")
            final_cols_for_empty_df = list(column_mapping.values()) if column_mapping else required_columns
            return pd.DataFrame(columns=final_cols_for_empty_df) if final_cols_for_empty_df else pd.DataFrame()


    def get_sku_mapping_data(self, table_id):
        expected_baserow_cols = ['sku', 'msku', 'Panel', 'Status']
        logger.info(f"Fetching SKU mapping data from table {table_id}. Expecting columns: {expected_baserow_cols}")
        
        # No column_mapping needed here if Baserow fields are already named 'sku', 'msku', etc.
        df = self.get_table_data_as_dataframe(table_id, required_columns=expected_baserow_cols)
        
        if df.empty:
            logger.warning(f"No SKU mapping data fetched or table {table_id} was empty.")
            return pd.DataFrame(columns=['sku', 'Panel', 'msku', 'Status'])

        if 'Status' in df.columns:
            df['Status'] = df['Status'].astype(str).str.strip().str.lower()
            original_row_count = len(df)
            df = df[df['Status'] == 'active']
            logger.info(f"Filtered SKU mapping data by Status='active'. Kept {len(df)} rows out of {original_row_count}.")
            if df.empty:
                logger.warning(f"No 'active' SKU mappings found in table {table_id} after filtering.")
                return pd.DataFrame(columns=['sku', 'Panel', 'msku', 'Status'])
        else:
            logger.error(f"'Status' column not found in the fetched data for table {table_id}. Cannot filter by active status.")
            return pd.DataFrame(columns=['sku', 'Panel', 'msku', 'Status'])

        final_columns = ['sku', 'Panel', 'msku', 'Status']
        # Ensure all final_columns exist before trying to select them
        # This can happen if required_columns check passed but then Status filter made df empty
        # or if get_table_data_as_dataframe returned an empty df with different columns.
        # However, the logic above should return an empty df with these columns if filtering results in empty.
        # For safety:
        actual_cols = [col for col in final_columns if col in df.columns]
        if not all(col in df.columns for col in final_columns):
             logger.warning(f"Not all expected final columns {final_columns} are in the dataframe after processing. Available: {df.columns.tolist()}")
             # Create missing columns with default values if necessary, or just select what's available
             for col in final_columns:
                 if col not in df.columns:
                     df[col] = '' # Or appropriate default
        df = df[final_columns]


        df['sku'] = df['sku'].astype(str).fillna('').str.strip().str.lower()
        df['msku'] = df['msku'].astype(str).fillna('').str.strip()
        df['Panel'] = df['Panel'].astype(str).fillna('').str.strip()
        df.dropna(subset=['sku', 'msku'], inplace=True)
        df = df[(df['sku'] != '') & (df['msku'] != '')]
        
        if df.empty:
            logger.warning(f"SKU mapping data for table {table_id} is empty after cleaning.")
        else:
            logger.info(f"Processed SKU mapping data for table {table_id}. Final active mappings: {len(df)}.")
            if df['sku'].duplicated().any():
                logger.warning(f"Duplicate 'sku' entries found in active SKU mappings for table {table_id}.")
        return df

    def get_combo_sku_data(self, table_id):
        logger.info(f"Fetching combo SKU data from table {table_id}.")
        # 'Combo' is essential. Other SKU component columns are discovered.
        df = self.get_table_data_as_dataframe(table_id, required_columns=['Combo'])
        if df.empty:
            logger.warning(f"No combo SKU data fetched or table {table_id} was empty.")
            return pd.DataFrame()

        df['Combo'] = df['Combo'].astype(str).fillna('').str.strip().str.lower()
        df.dropna(subset=['Combo'], inplace=True)
        df = df[df['Combo'] != '']

        if df.empty:
            logger.warning(f"Combo SKU data for table {table_id} is empty after cleaning 'Combo' column.")
            return df

        for col in df.columns:
            if col.lower().startswith('sku') and col.lower() not in ['sku', 'combo']:
                df[col] = df[col].astype(str).fillna('').str.strip()
        
        logger.info(f"Processed combo SKU data for table {table_id}. Found {len(df)} potential combo definitions.")
        return df

    def get_inventory_data(self, table_id):
        """
        Fetches inventory data from Baserow.
        Expects 'msku' and 'TLCQ' (Total Live Current Quantity) from Baserow.
        Renames them to 'MSKU' and 'Current Inventory'.
        Adds blank 'Images', 'Category', 'Code' columns.
        """
        # Baserow column names we expect for inventory
        baserow_inventory_cols = ['msku', 'TLCQ'] 
        # Target column names in our application
        target_col_mapping = {
            'msku': 'MSKU',
            'TLCQ': 'Current Inventory'
        }
        logger.info(f"Fetching inventory data from table {table_id}. Expecting Baserow columns: {baserow_inventory_cols}")

        df = self.get_table_data_as_dataframe(
            table_id, 
            required_columns=baserow_inventory_cols,
            column_mapping=target_col_mapping
        )
        
        if df.empty:
            logger.warning(f"No inventory data fetched or table {table_id} was empty after processing.")
            # Return empty DataFrame with the TARGET column names
            return pd.DataFrame(columns=['MSKU', 'Current Inventory', 'Images', 'Category', 'Code'])

        # Ensure 'MSKU' and 'Current Inventory' (which were mapped from msku, TLCQ) exist
        if 'MSKU' not in df.columns:
            logger.error(f"'MSKU' (mapped from 'msku') not found in processed inventory data for table {table_id}. Columns: {df.columns.tolist()}")
            return pd.DataFrame(columns=['MSKU', 'Current Inventory', 'Images', 'Category', 'Code'])
        if 'Current Inventory' not in df.columns:
            logger.error(f"'Current Inventory' (mapped from 'TLCQ') not found in processed inventory data for table {table_id}. Columns: {df.columns.tolist()}")
            # If MSKU exists but Current Inventory doesn't, we might still proceed but log a warning
            df['Current Inventory'] = 0 # Default to 0 if mapping failed or column was missing post-map
            logger.warning("Defaulted 'Current Inventory' to 0 due to missing mapped column.")


        # Clean 'MSKU' and convert 'Current Inventory'
        df['MSKU'] = df['MSKU'].astype(str).fillna('').str.strip()
        df['Current Inventory'] = pd.to_numeric(df['Current Inventory'], errors='coerce').fillna(0)

        # Add placeholder columns for Images, Category, Code
        df['Images'] = ''
        df['Category'] = ''
        df['Code'] = ''
        
        # Select and reorder to the final expected structure for the application
        final_inventory_cols = ['MSKU', 'Current Inventory', 'Images', 'Category', 'Code']
        df = df[final_inventory_cols]

        # Drop rows where MSKU is empty after cleaning
        df.dropna(subset=['MSKU'], inplace=True)
        df = df[df['MSKU'] != '']

        if df.empty:
            logger.warning(f"Inventory data for table {table_id} is empty after final cleaning.")
        else:
            logger.info(f"Processed inventory data for table {table_id}. Final records: {len(df)}.")
            if df['MSKU'].duplicated().any():
                logger.warning(f"Duplicate 'MSKU' entries found in inventory data for table {table_id}. Aggregating inventory for duplicates.")
                # If duplicates exist, sum their 'Current Inventory'. Other fields will take the first occurrence.
                df = df.groupby('MSKU', as_index=False).agg({
                    'Current Inventory': 'sum',
                    'Images': 'first', # Or some other logic if needed
                    'Category': 'first',
                    'Code': 'first'
                })
                logger.info(f"Aggregated duplicate MSKUs. Inventory records after aggregation: {len(df)}.")


        return df