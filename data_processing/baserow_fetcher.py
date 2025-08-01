# RMS/data_processing/baserow_fetcher.py
from datetime import datetime
import requests
import pandas as pd
import logging
import json # Make sure json is imported
import os
from utils.config_loader import APP_CONFIG

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
        Fetches inventory data from Baserow and aggregates stock from multiple warehouse columns.
        Warehouse columns are defined in settings.yaml under baserow.inventory_warehouse_columns.
        """
        logger.info(f"Fetching inventory data from table {table_id} for multiple warehouses.")

        # --- MODIFICATION: Get warehouse columns from config ---
        warehouse_columns = APP_CONFIG.get('baserow', {}).get('inventory_warehouse_columns', ['TLCQ']) # Default to TLCQ if not configured
        if not warehouse_columns:
            logger.error("`inventory_warehouse_columns` is not defined or empty in settings.yaml. Defaulting to ['TLCQ'].")
            warehouse_columns = ['TLCQ']
            
        # The only required column from Baserow is 'msku' plus at least one warehouse column.
        # We will fetch all warehouse columns plus 'msku'.
        baserow_inventory_cols = ['msku' , 'Product Name', 'Cost'] + warehouse_columns
        
        # We don't need a target_col_mapping here, as we'll be creating 'Current Inventory' ourselves.
        df = self.get_table_data_as_dataframe(
            table_id, 
            required_columns=baserow_inventory_cols
        )
        
        if df.empty:
            logger.warning(f"No inventory data fetched or table {table_id} was empty after processing.")
            return pd.DataFrame(columns=['MSKU', 'Product Name', 'Cost', 'Current Inventory']) # Return a minimal empty DataFrame

        # --- MODIFICATION: Aggregate stock from all warehouse columns ---
        # Ensure all warehouse columns exist, fill missing ones with 0
        for col in warehouse_columns:
            if col not in df.columns:
                logger.warning(f"Warehouse column '{col}' from settings.yaml not found in inventory table. Treating as 0.")
                df[col] = 0
            else:
                # Convert each warehouse column to numeric, coercing errors to NaN, then filling NaN with 0
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Sum across all warehouse columns to create the total 'Current Inventory'
        df['Current Inventory'] = df[warehouse_columns].sum(axis=1)
        
        # --- END MODIFICATION ---

        # Standardize MSKU column name
        df.rename(columns={'msku': 'MSKU'}, inplace=True)

        # Clean 'MSKU' and ensure 'Current Inventory' is an integer
        df['MSKU'] = df['MSKU'].astype(str).fillna('').str.strip()
        df['Product Name'] = df['Product Name'].astype(str).fillna('N/A').str.strip()
        df['Cost'] = pd.to_numeric(df['Cost'], errors='coerce').fillna(0)
        df['Current Inventory'] = df['Current Inventory'].astype(int)

        # Select the final columns we need for the app. We don't need the individual warehouse columns anymore.
        final_inventory_cols = ['MSKU','Product Name', 'Cost', 'Current Inventory']
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
                # If duplicates exist, sum their 'Current Inventory'.
                df = df.groupby('MSKU', as_index=False).agg({
                    'Current Inventory': 'sum',
                     'Cost': 'first', # Assuming cost is the same for duplicate MSKUs
                     'Product Name': 'first' # Assuming product name is the same for duplicate MSKUs
                })
                logger.info(f"Aggregated duplicate MSKUs. Inventory records after aggregation: {len(df)}.")

        return df
    
    def get_existing_sales_date_ranges(self, processed_sales_table_id):
        """
        Fetches MIN and MAX Sale Date for each Platform-Account combination.
        """
        logger.info(f"Fetching existing sales date ranges from table {processed_sales_table_id}")
        if not processed_sales_table_id:
            return {}

        try:
            all_sales_records_df = self.get_table_data_as_dataframe(processed_sales_table_id)

            all_sales_records_df['Sale Date'] = all_sales_records_df['Sale Date'].astype(str).str.strip()
            all_sales_records_df['parsed_date'] = pd.to_datetime(all_sales_records_df['Sale Date'], errors='coerce')

            if all_sales_records_df is None or all_sales_records_df.empty:
                return {}

            required_cols = {'Platform', 'Account Name', 'Sale Date'}
            if not required_cols.issubset(all_sales_records_df.columns):
                missing = required_cols - set(all_sales_records_df.columns)
                logger.error(f"Date Range Check: Missing required columns {missing}.")
                return {}

            # --- ROBUST DATE PARSING AND DEBUGGING ---
            logger.debug(f"Date Range Check: Raw 'Sale Date' dtypes:\n{all_sales_records_df[['Platform', 'Sale Date']].head(10)}")
            
            # Attempt to convert to datetime, coercing errors
            all_sales_records_df['parsed_date'] = pd.to_datetime(all_sales_records_df['Sale Date'], errors='coerce')
            
            # Find out which rows failed to parse
            failed_parsing = all_sales_records_df[all_sales_records_df['parsed_date'].isna()]
            if not failed_parsing.empty:
                logger.warning(f"Date Range Check: Failed to parse dates for {len(failed_parsing)} rows. Sample of failed rows:")
                logger.warning(f"\n{failed_parsing[['Platform', 'Account Name', 'Sale Date']].head().to_string()}")

            # Drop rows where date could not be parsed at all
            all_sales_records_df.dropna(subset=['parsed_date'], inplace=True)

            if all_sales_records_df.empty:
                logger.warning("Date Range Check: No valid date entries found after parsing all records.")
                return {}
            # --- END ROBUST DATE PARSING ---

            all_sales_records_df['Platform'] = all_sales_records_df['Platform'].fillna('Unknown Platform').astype(str)
            all_sales_records_df['Account Name'] = all_sales_records_df['Account Name'].fillna('Unknown Account').astype(str)

            # Use the newly parsed date column for aggregation
            date_ranges = all_sales_records_df.groupby(['Platform', 'Account Name'])['parsed_date'].agg(['min', 'max'])
            
            result = {}
            for index, row in date_ranges.iterrows():
                platform, account = index
                if platform not in result:
                    result[platform] = {}
                result[platform][account] = {
                    'min_date': row['min'].strftime('%Y-%m-%d') if pd.notna(row['min']) else None,
                    'max_date': row['max'].strftime('%Y-%m-%d') if pd.notna(row['max']) else None
                }
            logger.info(f"Fetched date ranges: {result}")
            return result
        except Exception as e:
            logger.error(f"Error fetching existing sales date ranges: {e}", exc_info=True)
            return {}
        
    def delete_sales_records_for_period(self, table_id, platform, account_name, msku, start_date_str, end_date_str):
        """
        Deletes sales records for a specific MSKU, platform, account within a date range.
        Baserow API for deleting multiple rows based on complex filters can be tricky.
        Often, you fetch IDs based on filters, then delete by ID.
        """
        logger.info(f"Attempting to delete records for {platform}-{account_name}, MSKU: {msku} from {start_date_str} to {end_date_str}")
        
        # Construct filter: by platform, account, msku, and date range
        # Baserow field names/IDs would be needed here. Assuming 'Platform', 'Account Name', 'MSKU', 'Sale Date' are field names.
        # Example filter string (syntax depends on Baserow API version and field types):
        # filter__field_Platform__equal=Amazon&filter__field_Account_Name__equal=Main&filter__field_MSKU__equal=XYZ
        # &filter__field_Sale_Date__date_after_or_equal=YYYY-MM-DD&filter__field_Sale_Date__date_before_or_equal=YYYY-MM-DD
        
        # This is a placeholder. Actual implementation requires knowing Baserow field IDs for filters.
        # For now, this function would need to:
        # 1. List rows matching the criteria (platform, account, msku, date range).
        # 2. Get their Baserow row IDs.
        # 3. Call the delete row API for each ID (or batch delete if available).
        
        # Simplified: For this example, we'll assume we'd query and delete.
        # This part is complex and needs careful implementation with Baserow's API.
        # For now, we'll just log it. A robust solution would involve fetching row IDs based on filters.
        
        # Fetch rows to delete
        # Example: /api/database/rows/table/{table_id}/?user_field_names=true&filter__Platform__equal={platform}&filter__Account_Name__equal={account_name}&filter__MSKU__equal={msku}&filter__Sale_Date__date_after_or_equal={start_date_str}&filter__Sale_Date__date_before_or_equal={end_date_str}
        # This requires field names to be exactly 'Platform', 'Account Name', etc. or using field_XXX IDs.
        
        # For now, let's assume a simpler strategy for the POC: if we are uploading daily aggregates,
        # we might delete records for specific MSKU/Platform/Account/SaleDate combinations.
        
        # To implement a true "delete for period" for an MSKU:
        # 1. Construct the filter URL.
        # 2. GET request to fetch matching row IDs.
        # 3. If rows found, POST to /api/database/rows/table/{table_id}/batch-delete/ with the list of row IDs.
        # This is non-trivial with the current generic fetcher.
        logger.warning("delete_sales_records_for_period is a placeholder and needs full Baserow API filter/delete implementation.")
        return True # Placeholder

    def batch_create_rows(self, table_id, records_list):
        """
        Creates rows in a Baserow table in batches.
        Baserow API typically supports creating multiple rows in one request.
        Max 200 items per request for batch row creation.
        """
        if not records_list:
            return True
        
        url = f"{self.base_url}/api/database/rows/table/{table_id}/batch/?user_field_names=true"
        all_results = []
        
        # Baserow batch create limit is often 200
        batch_size = 100 # Being conservative
        for i in range(0, len(records_list), batch_size):
            batch = records_list[i:i + batch_size]
            payload = {"items": batch} # Payload structure for batch create
            try:
                response = requests.post(url, headers=self.headers, json=payload)
                response.raise_for_status()
                all_results.extend(response.json().get("items", [])) # Response structure for batch create
                logger.info(f"Successfully created batch of {len(batch)} rows in table {table_id}.")
            except requests.exceptions.RequestException as e:
                logger.error(f"Error batch creating rows in table {table_id}: {e}")
                if response is not None: logger.error(f"Response content: {response.text}")
                return False # Indicate failure
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON response for batch create in table {table_id}: {e}. Response text: {response.text[:500]}")
                return False
        return True
    
    def check_existing_data_for_period(self, table_id, platform, account_name, start_date_str, end_date_str):
        """
        Checks if any data exists for the given platform, account, and date range.
        Returns True if data exists, False otherwise.
        This is a simplified check; it doesn't check for specific MSKUs yet.
        """
        logger.info(f"Checking for existing data: Table {table_id}, P: {platform}, A: {account_name}, {start_date_str}-{end_date_str}")
        if not all([table_id, platform, account_name, start_date_str, end_date_str]):
            logger.warning("Missing parameters for check_existing_data_for_period.")
            return False # Or raise error

        # Construct filter for Baserow API.
        # This requires knowing the actual field names/IDs in your Baserow table.
        # Assuming field names are 'Platform', 'Account Name', 'Sale Date'
        # And date format in Baserow is YYYY-MM-DD
        # Example: &filter__field_Platform__equal=Amazon&filter__field_Account_Name__equal=Main Account&filter__field_Sale_Date__date_after_or_equal=2023-01-01&filter__field_Sale_Date__date_before_or_equal=2023-01-31
        
        # For a robust solution, you'd get field IDs programmatically.
        # For now, let's assume you have these field names.
        # IMPORTANT: Replace 'field_Platform_ID', 'field_Account_Name_ID', 'field_Sale_Date_ID'
        # with the actual Baserow field IDs (e.g., field_12345) or ensure your field names are exactly as used.
        # If using user_field_names=true, you can use the display names.
        
        # Let's assume user_field_names=true is used in _get_all_rows, so we can use display names.
        # Ensure your Baserow table has fields named exactly "Platform", "Account Name", "Sale Date".
        filters = [
            f"filter__Platform__equal={requests.utils.quote(platform)}",
            f"filter__Account_Name__equal={requests.utils.quote(account_name)}",
            f"filter__Sale_Date__date_after_or_equal={start_date_str}", # Assumes YYYY-MM-DD
            f"filter__Sale_Date__date_before_or_equal={end_date_str}"  # Assumes YYYY-MM-DD
        ]
        filter_params = "&".join(filters)
        
        # We only need to know if at least one row exists, so limit to 1.
        url = f"{self.base_url}/api/database/rows/table/{table_id}/?user_field_names=true&size=1&{filter_params}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            if data.get("count", 0) > 0:
                logger.info("Existing data found for the specified period.")
                return True
            logger.info("No existing data found for the specified period.")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Error checking existing data: {e}", exc_info=True)
            if response is not None: logger.error(f"Response content: {response.text}")
            return False # Assume no data on error, or handle differently
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON for existing data check: {e}. Response: {response.text[:500]}")
            return False
        
        
    def get_row_ids_for_criteria(self, table_id, criteria_list_of_dicts):
        """
        Fetches Baserow row IDs for records matching a list of specific criteria.
        Each dict in criteria_list_of_dicts defines a unique record to find.
        Example criteria_dict: {'Sale Date': 'YYYY-MM-DD', 'MSKU': 'msku1', 'Platform': 'Amazon', 'Account Name': 'Main'}
        
        This is complex because Baserow API filter for "OR" conditions across multiple full criteria sets is not direct.
        A more robust way is to fetch all data for a broader range and filter in pandas, or do multiple API calls.
        
        For now, this will be a simplified version that assumes you might query for each criterion one by one,
        which is inefficient but illustrates the concept. A better way is to fetch based on a common denominator
        (like date range, platform, account) and then filter locally.

        Let's refine this: Fetch rows matching a broader filter (platform, account, date range of the new upload)
        and then locally identify which of those match the specific (Sale Date, MSKU) from the new upload.
        """
        row_ids_to_delete = []
        if not criteria_list_of_dicts:
            return row_ids_to_delete

        # Extract common filters: platform, account, min/max sale dates from the criteria
        platforms = list(set(c['Platform'] for c in criteria_list_of_dicts))
        accounts = list(set(c['Account Name'] for c in criteria_list_of_dicts))
        sale_dates = [datetime.strptime(c['Sale Date'], '%Y-%m-%d').date() for c in criteria_list_of_dicts]
        
        if not platforms or not accounts or not sale_dates:
            logger.warning("Insufficient common criteria to fetch rows for deletion.")
            return []

        # For simplicity, assume one platform/account per batch of criteria.
        # If multiple, this logic needs to be looped or made more generic.
        platform = platforms[0]
        account_name = accounts[0]
        min_date_str = min(sale_dates).strftime('%Y-%m-%d')
        max_date_str = max(sale_dates).strftime('%Y-%m-%d')

        logger.info(f"Fetching existing rows for P:{platform}, A:{account_name}, Dates:{min_date_str}-{max_date_str} to identify deletable IDs.")

        filters = [
            f"filter__Platform__equal={requests.utils.quote(platform)}",
            f"filter__Account_Name__equal={requests.utils.quote(account_name)}",
            f"filter__Sale_Date__date_after_or_equal={min_date_str}",
            f"filter__Sale_Date__date_before_or_equal={max_date_str}"
        ]
        filter_params = "&".join(filters)
        
        # Fetch all potentially relevant rows (this could be large)
        # The _get_all_rows helper is used here.
        url = f"{self.base_url}/api/database/rows/table/{table_id}/?user_field_names=true&{filter_params}&size=200" # Max size per page
        
        all_fetched_rows = []
        page = 1
        while True:
            paginated_url = f"{url}&page={page}"
            try:
                response = requests.get(paginated_url, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                results = data.get("results", [])
                all_fetched_rows.extend(results)
                if data.get("next") is None or not results: break
                page += 1
            except Exception as e:
                logger.error(f"Error fetching rows for ID identification: {e}", exc_info=True)
                return [] # Critical error, stop

        if not all_fetched_rows:
            logger.info("No existing rows found matching the broader criteria.")
            return []

        # Now, locally filter these fetched_rows against the specific criteria_list_of_dicts
        # This assumes 'Sale Date' from Baserow is string 'YYYY-MM-DD' or needs parsing.
        # And 'MSKU' from Baserow matches.
        for existing_row in all_fetched_rows:
            # Ensure Baserow 'Sale Date' is comparable (string YYYY-MM-DD)
            # Baserow date fields often come back as YYYY-MM-DD if user_field_names=true
            existing_sale_date_str = existing_row.get('Sale Date')
            existing_msku = existing_row.get('MSKU')
            
            # Handle if MSKU is None/NaN in Baserow (represented as None or empty string)
            if existing_msku is None: existing_msku = "" # Or some other placeholder if needed for comparison

            for new_record_criteria in criteria_list_of_dicts:
                new_msku = new_record_criteria.get('MSKU')
                if new_msku is None: new_msku = ""

                if (existing_sale_date_str == new_record_criteria.get('Sale Date') and
                    existing_msku == new_msku and # Compare potentially None MSKUs carefully
                    existing_row.get('Platform') == new_record_criteria.get('Platform') and
                    existing_row.get('Account Name') == new_record_criteria.get('Account Name')):
                    row_ids_to_delete.append(existing_row['id'])
                    break # Found a match for this existing_row, move to next existing_row
        
        unique_ids_to_delete = list(set(row_ids_to_delete)) # Ensure uniqueness
        logger.info(f"Identified {len(unique_ids_to_delete)} existing row IDs to delete.")
        return unique_ids_to_delete
    
    def delete_single_row(self, table_id, row_id):
        """Deletes a single row from a table given its Baserow row ID."""
        url = f"{self.base_url}/api/database/rows/table/{table_id}/{row_id}/"
        response = None
        try:
            logger.info(f"Table {table_id}: Attempting to delete single row ID: {row_id} via URL: {url}")
            response = requests.delete(url, headers=self.headers)
            
            if not response.ok and response.status_code != 404: # Log error unless it's a 404
                 logger.error(f"Table {table_id}: Single row delete API call failed for ID {row_id}. Status: {response.status_code}, Response: {response.text}")
            
            response.raise_for_status() # Will raise HTTPError for 4xx/5xx client/server errors (except we handle 404 below)
            
            logger.info(f"Table {table_id}: Successfully deleted row ID: {row_id}. Status: {response.status_code}")
            return True
        except requests.exceptions.HTTPError as http_err:
            if response is not None and response.status_code == 404:
                logger.warning(f"Table {table_id}: Row ID {row_id} not found for deletion (HTTP 404). It might have been already deleted or never existed.")
                return True # Treat as "success" in the context of trying to remove it
            logger.error(f"Table {table_id}: HTTPError deleting row ID {row_id}. Error: {http_err}", exc_info=False)
            if response is not None: logger.error(f"Response content: {response.text}") # Already logged if not response.ok
            return False
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Table {table_id}: RequestException deleting row ID {row_id}. Error: {req_err}", exc_info=True)
            return False

    def delete_rows_by_ids_one_by_one(self, table_id, row_ids_to_delete):
        """
        Deletes rows one by one. Slower than batch but a workaround if batch delete fails.
        """
        if not row_ids_to_delete:
            logger.info(f"Table {table_id}: No row IDs provided for one-by-one deletion.")
            return True # Or False if you consider it an issue that it was called with no IDs

        logger.info(f"Table {table_id}: Starting one-by-one deletion for {len(row_ids_to_delete)} row IDs.")
        overall_success = True
        successfully_deleted_count = 0
        failed_to_delete_ids = []

        for rid_str in row_ids_to_delete:
            try:
                row_id = int(rid_str)
                if self.delete_single_row(table_id, row_id):
                    successfully_deleted_count +=1
                else:
                    overall_success = False
                    failed_to_delete_ids.append(row_id)
                    # Continue trying to delete other rows
            except (ValueError, TypeError):
                logger.warning(f"Table {table_id}: Invalid non-integer row ID '{rid_str}' skipped for single deletion.")
                overall_success = False # Count this as a failure in the overall operation
                failed_to_delete_ids.append(rid_str) # Add the problematic ID
        
        logger.info(f"Table {table_id}: Finished one-by-one deletion. Successfully deleted: {successfully_deleted_count}. Failed/Skipped: {len(failed_to_delete_ids)}.")
        if failed_to_delete_ids:
            logger.warning(f"Table {table_id}: IDs that failed or were skipped: {failed_to_delete_ids}")
            
        return overall_success # Returns True if all attempts were successful (or row not found), False if any actual deletion error occurred for an existing row or if any ID was invalid.

    def get_row_ids_for_range_deletion(self, table_id, start_date_str, end_date_str, platform=None, account_name=None):
        """
        Fetches all row IDs for a given date range, with optional platform and account filters.
        If platform or account_name is None, it means "all" for that dimension.
        WARNING: Use with caution, as this can fetch many IDs.
        """
        logger.warning(f"Fetching ALL row IDs for range deletion: T:{table_id}, P:{platform}, A:{account_name}, {start_date_str}-{end_date_str}")
        
        # Build the filter string for the API call
        filters = []
        if platform:
            filters.append(f"filter__Platform__equal={requests.utils.quote(platform)}")
        if account_name:
            filters.append(f"filter__Account_Name__equal={requests.utils.quote(account_name)}")
        if start_date_str:
            filters.append(f"filter__Sale_Date__date_after_or_equal={start_date_str}")
        if end_date_str:
            filters.append(f"filter__Sale_Date__date_before_or_equal={end_date_str}")
        
        filter_params = "&".join(filters)
        
        all_matching_rows = []
        page = 1
        # We only need the 'id' field, but fetching all is simpler with get_table_data_as_dataframe
        # Let's construct a paginated fetch manually to be more efficient.
        base_url_for_list = f"{self.base_url}/api/database/rows/table/{table_id}/?user_field_names=true&size=200"
        if filter_params:
            base_url_for_list += f"&{filter_params}"

        logger.debug(f"Fetching rows for range deletion with URL (page 1): {base_url_for_list}&page=1")

        while True:
            current_url = f"{base_url_for_list}&page={page}"
            try:
                response = requests.get(current_url, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                results = data.get("results", [])
                if not results:
                    break
                all_matching_rows.extend(results)
                if data.get("next") is None:
                    break
                page += 1
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching rows for range deletion: {e}", exc_info=True)
                if response is not None: logger.error(f"Response: {response.text}")
                return [] # Return empty on error
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error fetching rows for range deletion: {e}. Response: {response.text[:500]}")
                return []

        row_ids = [row['id'] for row in all_matching_rows if 'id' in row]
        logger.info(f"Found {len(row_ids)} row IDs for range deletion criteria.")
        return row_ids
    
    def batch_delete_rows(self, table_id, row_ids_to_delete):
        """
        Deletes rows in batches of 200 to comply with Baserow API limits.
        This is the fast and preferred method.
        """
        if not row_ids_to_delete:
            logger.info(f"Table {table_id}: No row IDs provided for batch deletion.")
            return True

        valid_row_ids = []
        for rid in row_ids_to_delete:
            try:
                valid_row_ids.append(int(rid))
            except (ValueError, TypeError):
                logger.warning(f"Table {table_id}: Invalid non-integer row ID '{rid}' skipped for batch deletion.")
        
        if not valid_row_ids:
            logger.info(f"Table {table_id}: No valid integer row IDs for batch deletion.")
            return True

        logger.info(f"Table {table_id}: Starting FAST BATCH DELETE for a total of {len(valid_row_ids)} rows.")
        
        url = f"{self.base_url}/api/database/rows/table/{table_id}/batch-delete/"
        
        # --- CHUNKING LOGIC ---
        batch_size = 200 # Baserow's limit
        overall_success = True

        for i in range(0, len(valid_row_ids), batch_size):
            chunk_of_ids = valid_row_ids[i:i + batch_size]
            
            logger.info(f"Table {table_id}: Deleting chunk {i//batch_size + 1}, containing {len(chunk_of_ids)} row IDs.")
            
            payload = {"items": chunk_of_ids}
            
            response = None
            try:
                logger.debug(f"Table {table_id}: Sending POST to {url} with payload: {json.dumps(payload)}")
                response = requests.post(url, headers=self.headers, json=payload)
                
                if not response.ok:
                    logger.error(f"Table {table_id}: Batch delete API call for chunk failed. Status: {response.status_code}, Response: {response.text}")
                
                response.raise_for_status()
                
                logger.info(f"Table {table_id}: Successfully submitted batch delete request for chunk. Status: {response.status_code}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Table {table_id}: FAST BATCH DELETE FAILED for a chunk. Error: {e}", exc_info=False)
                if response is not None:
                    logger.error(f"Table {table_id}: Failing response content: {response.text}")
                overall_success = False # Mark the entire operation as failed
                # You could choose to stop here or continue with other chunks
                # For now, let's stop on the first failed chunk.
                break 
        
        if not overall_success:
            logger.error(f"Table {table_id}: One or more chunks failed to delete. The operation was aborted.")
        else:
            logger.info(f"Table {table_id}: All chunks processed successfully for deletion.")

        return overall_success
    
    def get_category_data(self, table_id):
        """
        Fetches product category, cost, supplier, and strategic data from Baserow.
        """
        logger.info(f"Fetching category & strategic data from table {table_id}")
        
        # --- MODIFICATION: Add all new required columns ---
        required_cols = [
            'Msku', 'Category', 'HSN Code', 'Cost Inc.GST',
            'Vendor', 'Product Type', 'Vendor Lead Time' # Your sample shows 'Vendor Lead time'
        ]
        
        df = self.get_table_data_as_dataframe(table_id, required_columns=required_cols)
        
        if df.empty:
            logger.warning(f"No category data fetched or table {table_id} was empty.")
            return pd.DataFrame()

        # Standardize the 'Msku' column name to 'MSKU'
        df.rename(columns={'Msku': 'MSKU', 'Vendor': 'Supplier', 'Vendor Lead Time': 'Vendor Lead Time (days)'}, inplace=True)
        df['MSKU'] = df['MSKU'].astype(str).str.strip()
        df.dropna(subset=['MSKU'], inplace=True)
        df = df[df['MSKU'] != '']

        # Handle duplicates
        if df['MSKU'].duplicated().any():
            logger.warning(f"Duplicate MSKUs found in category table {table_id}. Keeping first entry.")
            df.drop_duplicates(subset=['MSKU'], keep='first', inplace=True)
            
        logger.info(f"Successfully processed {len(df)} unique MSKU category/strategic records.")
        return df

    def get_catalogue_data(self, table_id):
        """
        Fetches product catalogue data, specifically MSKU and the first product image URL.
        """
        logger.info(f"Fetching catalogue data from table {table_id}")
        
        # We need 'msku' and the first image column.
        # Your sample shows the column name is 'Product Image 1'.
        required_cols = ['msku', 'Product Image 1']
        
        df = self.get_table_data_as_dataframe(table_id, required_columns=required_cols)
        
        if df.empty:
            logger.warning(f"No catalogue data fetched or table {table_id} was empty.")
            return pd.DataFrame(columns=['MSKU', 'Image URL'])

        # Standardize column names for consistency
        df.rename(columns={'msku': 'MSKU', 'Product Image 1': 'Image URL'}, inplace=True)
        
        # Clean the data
        df['MSKU'] = df['MSKU'].astype(str).str.strip()
        df.dropna(subset=['MSKU', 'Image URL'], inplace=True) # Drop rows where either MSKU or the image URL is missing
        df = df[df['MSKU'] != '']

        # Select only the columns we need
        final_df = df[['MSKU', 'Image URL']].copy()
        
        # Handle duplicate MSKUs - take the first one found
        if final_df['MSKU'].duplicated().any():
            logger.warning(f"Duplicate MSKUs found in catalogue table {table_id}. Keeping the first image entry for each.")
            final_df.drop_duplicates(subset=['MSKU'], keep='first', inplace=True)
            
        logger.info(f"Successfully processed {len(final_df)} unique MSKU image records.")
        return final_df
    
# RMS/data_processing/baserow_fetcher.py

    def get_outbound_packaging_data(self, table_id):
        """
        Fetches the raw outbound data containing the packaging material used for each shipment.
        Handles mixed date formats and avoids creating duplicate column names.
        """
        logger.info(f"--- Starting get_outbound_packaging_data for table {table_id} ---")
        
        required_cols = ['Date', 'Packing material']
        
        df = self.get_table_data_as_dataframe(table_id, required_columns=required_cols)
        
        if df.empty:
            logger.error(f"FETCH_FAIL: get_table_data_as_dataframe returned an empty DataFrame for table {table_id}.")
            return pd.DataFrame()
        
        logger.debug(f"Outbound data fetched. Columns: {df.columns.tolist()}. Head:\n{df.head().to_string()}")

        # --- THIS IS THE FIX ---
        # 1. Parse the original 'Date' column. Let Pandas infer the format.
        parsed_dates = pd.to_datetime(df['Date'], errors='coerce').dt.date
        
        # 2. Create the final DataFrame by selecting and renaming columns in one step.
        #    This avoids creating intermediate columns with conflicting names.
        final_df = pd.DataFrame({
            'Date': parsed_dates,
            'Material Name': df['Packing material'].astype(str) # Select and rename in one go
        })
        # --- END FIX ---
        
        # 3. Now, perform cleaning on the new, clean DataFrame.
        # Handle cases where 'Material Name' might be None/NaN, fill with empty string
        final_df['Material Name'] = final_df['Material Name'].fillna('')
        
        # Drop rows where the date could not be parsed
        df_before_drop = len(final_df)
        final_df.dropna(subset=['Date'], inplace=True)
        num_null_dates = df_before_drop - len(final_df)
        
        if num_null_dates > 0:
            logger.warning(f"DATE_PARSE_WARN: Dropped {num_null_dates} out of {df_before_drop} rows due to invalid dates.")
        
        if final_df.empty:
            logger.warning("DataFrame became empty after dropping rows with invalid dates.")
            return pd.DataFrame()
        
        logger.info(f"Successfully processed {len(final_df)} outbound records.")
        return final_df
    
    def get_packaging_inventory(self, table_id):
        """
        Fetches the current inventory for packaging materials.
        """
        logger.info(f"Fetching packaging material inventory from table {table_id}")
        
        # Required columns from the 'Packing Material Inventory' table
        required_cols = ['material', 'Current Inventory']
        
        df = self.get_table_data_as_dataframe(table_id, required_columns=required_cols)
        
        if df.empty:
            logger.warning(f"No packaging inventory data fetched from table {table_id}.")
            return pd.DataFrame(columns=['Material Name', 'Current Inventory'])

        # Standardize column names and clean data
        df.rename(columns={'material': 'Material Name'}, inplace=True)
        df['Current Inventory'] = pd.to_numeric(df['Current Inventory'], errors='coerce').fillna(0).astype(int)
        df['Material Name'] = df['Material Name'].astype(str).str.strip()
        df.dropna(subset=['Material Name'], inplace=True)
        df = df[df['Material Name'] != '']

        # Handle duplicates - sum inventory for any materials listed more than once
        if df['Material Name'].duplicated().any():
            logger.warning(f"Duplicate materials found in packaging inventory table {table_id}. Summing their inventory.")
            df = df.groupby('Material Name', as_index=False)['Current Inventory'].sum()

        logger.info(f"Successfully processed {len(df)} unique packaging material inventory records.")
        return df