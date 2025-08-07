# RMS/data_processing/sku_mapper.py
import pandas as pd
import os
import logging
import unicodedata
from datetime import datetime

# Assuming cache_manager and BaserowFetcher are in accessible paths
from utils.cache_manager import load_from_cache, save_to_cache
# BaserowFetcher is initialized outside and passed in

logger = logging.getLogger(__name__)

class SKUMapper:
    def __init__(self, baserow_fetcher, sku_mapping_table_id, combo_sku_table_id,
                 amazon_listing_table_id, cache_config, project_root_dir, force_refresh_cache=False):
        """
        Initialize the SKUMapper.

        Args:
            baserow_fetcher (BaserowFetcher): Instance of BaserowFetcher.
            sku_mapping_table_id (str): ID of the Baserow SKU mapping table.
            combo_sku_table_id (str): ID of the Baserow combo SKU table.
            cache_config (dict): Dictionary with 'directory' and 'expiry_days'.
            project_root_dir (str): Absolute path to the project root directory.
            force_refresh_cache (bool): If True, ignore existing cache.
        """
        self.fetcher = baserow_fetcher
        self.amazon_listing_table_id = amazon_listing_table_id
        self.sku_mapping_table_id = sku_mapping_table_id
        self.combo_sku_table_id = combo_sku_table_id
        
        self.cache_dir_name = cache_config.get('directory', '.rms_cache')
        # Ensure cache_dir is an absolute path or relative to a known root
        if os.path.isabs(self.cache_dir_name):
            self.cache_dir = self.cache_dir_name
        else:
            self.cache_dir = os.path.join(project_root_dir, self.cache_dir_name)
        
        self.cache_expiry_days = cache_config.get('expiry_days', 5)
        self.force_refresh_cache = force_refresh_cache

        self.mapping_df = self._load_data_with_cache(
            cache_name="sku_mapping_data",
            fetch_function=lambda: self.fetcher.get_sku_mapping_data(self.sku_mapping_table_id)
        )
        self.combo_df = self._load_data_with_cache(
            cache_name="combo_sku_data",
            fetch_function=lambda: self.fetcher.get_combo_sku_data(self.combo_sku_table_id)
        )

        self.asin_df = self._load_data_with_cache(
            cache_name="asin_mapping_data",
            fetch_function=lambda: self.fetcher.get_asin_mapping_data(self.amazon_listing_table_id)
        )
        
        # Pre-process and build dictionaries for faster lookups if dfs are large
        if not self.mapping_df.empty:
            self._sku_to_msku_dict = pd.Series(self.mapping_df.msku.values, index=self.mapping_df.sku).to_dict()
        else:
            self._sku_to_msku_dict = {}

        if not self.combo_df.empty and 'Combo' in self.combo_df.columns:
            self._combo_to_mskus_dict = {}
            # Ensure 'Combo' column exists before trying to iterate
            for _, row in self.combo_df.iterrows():
                combo_sku_val = row['Combo'] # Already lowercased during fetch
                if pd.isna(combo_sku_val) or combo_sku_val == '':
                    continue
                
                # Component MSKUs are in columns like SKU1, SKU2, etc.
                # These columns should contain the MSKUs of the child products.
                component_mskus = []
                for col_name in self.combo_df.columns:
                    if col_name.upper().startswith('SKU') and col_name.upper() != 'SKU': # e.g., SKU1, SKU2
                        msku_val = row[col_name]
                        if pd.notna(msku_val) and str(msku_val).strip() != '':
                            component_mskus.append(str(msku_val).strip())
                
                if component_mskus: # Only add if there are actual component MSKUs
                    self._combo_to_mskus_dict[combo_sku_val] = component_mskus
        else:
            self._combo_to_mskus_dict = {}
        
        logger.info(f"SKUMapper initialized. SKU mappings: {len(self._sku_to_msku_dict)}, Combo mappings: {len(self._combo_to_mskus_dict)}")


    def _load_data_with_cache(self, cache_name, fetch_function):
        """Generic function to load data, using cache if available and not stale."""
        cached_df = None
        if not self.force_refresh_cache:
            cached_df = load_from_cache(cache_name, self.cache_dir, self.cache_expiry_days)
            if cached_df is not None:
                return cached_df
        
        logger.info(f"Fetching fresh data for '{cache_name}' (force_refresh={self.force_refresh_cache}).")
        try:
            df = fetch_function()
            if df is None or df.empty: # fetch_function might return None or empty df on error/no data
                # Try to load from cache as a fallback, even if stale, if fetching fails
                logger.warning(f"Fetching fresh data for '{cache_name}' resulted in empty or None. Attempting to use cache as fallback.")
                cached_df_fallback = load_from_cache(cache_name, self.cache_dir, self.cache_expiry_days)
                if cached_df_fallback is not None:
                    logger.warning(f"Using cache (possibly stale) for '{cache_name}' due to fetch issue.")
                    return cached_df_fallback
                else: # No fresh data and no cache
                    logger.error(f"No data found for '{cache_name}' from source and no cache available.")
                    return pd.DataFrame() # Return an empty DataFrame
            
            save_to_cache(df, cache_name, self.cache_dir)
            return df
        except Exception as e:
            logger.error(f"Error loading data for '{cache_name}': {e}")
            # Attempt to load from cache as a fallback if fetching fails, even if stale
            cached_df_fallback = load_from_cache(cache_name, self.cache_dir, self.cache_expiry_days)
            if cached_df_fallback is not None:
                logger.warning(f"Using cache (possibly stale) for '{cache_name}' due to critical fetch error.")
                return cached_df_fallback
            logger.error(f"Critical error loading '{cache_name}' and no cache available. Returning empty DataFrame.")
            return pd.DataFrame() # Return an empty DataFrame on critical error

    def map_sku_to_msku(self, platform_sku):
        """
        Map a single platform SKU to its corresponding MSKU or list of MSKUs for combos.

        Args:
            platform_sku (str): The SKU from the platform report.

        Returns:
            str or list or None: The MSKU (str) for single SKUs, 
                                 list of MSKUs for combos, or None if not found.
        """
        if not platform_sku or pd.isna(platform_sku):
            logger.debug("Received empty or NaN platform_sku for mapping.")
            return None

        try:
            # Normalize SKU: NFKD for compatibility, remove control chars, lowercase, strip whitespace
            normalized_sku = unicodedata.normalize('NFKD', str(platform_sku).strip()).encode('ascii', 'ignore').decode('utf-8').lower()
            if not normalized_sku:
                logger.warning(f"Platform SKU '{platform_sku}' became empty after normalization.")
                return None
            
            logger.debug(f"Attempting to map normalized SKU: '{normalized_sku}' (original: '{platform_sku}')")

            # 1. Check if it's a combo SKU
            if normalized_sku in self._combo_to_mskus_dict:
                component_mskus = self._combo_to_mskus_dict[normalized_sku]
                if component_mskus: # Ensure the list is not empty
                    logger.debug(f"SKU '{normalized_sku}' is a combo, maps to MSKUs: {component_mskus}")
                    return component_mskus
                else:
                    logger.warning(f"Combo SKU '{normalized_sku}' found but has no component MSKUs listed.")
                    # Fall through to check standard mapping, or return None based on desired logic
                    # For now, let's assume if it's in combo dict, it's handled, even if empty list.
                    # Or, perhaps it should then be treated as unmapped.
                    # Let's return None if component_mskus is empty.
                    return None


            # 2. Check if it's a standard SKU
            if normalized_sku in self._sku_to_msku_dict:
                msku = self._sku_to_msku_dict[normalized_sku]
                if msku and str(msku).strip() != '': # Ensure MSKU is not empty
                    logger.debug(f"SKU '{normalized_sku}' maps to MSKU: '{msku}'")
                    return str(msku).strip()
                else:
                    logger.warning(f"SKU '{normalized_sku}' found in mapping but has an empty MSKU.")
                    return None
            
            logger.warning(f"No MSKU mapping found for SKU: '{normalized_sku}' (original: '{platform_sku}')")
            return None
        except Exception as e:
            logger.error(f"Error mapping SKU '{platform_sku}': {e}", exc_info=True)
            return None

    # --- Functions for later stages (processing reports, getting inventory) ---
    def map_sales_report_skus(self, sales_report_df, platform_sku_column):
        """
        Adds an 'msku' column to the sales report DataFrame by mapping platform SKUs.
        For combo SKUs, it might duplicate rows or handle as per business logic.
        This is a simplified version; your WMS version was more complex.

        Args:
            sales_report_df (pd.DataFrame): The sales report.
            platform_sku_column (str): Name of the column containing platform SKUs.

        Returns:
            pd.DataFrame: Report DataFrame with an added 'msku_mapped' column (can be str or list).
        """
        if platform_sku_column not in sales_report_df.columns:
            logger.error(f"SKU column '{platform_sku_column}' not found in sales report.")
            sales_report_df['msku_mapped'] = None
            return sales_report_df

        logger.info(f"Mapping SKUs for sales report using column '{platform_sku_column}'.")
        sales_report_df['msku_mapped'] = sales_report_df[platform_sku_column].apply(self.map_sku_to_msku)
        
        unmapped_count = sales_report_df['msku_mapped'].isnull().sum()
        if unmapped_count > 0:
            logger.warning(f"{unmapped_count} SKUs in the report could not be mapped to an MSKU.")
            # For debugging, you might want to log some unmapped SKUs
            # unmapped_sample = sales_report_df[sales_report_df['msku_mapped'].isnull()][platform_sku_column].unique()[:5]
            # logger.debug(f"Sample unmapped SKUs: {list(unmapped_sample)}")
            
        return sales_report_df
    
    def get_mapping_details_for_sku(self, platform_sku: str) -> dict | None:
        """
        Maps a single platform SKU and returns a dictionary of all its mapping details.

        Args:
            platform_sku (str): The SKU from the platform report.

        Returns:
            dict or None: A dictionary with keys like 'msku', 'Panel', 'Status'
                          if a match is found, otherwise None.
        """
        if not platform_sku or pd.isna(platform_sku):
            return None

        try:
            normalized_sku = unicodedata.normalize('NFKD', str(platform_sku).strip()).encode('ascii', 'ignore').decode('utf-8').lower()
            if not normalized_sku:
                return None
            
            # We will search directly in the mapping_df for this
            # This is slightly slower than the pre-built dict but more flexible for getting multiple columns
            if self.mapping_df.empty:
                return None
            
            match_row = self.mapping_df[self.mapping_df['sku'] == normalized_sku]
            
            if not match_row.empty:
                # Return the first match as a dictionary
                details = match_row.iloc[0].to_dict()
                # Ensure keys are consistent if needed (e.g., lowercase)
                # For now, we assume they match the DataFrame columns: 'msku', 'Panel', 'Status'
                return details
            else:
                # We don't handle combos in this detailed lookup for now, as they don't have a single Panel/Status
                logger.warning(f"No mapping details found for SKU: '{normalized_sku}' (original: '{platform_sku}')")
                return None
        except Exception as e:
            logger.error(f"Error getting mapping details for SKU '{platform_sku}': {e}", exc_info=True)
            return None
        
    def get_mapping_details_for_msku(self, msku: str) -> list[dict] | None:
        """
        Finds all mapping records associated with a single MSKU.
        An MSKU can be linked to multiple Platform SKUs.

        Args:
            msku (str): The Master SKU to search for.

        Returns:
            list[dict] or None: A list of dictionaries, where each dictionary
                                represents a matching row (a platform SKU mapping).
                                Returns None if no matches are found.
        """
        if not msku or pd.isna(msku):
            return None

        try:
            # We search directly in the mapping_df for this
            if self.mapping_df.empty:
                return None
            
            # Find all rows where the 'msku' column matches
            # We use .strip() and .lower() on the MSKU for a case-insensitive match,
            # assuming MSKUs should be consistent.
            # Note: The 'msku' column in the df is already stripped from the fetcher.
            msku_to_find = str(msku).strip()
            
            match_rows = self.mapping_df[self.mapping_df['msku'] == msku_to_find]
            
            if not match_rows.empty:
                # Return all matching rows as a list of dictionaries
                return match_rows.to_dict('records')
            else:
                logger.warning(f"No mapping details found for MSKU: '{msku_to_find}'")
                return None
        except Exception as e:
            logger.error(f"Error getting mapping details for MSKU '{msku}': {e}", exc_info=True)
            return None
        
    def get_mapping_details_for_asin(self, asin: str) -> dict | None:
        """
        Finds the mapping record for a single ASIN.
        An ASIN should ideally map to one SKU/MSKU combination.
        """
        if not asin or pd.isna(asin):
            return None

        try:
            if self.asin_df.empty:
                return None
            
            asin_to_find = str(asin).strip().upper()
            
            match_row = self.asin_df[self.asin_df['asin'] == asin_to_find]
            
            if not match_row.empty:
                # Return the first match as a dictionary
                # We can merge this with the main mapping_df to get Panel info if needed
                # For now, let's return what's in the asin_df
                details = match_row.iloc[0].to_dict()
                return details
            else:
                logger.warning(f"No mapping details found for ASIN: '{asin_to_find}'")
                return None
        except Exception as e:
            logger.error(f"Error getting mapping details for ASIN '{asin}': {e}", exc_info=True)
            return None

    # Placeholder for inventory fetching logic
    # def get_inventory_for_mskus(self, mskus_list):
    #     """
    #     Fetches inventory for a list of MSKUs. (To be implemented later)
    #     This would use self.fetcher.get_inventory_data(inventory_table_id)
    #     and then filter/lookup based on mskus_list.
    #     """
    #     logger.info(f"Placeholder: Fetching inventory for MSKUs: {mskus_list}")
    #     # Example:
    #     # inventory_df = self._load_data_with_cache("inventory_data",
    #     #    lambda: self.fetcher.get_inventory_data(self.inventory_table_id))
    #     # if inventory_df.empty: return {}
    #     # result = inventory_df[inventory_df['MSKU'].isin(mskus_list)]
    #     # return result.set_index('MSKU')['Current Inventory'].to_dict()
    #     return {msku: 0 for msku in mskus_list} # Dummy data