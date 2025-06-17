# RMS/main_rms.py
import logging
import os
import pandas as pd

# Ensure project root is in sys.path if running from a different directory or for imports
import sys
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG # This also sets up logging
from data_processing.baserow_fetcher import BaserowFetcher
from data_processing.sku_mapper import SKUMapper

logger = logging.getLogger(__name__)

def run_mapper_test():
    logger.info("Starting SKU Mapper Test...")

    if not APP_CONFIG:
        logger.error("Application configuration not loaded. Exiting test.")
        return

    baserow_config = APP_CONFIG.get('baserow', {})
    cache_config = APP_CONFIG.get('cache', {})

    api_token = baserow_config.get('api_token')
    sku_mapping_table_id = baserow_config.get('sku_mapping_table_id')
    combo_sku_table_id = baserow_config.get('combo_sku_table_id')

    if not all([api_token, sku_mapping_table_id, combo_sku_table_id]):
        logger.error("Baserow configuration (api_token, sku_mapping_table_id, combo_sku_table_id) is incomplete in settings.yaml.")
        return

    try:
        fetcher = BaserowFetcher(api_token=api_token, base_url=baserow_config.get('base_url'))
        
        # Pass project_root for correct cache path construction
        # Set force_refresh_cache=True if you want to bypass cache for this test run
        sku_mapper = SKUMapper(
            baserow_fetcher=fetcher,
            sku_mapping_table_id=sku_mapping_table_id,
            combo_sku_table_id=combo_sku_table_id,
            cache_config=cache_config,
            project_root_dir=project_root, 
            force_refresh_cache=False # Set to True to test fetching fresh data
        )

        # --- Test individual SKU mapping ---
        logger.info("\n--- Testing individual SKU mapping ---")
        test_skus = [
            "CSTE_0142_SG_FireGlass_Black", # Example from your target output (assuming it's a platform SKU)
            "SOME_COMBO_SKU_FROM_YOUR_BASEROW", # Replace with an actual combo SKU from your Baserow
            "NON_EXISTENT_SKU_123",
            "CSTE_0020_MB_HP_Black", # Another example
            None, # Test None input
            ""    # Test empty string input
        ]

        for sku in test_skus:
            if sku: # Only log if sku is not None or empty for cleaner logs here
                 logger.info(f"Mapping platform SKU: '{sku}'")
            msku_result = sku_mapper.map_sku_to_msku(sku)
            if msku_result:
                logger.info(f"  -> Mapped to: {msku_result} (Type: {type(msku_result).__name__})")
            else:
                logger.info(f"  -> No mapping found or error for SKU: '{sku}'")
        
        # --- Test mapping SKUs in a dummy sales report DataFrame ---
        logger.info("\n--- Testing mapping SKUs in a DataFrame ---")
        
        # Create a dummy sales report based on your Amazon sample
        # For a real test, you'd load an actual (small) sales CSV
        amazon_sample_data = {
            'Title': [
                'CAAJU Money Bank with Amount 100000 Piggy Bank for Girls and Boys - Carved Wooden Money Saving Box - Money Box Gullak for Adults', 
                'Fusked Harry Potter Light Wand...', 
                'Fusked Dumbledore Light Wand...',
                'A Combo Product Example', # Add a row for a combo SKU
                'An Unmappable Product'
            ],
            'SKU': [ # This is the platform SKU column
                'CSTE_0550_HD_Money Box_Wooden_Brown', 
                'CSTE_0330_Wands_Harrypotter_Light', 
                'CSTE_0333_Wands_Dumbledore_Light',
                'SOME_COMBO_SKU_FROM_YOUR_BASEROW', # Use the same combo SKU as above
                'UNMAPPABLE_XYZ789'
            ],
            'Units Ordered': [1607, 212, 167, 10, 5]
        }
        sales_df = pd.DataFrame(amazon_sample_data)
        logger.info("Original Sales DataFrame:")
        logger.info(f"\n{sales_df.to_string()}")

        # Assuming 'SKU' is the column name in your Amazon report that needs mapping
        # This 'SKU' column should contain the platform-specific SKUs
        mapped_sales_df = sku_mapper.map_sales_report_skus(sales_df.copy(), platform_sku_column='SKU')
        
        logger.info("\nSales DataFrame after MSKU mapping:")
        logger.info(f"\n{mapped_sales_df.to_string()}")
        
        logger.info("\nSKU Mapper Test Completed.")

    except ValueError as ve:
        logger.error(f"Configuration error during test: {ve}")
    except Exception as e:
        logger.error(f"An error occurred during the SKU mapper test: {e}", exc_info=True)

if __name__ == "__main__":
    run_mapper_test()