# RMS/utils/cache_manager.py
import pandas as pd
import os
import json
import logging
from datetime import datetime, timedelta

# Get logger for this module
logger = logging.getLogger(__name__)

def load_from_cache(cache_name, cache_dir, cache_expiry_days):
    """Loads DataFrame and metadata from cache if valid."""
    df_path = os.path.join(cache_dir, f"{cache_name}.parquet")
    meta_path = os.path.join(cache_dir, f"{cache_name}_meta.json")

    if os.path.exists(df_path) and os.path.exists(meta_path):
        try:
            with open(meta_path, 'r') as f:
                meta = json.load(f)
            last_updated_str = meta.get("last_updated")
            if not last_updated_str:
                logger.warning(f"Cache metadata for '{cache_name}' is missing 'last_updated' field.")
                return None

            last_updated = datetime.fromisoformat(last_updated_str)
            if datetime.now() - last_updated < timedelta(days=cache_expiry_days):
                df = pd.read_parquet(df_path)
                logger.info(f"Loaded '{cache_name}' from cache (last updated: {last_updated.strftime('%Y-%m-%d %H:%M')}).")
                return df
            else:
                logger.info(f"Cache for '{cache_name}' is stale (last updated: {last_updated.strftime('%Y-%m-%d %H:%M')}).")
        except Exception as e:
            logger.warning(f"Error loading '{cache_name}' from cache: {e}. Will attempt to fetch fresh data.")
    else:
        logger.info(f"Cache for '{cache_name}' not found at {df_path} or {meta_path}.")
    return None

def save_to_cache(df, cache_name, cache_dir):
    """Saves DataFrame and metadata to cache."""
    os.makedirs(cache_dir, exist_ok=True) # Ensure cache directory exists
    df_path = os.path.join(cache_dir, f"{cache_name}.parquet")
    meta_path = os.path.join(cache_dir, f"{cache_name}_meta.json")
    try:
        df.to_parquet(df_path, index=False)
        meta = {"last_updated": datetime.now().isoformat()}
        with open(meta_path, 'w') as f:
            json.dump(meta, f)
        logger.info(f"Saved '{cache_name}' to cache at {df_path}.")
    except Exception as e:
        logger.error(f"Error saving '{cache_name}' to cache: {e}")