# RMS/utils/file_utils.py
import os
import logging
from datetime import datetime
import shutil
import pandas as pd

logger = logging.getLogger(__name__)

# --- save_uploaded_file and clear_uploaded_data_folders remain the same ---
def save_uploaded_file(uploaded_file, platform_slug, account_slug, report_type, config):
    if uploaded_file is None: return None
    try:
        data_paths_config = config.get('data_paths', {})
        uploaded_root = data_paths_config.get('uploaded_sales_root', 'uploaded_data')
        sales_subdir = data_paths_config.get('sales_reports_subdir', 'sales')
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        target_dir_name = f"{platform_slug}_{account_slug}"
        target_path = os.path.join(project_root, uploaded_root, sales_subdir, target_dir_name)
        os.makedirs(target_path, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        original_filename, original_extension = os.path.splitext(uploaded_file.name)
        new_filename = f"{platform_slug}_{account_slug}_sales_{report_type}_{timestamp}{original_extension}"
        file_location = os.path.join(target_path, new_filename)
        with open(file_location, "wb") as f: f.write(uploaded_file.getbuffer())
        logger.info(f"Saved uploaded file: {new_filename} to {target_path}")
        return file_location
    except Exception as e:
        logger.error(f"Error saving uploaded file {uploaded_file.name}: {e}", exc_info=True)
        return None

def clear_uploaded_data_folders(config):
    try:
        data_paths_config = config.get('data_paths', {})
        uploaded_root = data_paths_config.get('uploaded_sales_root', 'uploaded_data')
        sales_subdir = data_paths_config.get('sales_reports_subdir', 'sales')
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        base_upload_path = os.path.join(project_root, uploaded_root, sales_subdir)
        if os.path.exists(base_upload_path) and os.path.isdir(base_upload_path):
            for item_name in os.listdir(base_upload_path):
                item_path = os.path.join(base_upload_path, item_name)
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                    logger.info(f"Removed directory: {item_path}")
            logger.info(f"All subdirectories in {base_upload_path} cleared.")
            return True
        else:
            logger.info(f"Base upload directory {base_upload_path} does not exist. Nothing to clear.")
            return True
    except Exception as e:
        logger.error(f"Error clearing uploaded data folders: {e}", exc_info=True)
        return False

def _find_latest_file_for_pattern(search_dir, file_pattern_start):
    """Helper to find the most recent file matching a starting pattern."""
    matching_files = []
    if not os.path.isdir(search_dir):
        return None
    for fname in os.listdir(search_dir):
        if fname.startswith(file_pattern_start) and (fname.endswith(".csv") or fname.endswith(".xlsx")):
            matching_files.append(os.path.join(search_dir, fname))
    if not matching_files:
        return None
    matching_files.sort(key=os.path.getmtime, reverse=True)
    return matching_files[0]

def get_uploaded_sales_files(platform_slug, account_slug, report_type_base, config):
    """
    Lists uploaded sales files.
    For '60day' report_type_base and specific platforms (meesho, flipkart),
    it looks for month1 and month2 files.
    Returns:
        - Single file path (str) for standard cases.
        - Dictionary {'month1': path1, 'month2': path2} for 2-part 60-day reports.
        - None if no files found.
    """
    try:
        data_paths_config = config.get('data_paths', {})
        uploaded_root = data_paths_config.get('uploaded_sales_root', 'uploaded_data')
        sales_subdir = data_paths_config.get('sales_reports_subdir', 'sales')

        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        target_dir_name = f"{platform_slug}_{account_slug}"
        search_dir = os.path.join(project_root, uploaded_root, sales_subdir, target_dir_name)

        platforms_needing_two_files_for_60day = ["meesho", "flipkart"] # Configurable if needed

        if report_type_base == "60day" and platform_slug.lower() in platforms_needing_two_files_for_60day:
            pattern_month1_start = f"{platform_slug}_{account_slug}_sales_60day_month1_"
            pattern_month2_start = f"{platform_slug}_{account_slug}_sales_60day_month2_"
            
            path_month1 = _find_latest_file_for_pattern(search_dir, pattern_month1_start)
            path_month2 = _find_latest_file_for_pattern(search_dir, pattern_month2_start)
            
            if path_month1 or path_month2: # Return dict even if one is missing
                return {'month1': path_month1, 'month2': path_month2}
            else:
                return None # Neither month found
        else:
            # Standard case (30day or 60day for other platforms)
            file_pattern_start = f"{platform_slug}_{account_slug}_sales_{report_type_base}_"
            return _find_latest_file_for_pattern(search_dir, file_pattern_start)

    except Exception as e:
        logger.error(f"Error getting uploaded files for {platform_slug}/{account_slug}/{report_type_base}: {e}", exc_info=True)
        return None


# --- Session DataFrame persistence functions (save_session_dataframe, etc.) remain the same ---
SESSION_DATA_DIR_NAME = ".session_data"
def _get_session_data_dir(project_root): return os.path.join(project_root, SESSION_DATA_DIR_NAME)
def save_session_dataframe(df, filename_prefix, project_root):
    if df is None: delete_session_dataframe(filename_prefix, project_root); return
    try:
        session_data_dir = _get_session_data_dir(project_root)
        os.makedirs(session_data_dir, exist_ok=True)
        file_path = os.path.join(session_data_dir, f"{filename_prefix}.parquet")
        df.to_parquet(file_path, index=False)
        logger.info(f"Saved session DataFrame to {file_path}")
    except Exception as e: logger.error(f"Error saving session DataFrame {filename_prefix}: {e}", exc_info=True)
def load_session_dataframe(filename_prefix, project_root):
    try:
        session_data_dir = _get_session_data_dir(project_root)
        file_path = os.path.join(session_data_dir, f"{filename_prefix}.parquet")
        if os.path.exists(file_path):
            df = pd.read_parquet(file_path); logger.info(f"Loaded session DataFrame from {file_path}"); return df
        else: logger.debug(f"Session DataFrame file not found: {file_path}"); return None
    except Exception as e: logger.error(f"Error loading session DataFrame {filename_prefix}: {e}", exc_info=True); return None
def delete_session_dataframe(filename_prefix, project_root):
    try:
        session_data_dir = _get_session_data_dir(project_root)
        file_path = os.path.join(session_data_dir, f"{filename_prefix}.parquet")
        if os.path.exists(file_path): os.remove(file_path); logger.info(f"Deleted session DataFrame file: {file_path}")
        else: logger.debug(f"Session DataFrame file to delete not found: {file_path}")
    except Exception as e: logger.error(f"Error deleting session DataFrame {filename_prefix}: {e}", exc_info=True)
def clear_all_session_dataframes(project_root):
    try:
        session_data_dir = _get_session_data_dir(project_root)
        if os.path.exists(session_data_dir) and os.path.isdir(session_data_dir):
            for item_name in os.listdir(session_data_dir):
                item_path = os.path.join(session_data_dir, item_name)
                if os.path.isfile(item_path) and item_path.endswith(".parquet"):
                    os.remove(item_path); logger.info(f"Deleted session data file: {item_path}")
            logger.info(f"All session DataFrame files in {session_data_dir} cleared.")
        else: logger.info(f"Session data directory {session_data_dir} does not exist. Nothing to clear.")
    except Exception as e: logger.error(f"Error clearing all session DataFrame files: {e}", exc_info=True)