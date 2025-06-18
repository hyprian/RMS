# RMS/utils/file_utils.py
import os
import logging
from datetime import datetime
import shutil

logger = logging.getLogger(__name__)

def save_uploaded_file(uploaded_file, platform_slug, account_slug, report_type, config):
    """
    Saves an uploaded file to a structured directory.

    Args:
        uploaded_file: The file object from st.file_uploader.
        platform_slug (str): Slug for the platform (e.g., 'amazon').
        account_slug (str): Slug for the account (e.g., 'main').
        report_type (str): '30day' or '60day'.
        config (dict): The application configuration (APP_CONFIG).

    Returns:
        str: The path where the file was saved, or None if error.
    """
    if uploaded_file is None:
        return None

    try:
        data_paths_config = config.get('data_paths', {})
        uploaded_root = data_paths_config.get('uploaded_sales_root', 'uploaded_data')
        sales_subdir = data_paths_config.get('sales_reports_subdir', 'sales')

        # Construct path: RMS_ROOT/uploaded_data/sales/amazon_main/
        # Ensure project_root is correctly determined if this util is called from different places
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # RMS directory
        
        target_dir_name = f"{platform_slug}_{account_slug}"
        target_path = os.path.join(project_root, uploaded_root, sales_subdir, target_dir_name)
        
        os.makedirs(target_path, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        original_filename, original_extension = os.path.splitext(uploaded_file.name)
        # Sanitize original_filename if needed, though timestamp makes it unique
        
        # New filename: amazon_main_sales_30day_20240315103045.csv
        new_filename = f"{platform_slug}_{account_slug}_sales_{report_type}_{timestamp}{original_extension}"
        file_location = os.path.join(target_path, new_filename)

        with open(file_location, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        logger.info(f"Saved uploaded file: {new_filename} to {target_path}")
        return file_location
    except Exception as e:
        logger.error(f"Error saving uploaded file {uploaded_file.name}: {e}", exc_info=True)
        return None

def get_uploaded_sales_files(platform_slug, account_slug, report_type, config):
    """
    Lists uploaded sales files for a given platform, account, and report type.
    Returns the most recent one if multiple exist, or a list of all.
    For now, let's return the path to the most recent one.
    """
    try:
        data_paths_config = config.get('data_paths', {})
        uploaded_root = data_paths_config.get('uploaded_sales_root', 'uploaded_data')
        sales_subdir = data_paths_config.get('sales_reports_subdir', 'sales')

        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        target_dir_name = f"{platform_slug}_{account_slug}"
        search_dir = os.path.join(project_root, uploaded_root, sales_subdir, target_dir_name)

        if not os.path.isdir(search_dir):
            return None # Or [] if returning a list

        # Pattern: platform_account_sales_reporttype_timestamp.ext
        file_pattern = f"{platform_slug}_{account_slug}_sales_{report_type}_"
        
        matching_files = []
        for fname in os.listdir(search_dir):
            if fname.startswith(file_pattern) and (fname.endswith(".csv") or fname.endswith(".xlsx")): # Support both
                matching_files.append(os.path.join(search_dir, fname))
        
        if not matching_files:
            return None

        # Sort by modification time (or parse timestamp from filename) to get the latest
        matching_files.sort(key=os.path.getmtime, reverse=True)
        return matching_files[0] # Return the most recent file path

    except Exception as e:
        logger.error(f"Error getting uploaded files for {platform_slug}/{account_slug}/{report_type}: {e}", exc_info=True)
        return None # Or []
    

def clear_uploaded_data_folders(config):
    """Clears all subdirectories within the uploaded sales data folder."""
    try:
        data_paths_config = config.get('data_paths', {})
        uploaded_root = data_paths_config.get('uploaded_sales_root', 'uploaded_data')
        sales_subdir = data_paths_config.get('sales_reports_subdir', 'sales')
        
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # RMS directory
        base_upload_path = os.path.join(project_root, uploaded_root, sales_subdir)

        if os.path.exists(base_upload_path) and os.path.isdir(base_upload_path):
            for item_name in os.listdir(base_upload_path):
                item_path = os.path.join(base_upload_path, item_name)
                if os.path.isdir(item_path): # We expect subdirs like amazon_main, flipkart_fk_acc1
                    shutil.rmtree(item_path)
                    logger.info(f"Removed directory: {item_path}")
            logger.info(f"All subdirectories in {base_upload_path} cleared.")
            return True
        else:
            logger.info(f"Base upload directory {base_upload_path} does not exist. Nothing to clear.")
            return True # No error if it doesn't exist
    except Exception as e:
        logger.error(f"Error clearing uploaded data folders: {e}", exc_info=True)
        return False