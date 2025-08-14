# RMS/po_module/po_management.py
import pandas as pd
import logging
import requests
import json
from datetime import datetime

logger = logging.getLogger(__name__)

# --- Core PO Data Functions ---

def get_all_pos(fetcher, po_table_id: int) -> pd.DataFrame:
    """Fetches all rows from the Purchase Orders table."""
    logger.info(f"PO_MGMT: Fetching all purchase orders from table {po_table_id}")
    po_df = fetcher.get_table_data_as_dataframe(po_table_id)
    if po_df is None or po_df.empty:
        return pd.DataFrame()
    
    # --- CORRECTED DATE FORMAT ---
    # The format string now matches your sample data like "9-Dec-2024"
    date_format = "%d-%b-%Y"
    
    if 'Order Date' in po_df.columns:
        # We apply the correct format here
        po_df['Order Date'] = pd.to_datetime(po_df['Order Date'], format=date_format, errors='coerce')
    
    if 'Arrive by' in po_df.columns:
        po_df['Arrive by'] = pd.to_datetime(po_df['Arrive by'], format=date_format, errors='coerce')
        
    if 'Actual Receiving Date' in po_df.columns:
        po_df['Actual Receiving Date'] = pd.to_datetime(po_df['Actual Receiving Date'], format=date_format, errors='coerce')
    # --- END FIX ---

    # Convert other columns as before
    if 'Quantity' in po_df.columns:
        po_df['Quantity'] = pd.to_numeric(po_df['Quantity'], errors='coerce').fillna(0)
    if 'INR Amt' in po_df.columns:
        po_df['INR Amt'] = pd.to_numeric(po_df['INR Amt'], errors='coerce').fillna(0)
    
    return po_df

def get_po_details(po_df: pd.DataFrame, po_number: str) -> pd.DataFrame:
    """Filters the main PO DataFrame to get all line items for a specific PO number."""
    if po_df is None or po_df.empty or 'Po No.' not in po_df.columns:
        return pd.DataFrame()
    return po_df[po_df['Po No.'] == po_number].copy()

def create_po_line_item(fetcher, po_table_id: int, data_dict: dict) -> bool:
    """Creates a new row (a single line item) in the PO table."""
    logger.info(f"PO_MGMT: Creating new PO line item in table {po_table_id} for PO: {data_dict.get('Po No.')}")
    # This function already saves dates in the standard 'YYYY-MM-DD' format, which is good.
    # The warning only appears when reading inconsistent, manually entered data.
    return fetcher.batch_create_rows(po_table_id, [data_dict])

def update_po_line_item(fetcher, po_table_id: int, row_id: int, data_dict: dict) -> bool:
    """Updates an existing PO line item row."""
    logger.info(f"PO_MGMT: Updating PO line item row {row_id} in table {po_table_id}")
    url = f"{fetcher.base_url}/api/database/rows/table/{po_table_id}/{row_id}/?user_field_names=true"
    try:
        response = requests.patch(url, headers=fetcher.headers, json=data_dict)
        response.raise_for_status()
        logger.info(f"Successfully updated row {row_id}.")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Error updating row {row_id}: {e}", exc_info=True)
        if response is not None: logger.error(f"Response content: {response.text}")
        return False

def delete_po_line_item(fetcher, po_table_id: int, row_id: int) -> bool:
    """Deletes a specific PO line item row."""
    logger.info(f"PO_MGMT: Deleting PO line item row {row_id} from table {po_table_id}")
    return fetcher.delete_single_row(po_table_id, row_id) # Reuse existing working method

# --- File Handling Functions ---

def upload_file_to_baserow(fetcher, file_bytes, filename: str) -> dict | None:
    """
    Uploads a file to Baserow's user files endpoint.
    Returns a dictionary object suitable for a Baserow File field, or None on failure.
    Example return: {'name': 'invoice.pdf', 'url': 'https://.../invoice.pdf', 'is_image': False}
    """
    logger.info(f"PO_MGMT: Uploading file '{filename}' to Baserow user files.")
    url = f"{fetcher.base_url}/api/user-files/upload-file/"
    try:
        files = {'file': (filename, file_bytes)}
        # Note: For file uploads, we don't send a JSON content-type header
        upload_headers = {"Authorization": fetcher.headers["Authorization"]}
        
        response = requests.post(url, headers=upload_headers, files=files)
        response.raise_for_status()
        
        file_data = response.json()
        logger.info(f"Successfully uploaded file '{filename}'. URL: {file_data.get('url')}")
        
        # Baserow file fields expect a list of objects with a 'name' key
        # The API response for user-file upload gives 'name', 'url', 'is_image', etc.
        # We just need to return the main object.
        return file_data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error uploading file '{filename}': {e}", exc_info=True)
        if response is not None: logger.error(f"Response content: {response.text}")
        return None

# --- Helper Functions ---

def get_distinct_values(po_df: pd.DataFrame, column_name: str) -> list:
    """Gets a sorted list of unique, non-empty values from a DataFrame column."""
    if po_df is None or po_df.empty or column_name not in po_df.columns:
        return []
    return sorted(list(po_df[column_name].dropna().unique()))

def get_msku_details(category_df: pd.DataFrame, msku: str) -> dict:
    """
    Fetches details like Category and HSN code for a given MSKU from the cached category DataFrame.
    """
    if category_df is None or category_df.empty or not msku:
        return {}
    
    msku_details = category_df[category_df['MSKU'] == msku]
    if msku_details.empty:
        return {}
    
    # Convert the first matching row to a dictionary
    first_row_dict = msku_details.iloc[0].to_dict()
    
    details_to_return = {
        'Category': first_row_dict.get('Category', ''),
        'HSN Code': first_row_dict.get('HSN Code', '') 
    }
    
    logger.debug(f"Fetched details for {msku}: {details_to_return}")
    return details_to_return

def generate_po_number() -> str:
    """Generates a new, unique PO Number."""
    now = datetime.now()
    # Format: PO-YYYYMMDD-HHMMSS
    return f"PO-{now.strftime('%Y%m%d-%H%M%S')}"

def get_msku_cost_details(category_df: pd.DataFrame, msku: str) -> dict:
    """
    Fetches cost details for a given MSKU from the cached category/product DataFrame.
    Returns a dictionary with costs, or defaults to 0.0 if not found.
    """
    if category_df is None or category_df.empty or not msku: # Added check for empty msku
        return {'inr_cost': 0.0, 'usd_cost': 0.0}
    
    msku_details = category_df[category_df['MSKU'] == msku]
    
    # --- FIX FOR INDEXERROR ---
    if msku_details.empty:
        # If no matching MSKU is found, return default values
        return {'inr_cost': 0.0, 'usd_cost': 0.0}
    
    # If a match is found, get the values from the first row
    # Use .get() on the dictionary representation of the row for safety
    first_row_dict = msku_details.iloc[0].to_dict()
    
    cost_info = {
        'inr_cost': first_row_dict.get('Cost Inc.GST', 0.0),
        'usd_cost': first_row_dict.get('per pcs price usd', 0.0)
    }
    return cost_info

def get_open_po_data(all_pos_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates data for all open POs to get on-order quantities
    by shipment route, the earliest sea ETA, AND a detailed list of POs for each MSKU.
    """
    if all_pos_df is None or all_pos_df.empty:
        return pd.DataFrame()

    logger.info("PO_MGMT: Aggregating open PO data...")
    open_statuses = ["Draft", "Sent For Approval", "Final Invoice Received", "Dispatched", "In Transit", "On Hold"]
    open_pos_df = all_pos_df[all_pos_df['Status'].isin(open_statuses)].copy()

    if open_pos_df.empty:
        logger.info("PO_MGMT: No open POs found.")
        return pd.DataFrame()

    open_pos_df['Quantity'] = pd.to_numeric(open_pos_df['Quantity'], errors='coerce').fillna(0)
    open_pos_df['Arrive by'] = pd.to_datetime(open_pos_df['Arrive by'], errors='coerce')
    open_pos_df['Shipment Route'] = open_pos_df['Shipment Route'].fillna('Air')
    open_pos_df['Msku Code'] = open_pos_df['Msku Code'].astype(str)

    # --- AGGREGATE ON-ORDER QUANTITIES BY ROUTE (No change here) ---
    on_order_qty_df = open_pos_df.pivot_table(
        index='Msku Code', columns='Shipment Route', values='Quantity',
        aggfunc='sum', fill_value=0
    ).reset_index()
    on_order_qty_df.rename(columns={'Msku Code': 'MSKU', 'Air': 'on_order_qty_air', 'Sea': 'on_order_qty_sea'}, inplace=True)
    if 'on_order_qty_air' not in on_order_qty_df.columns: on_order_qty_df['on_order_qty_air'] = 0
    if 'on_order_qty_sea' not in on_order_qty_df.columns: on_order_qty_df['on_order_qty_sea'] = 0

    # --- FIND EARLIEST SEA ETA (No change here) ---
    sea_pos_df = open_pos_df[open_pos_df['Shipment Route'].str.upper() == 'SEA'].copy()
    if not sea_pos_df.empty:
        sea_eta_df = sea_pos_df.groupby('Msku Code')['Arrive by'].min().reset_index()
        sea_eta_df.rename(columns={'Msku Code': 'MSKU', 'Arrive by': 'sea_order_eta'}, inplace=True)
        final_open_po_df = pd.merge(on_order_qty_df, sea_eta_df, on='MSKU', how='left')
    else:
        final_open_po_df = on_order_qty_df
        final_open_po_df['sea_order_eta'] = pd.NaT

    # --- NEW: AGGREGATE THE DETAILED LIST OF DICTIONARIES ---
    def create_detail_dict(row):
        return {
            'Po No.': row.get('Po No.'),
            'Vendor Name': row.get('Vendor Name'),
            'Quantity': int(row.get('Quantity', 0)),
            'Arrive by': row.get('Arrive by').strftime('%d-%b-%Y') if pd.notna(row.get('Arrive by')) else 'N/A',
            'Shipment Route': row.get('Shipment Route')
        }
    
    open_pos_df['PO_Detail_Dict'] = open_pos_df.apply(create_detail_dict, axis=1)
    
    po_details_agg = open_pos_df.groupby('Msku Code').agg(
        PO_Details=('PO_Detail_Dict', lambda x: list(x))
    ).reset_index()
    po_details_agg.rename(columns={'Msku Code': 'MSKU'}, inplace=True)
    
    # --- MERGE THE DETAILS BACK INTO THE FINAL DATAFRAME ---
    final_open_po_df = pd.merge(final_open_po_df, po_details_agg, on='MSKU', how='left')
    # Ensure the PO_Details column exists even if the merge fails for some reason
    if 'PO_Details' not in final_open_po_df.columns:
        final_open_po_df['PO_Details'] = [[] for _ in range(len(final_open_po_df))]
    else:
        # Fill any NaNs with empty lists
        final_open_po_df['PO_Details'] = final_open_po_df['PO_Details'].apply(lambda d: d if isinstance(d, list) else [])

    logger.info(f"PO_MGMT: Aggregated open PO data for {len(final_open_po_df)} MSKUs.")
    return final_open_po_df


def get_last_order_dates(all_pos_df: pd.DataFrame) -> pd.DataFrame:
    """
    Finds the most recent 'Order Date' for each MSKU from all POs.
    """
    if all_pos_df is None or all_pos_df.empty or 'Msku Code' not in all_pos_df.columns or 'Order Date' not in all_pos_df.columns:
        logger.warning("PO_MGMT: Cannot get last order dates. DataFrame is empty or missing required columns.")
        return pd.DataFrame(columns=['MSKU', 'last_order_date'])

    # Ensure 'Order Date' is a datetime object, coercing errors
    df = all_pos_df[['Msku Code', 'Order Date']].copy()
    df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
    df.dropna(subset=['Msku Code', 'Order Date'], inplace=True)

    if df.empty:
        return pd.DataFrame(columns=['MSKU', 'last_order_date'])

    # Find the max (most recent) date for each MSKU
    last_dates = df.groupby('Msku Code')['Order Date'].max().reset_index()
    last_dates.rename(columns={'Msku Code': 'MSKU', 'Order Date': 'last_order_date'}, inplace=True)
    
    logger.info(f"PO_MGMT: Found last order dates for {len(last_dates)} MSKUs.")
    return last_dates