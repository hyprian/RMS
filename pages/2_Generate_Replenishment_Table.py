# RMS/pages/2_Generate_Replenishment_Table.py
import streamlit as st
import pandas as pd
import os
import sys
from datetime import datetime
import math

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
from data_processing.sku_mapper import SKUMapper
from utils.file_utils import get_uploaded_sales_files, clear_uploaded_data_folders
from utils.table_utils import get_replenishment_table_column_config # Import the new function
import logging

logger = logging.getLogger(__name__)

st.set_page_config(page_title="Generate Replenishment Table - RMS", layout="wide")
st.title("📋 Generate Replenishment Table")

# Initialize session state keys
if 'replenishment_df' not in st.session_state:
    st.session_state.replenishment_df = None
if 'unmapped_df' not in st.session_state:
    st.session_state.unmapped_df = None
if 'data_processed_successfully' not in st.session_state:
    st.session_state.data_processed_successfully = False
if 'edited_replenishment_df' not in st.session_state: # To store user edits
    st.session_state.edited_replenishment_df = None


baserow_config = APP_CONFIG.get('baserow', {})
cache_config = APP_CONFIG.get('cache', {})
platforms_config = APP_CONFIG.get('platforms', [])
replenishment_config = APP_CONFIG.get('replenishment_table', {})
feature_date_format = replenishment_config.get('feature_date_format', "%d-%b-%Y")

@st.cache_resource
def get_data_tools():
    # ... (get_data_tools function remains the same) ...
    logger.info("Initializing BaserowFetcher and SKUMapper...")
    try:
        fetcher = BaserowFetcher(api_token=baserow_config.get('api_token'), base_url=baserow_config.get('base_url'))
        project_root_for_mapper = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sku_mapper = SKUMapper(
            baserow_fetcher=fetcher,
            sku_mapping_table_id=baserow_config.get('sku_mapping_table_id'),
            combo_sku_table_id=baserow_config.get('combo_sku_table_id'),
            cache_config=cache_config,
            project_root_dir=project_root_for_mapper,
            force_refresh_cache=st.session_state.get('force_refresh_baserow_cache', False)
        )
        logger.info("BaserowFetcher and SKUMapper initialized successfully.")
        return fetcher, sku_mapper
    except Exception as e:
        logger.error(f"Error initializing data tools: {e}", exc_info=True)
        st.error(f"Error initializing connection to data sources: {e}")
        return None, None

fetcher, sku_mapper = get_data_tools()

if not fetcher or not sku_mapper:
    st.error("Could not initialize data processing tools. Please check configuration and Baserow connection.")
    st.stop()

# --- UI Elements for Controls ---
col1, col3 = st.columns([2,1]) # Removed middle column for checkbox, moved to sidebar
with col1:
    process_button_clicked = st.button("🚀 Process Data and Generate Table", key="process_data", use_container_width=True)
st.sidebar.checkbox("Force refresh Baserow cache", key="force_refresh_baserow_cache",
                    help="If checked, SKU/Combo mappings will be fetched fresh from Baserow, ignoring local cache. Re-click 'Process Data' after checking/unchecking.")
with col3:
    if st.button("🔄 Reset All Data & Uploads", key="reset_all", type="primary", use_container_width=True):
        st.session_state.replenishment_df = None
        st.session_state.unmapped_df = None
        st.session_state.data_processed_successfully = False
        st.session_state.edited_replenishment_df = None # Reset edited DF
        if 'uploaded_file_paths' in st.session_state:
            st.session_state.uploaded_file_paths = {}
        
        if clear_uploaded_data_folders(APP_CONFIG):
            st.success("All uploaded data files and generated tables have been reset.")
        else:
            st.error("Failed to clear all uploaded data files. Please check logs.")
        st.rerun()


st.info("This page will process the uploaded sales data and inventory to generate the replenishment table.")
st.subheader("Uploaded Sales Data to be Processed:")
# ... (displaying uploaded files summary - files_to_process_summary - remains the same) ...
files_to_process_summary = []
at_least_one_file_pair = False
for platform_conf in platforms_config:
    p_slug = platform_conf['slug']
    for acc_conf in platform_conf['accounts']:
        a_slug = acc_conf['slug']
        key_30day = f"{p_slug}_{a_slug}_30day"
        file_30day_path = st.session_state.get('uploaded_file_paths', {}).get(key_30day) or \
                          get_uploaded_sales_files(p_slug, a_slug, "30day", APP_CONFIG)
        key_60day = f"{p_slug}_{a_slug}_60day"
        file_60day_path = st.session_state.get('uploaded_file_paths', {}).get(key_60day) or \
                          get_uploaded_sales_files(p_slug, a_slug, "60day", APP_CONFIG)
        if file_30day_path and file_60day_path:
            files_to_process_summary.append(
                f"- {platform_conf['name']} ({acc_conf['name']}): "
                f"30-day ({os.path.basename(file_30day_path)}), "
                f"60-day ({os.path.basename(file_60day_path)})"
            )
            at_least_one_file_pair = True
        elif file_30day_path or file_60day_path:
             missing = "60-day" if file_30day_path else "30-day"
             present_file = os.path.basename(file_30day_path or file_60day_path)
             files_to_process_summary.append(
                f"- {platform_conf['name']} ({acc_conf['name']}): "
                f"Only one report found ({present_file}). Missing {missing} report. This account will be skipped or processed partially."
            )
if not files_to_process_summary and st.session_state.replenishment_df is None :
    st.warning("No sales data uploaded yet or files not found. Please go to the 'Upload Sales Data' page.")
elif files_to_process_summary:
    for item in files_to_process_summary:
        st.markdown(item)

process_button_disabled = not at_least_one_file_pair
if process_button_clicked and process_button_disabled:
    st.error("At least one platform account needs both 30-day and 60-day sales reports uploaded to proceed.")

def read_sales_data(file_path, sku_col_name_config):
    # ... (read_sales_data function remains the same) ...
    _, file_extension = os.path.splitext(file_path)
    df = None
    dtype_spec = {sku_col_name_config: str} if sku_col_name_config else None
    try:
        if file_extension.lower() == ".csv":
            df = pd.read_csv(file_path, dtype=dtype_spec)
        elif file_extension.lower() == ".xlsx":
            df = pd.read_excel(file_path, dtype=dtype_spec, engine='openpyxl')
        else:
            st.error(f"Unsupported file format: {file_extension} for file {file_path}")
            logger.error(f"Unsupported file format: {file_extension} for file {file_path}")
            return pd.DataFrame()
        return df
    except Exception as e:
        st.error(f"Error reading file {os.path.basename(file_path)}: {e}")
        logger.error(f"Error reading file {file_path}: {e}", exc_info=True)
        return pd.DataFrame()


if process_button_clicked and not process_button_disabled:
    with st.spinner("Processing data... This may take a moment."):
        try:
            # ... (Sales data processing logic remains the same, populating all_sales_data_30day, etc.) ...
            # ... (Inventory fetching logic remains the same) ...
            # ... (Constructing final_data list with calculations remains the same) ...
            all_sales_data_30day = {}
            all_sales_data_60day = {}
            processed_mskus = set()
            unmapped_skus_collection = []
            current_date_for_code = datetime.now()
            fixed_generated_code = f"{current_date_for_code.strftime('%m-%d')}-001"
            logger.info("Starting sales data processing...")
            for platform_conf in platforms_config:
                p_slug = platform_conf['slug']
                p_name = platform_conf['name']
                for acc_conf in platform_conf['accounts']:
                    a_slug = acc_conf['slug']
                    a_name = acc_conf['name']
                    sku_col_name = acc_conf.get('sku_column') 
                    if not sku_col_name: 
                        sku_col_name = platform_conf.get('sku_column', APP_CONFIG.get('default_sales_report_columns',{}).get('sku_column','SKU'))
                    actual_qty_cols = []
                    qty_cols_list_config_acc = acc_conf.get('quantity_columns')
                    if qty_cols_list_config_acc and isinstance(qty_cols_list_config_acc, list):
                        actual_qty_cols = qty_cols_list_config_acc
                    else:
                        qty_col_config_acc = acc_conf.get('quantity_column')
                        if qty_col_config_acc and isinstance(qty_col_config_acc, str):
                            actual_qty_cols = [qty_col_config_acc]
                        else:
                            qty_cols_list_config_plat = platform_conf.get('quantity_columns')
                            if qty_cols_list_config_plat and isinstance(qty_cols_list_config_plat, list):
                                actual_qty_cols = qty_cols_list_config_plat
                            else:
                                qty_col_config_plat = platform_conf.get('quantity_column')
                                if qty_col_config_plat and isinstance(qty_col_config_plat, str):
                                    actual_qty_cols = [qty_col_config_plat]
                                else:
                                    default_qty_col = APP_CONFIG.get('default_sales_report_columns',{}).get('quantity_column','Quantity')
                                    if default_qty_col:
                                        actual_qty_cols = [default_qty_col]
                    if not actual_qty_cols:
                        st.error(f"Quantity column(s) not configured correctly for {p_name} - {a_name}. Skipping.")
                        continue
                    key_30day = f"{p_slug}_{a_slug}_30day"
                    file_30day_path = st.session_state.get('uploaded_file_paths', {}).get(key_30day) or \
                                      get_uploaded_sales_files(p_slug, a_slug, "30day", APP_CONFIG)
                    key_60day = f"{p_slug}_{a_slug}_60day"
                    file_60day_path = st.session_state.get('uploaded_file_paths', {}).get(key_60day) or \
                                      get_uploaded_sales_files(p_slug, a_slug, "60day", APP_CONFIG)
                    if not file_30day_path or not file_60day_path:
                        continue
                    try: # 30-day
                        sales_df_30 = read_sales_data(file_30day_path, sku_col_name)
                        if sales_df_30.empty: continue
                        if sku_col_name not in sales_df_30.columns: continue
                        missing_qty_cols = [q_col for q_col in actual_qty_cols if q_col not in sales_df_30.columns]
                        if missing_qty_cols: continue
                        sales_df_30['calculated_quantity'] = 0
                        for q_col in actual_qty_cols:
                            if sales_df_30[q_col].dtype == 'object': sales_df_30[q_col] = sales_df_30[q_col].astype(str).str.replace('₹', '', regex=False).str.replace(',', '', regex=False)
                            sales_df_30[q_col] = pd.to_numeric(sales_df_30[q_col], errors='coerce').fillna(0)
                            sales_df_30['calculated_quantity'] += sales_df_30[q_col]
                        for _, row in sales_df_30.iterrows():
                            platform_sku_original = row[sku_col_name]
                            quantity = row['calculated_quantity']
                            msku_result = sku_mapper.map_sku_to_msku(platform_sku_original)
                            if msku_result:
                                mskus_to_update = [msku_result] if isinstance(msku_result, str) else msku_result
                                for msku_item in mskus_to_update:
                                    if msku_item:
                                        all_sales_data_30day[msku_item] = all_sales_data_30day.get(msku_item, 0) + quantity
                                        processed_mskus.add(msku_item)
                            else:
                                if platform_sku_original and not pd.isna(platform_sku_original): unmapped_skus_collection.append({'Platform SKU': platform_sku_original, 'Platform': p_name, 'Account': a_name, 'Report Type': '30-Day Sales'})
                    except Exception as e: st.error(f"Error processing 30-day report for {p_name} - {a_name}: {e}")
                    try: # 60-day
                        sales_df_60 = read_sales_data(file_60day_path, sku_col_name)
                        if sales_df_60.empty: continue
                        if sku_col_name not in sales_df_60.columns: continue
                        missing_qty_cols_60 = [q_col for q_col in actual_qty_cols if q_col not in sales_df_60.columns]
                        if missing_qty_cols_60: continue
                        sales_df_60['calculated_quantity'] = 0
                        for q_col in actual_qty_cols:
                            if sales_df_60[q_col].dtype == 'object': sales_df_60[q_col] = sales_df_60[q_col].astype(str).str.replace('₹', '', regex=False).str.replace(',', '', regex=False)
                            sales_df_60[q_col] = pd.to_numeric(sales_df_60[q_col], errors='coerce').fillna(0)
                            sales_df_60['calculated_quantity'] += sales_df_60[q_col]
                        for _, row in sales_df_60.iterrows():
                            platform_sku_original = row[sku_col_name]
                            quantity = row['calculated_quantity']
                            msku_result = sku_mapper.map_sku_to_msku(platform_sku_original)
                            if msku_result:
                                mskus_to_update = [msku_result] if isinstance(msku_result, str) else msku_result
                                for msku_item in mskus_to_update:
                                     if msku_item:
                                        all_sales_data_60day[msku_item] = all_sales_data_60day.get(msku_item, 0) + quantity
                                        processed_mskus.add(msku_item)
                            else:
                                if platform_sku_original and not pd.isna(platform_sku_original): unmapped_skus_collection.append({'Platform SKU': platform_sku_original, 'Platform': p_name, 'Account': a_name, 'Report Type': '60-Day Sales'})
                    except Exception as e: st.error(f"Error processing 60-day report for {p_name} - {a_name}: {e}")

            inventory_df = pd.DataFrame()
            if processed_mskus:
                inventory_table_id = baserow_config.get('inventory_table_id')
                if inventory_table_id:
                    raw_inventory_df = fetcher.get_inventory_data(inventory_table_id)
                    if not raw_inventory_df.empty and 'MSKU' in raw_inventory_df.columns:
                        inventory_df = raw_inventory_df[raw_inventory_df['MSKU'].isin(list(processed_mskus))].copy()
            final_data = []
            current_date_formatted = datetime.now().strftime(feature_date_format)
            all_mskus_for_table = set(inventory_df['MSKU'].tolist()) | processed_mskus
            if not all_mskus_for_table: st.warning("No MSKUs to process for the final table.")
            for msku_loop_var in sorted(list(all_mskus_for_table)):
                inv_row = inventory_df[inventory_df['MSKU'] == msku_loop_var]
                current_inventory = inv_row['Current Inventory'].iloc[0] if not inv_row.empty else 0
                images, category = "", ""
                sales_30 = all_sales_data_30day.get(msku_loop_var, 0)
                sales_60 = all_sales_data_60day.get(msku_loop_var, 0)
                required_qty = math.ceil(sales_60 * 1.20)
                buffer_stock = math.ceil(required_qty * 0.20)
                marketing_stock = buffer_stock
                final_order_qty = required_qty + buffer_stock + marketing_stock
                shipment_status = "Sea" if current_inventory >= sales_30 else "Air"
                product_status, po_no, order_status, notes = "Fast Moving", "KI1589559", "", "" # Default PO No.
                final_data.append({
                    "Feature": current_date_formatted, "Msku": msku_loop_var, "Images": images,
                    "Category": category, "Code": fixed_generated_code, "Current Inventory": current_inventory,
                    "30 days sales": sales_30, "60 days sales": sales_60, "Required Qty": required_qty,
                    "Buffer Stock": buffer_stock, "Marketing Stock": marketing_stock,
                    "Final Order Qty": final_order_qty, "Shipment Status": shipment_status,
                    "Product Status": product_status, "Po No.": po_no, "Order Status": order_status, "Notes": notes
                })


            if final_data:
                # Store the originally generated DF
                st.session_state.replenishment_df = pd.DataFrame(final_data)
                # Also copy it to edited_replenishment_df initially
                st.session_state.edited_replenishment_df = st.session_state.replenishment_df.copy()
                st.session_state.data_processed_successfully = True
            else:
                st.session_state.replenishment_df = None
                st.session_state.edited_replenishment_df = None
                st.session_state.data_processed_successfully = True

            if unmapped_skus_collection:
                unmapped_df_temp = pd.DataFrame(unmapped_skus_collection)
                unmapped_df_temp.drop_duplicates(subset=['Platform SKU', 'Platform', 'Account'], inplace=True)
                st.session_state.unmapped_df = unmapped_df_temp
            else:
                st.session_state.unmapped_df = None
            
            logger.info("Replenishment table generation process completed.")
            st.success("Data processing complete!")
            st.rerun()

        except Exception as e:
            logger.error(f"Error during replenishment table generation: {e}", exc_info=True)
            st.error(f"An error occurred: {e}")
            st.session_state.replenishment_df = None
            st.session_state.edited_replenishment_df = None
            st.session_state.unmapped_df = None
            st.session_state.data_processed_successfully = False


# --- Display Logic (uses session state) ---
# Use edited_replenishment_df for display and editing if it exists, otherwise use replenishment_df
# This ensures that if processing happens again, edits are not lost until explicitly reset or reprocessed.
# However, for simplicity, let's always edit replenishment_df and save it back.
# If you want to keep original and edited separate, you'd need more complex logic.

# For now, we edit st.session_state.replenishment_df directly
if st.session_state.get('replenishment_df') is not None:
    st.subheader("Generated Replenishment Table (Editable)")
    
    # Get column configurations
    column_config = get_replenishment_table_column_config()

    # Use st.data_editor
    # The key for data_editor is important if you want to access its state, but for just getting
    # the edited_df back, it's not strictly necessary to assign it to a variable if you use the return value.
    edited_df = st.data_editor(
        st.session_state.replenishment_df,
        column_config=column_config,
        num_rows="dynamic", # Allows adding/deleting rows, set to "fixed" if not desired
        use_container_width=True,
        height=600, # Adjust height as needed
        key="replenishment_editor" # Add a key
    )

    # IMPORTANT: Update session state with the edited data
    # This happens on every interaction with the data_editor (e.g., cell edit)
    if not edited_df.equals(st.session_state.replenishment_df):
        st.session_state.replenishment_df = edited_df
        # st.info("Changes saved to current session.") # Optional feedback

    # Download button will now download the potentially edited data
    if st.session_state.replenishment_df is not None and not st.session_state.replenishment_df.empty:
        csv_repl = st.session_state.replenishment_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Edited Table as CSV", data=csv_repl,
            file_name=f"edited_replenishment_table_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv", mime="text/csv",
            key="download_edited_repl_table"
        )

elif st.session_state.data_processed_successfully and st.session_state.replenishment_df is None:
    st.warning("Processing complete, but no data to display in the replenishment table.")


if st.session_state.unmapped_df is not None and not st.session_state.unmapped_df.empty:
    st.subheader("⚠️ Unmapped SKUs")
    st.warning(f"Found {len(st.session_state.unmapped_df)} unique platform SKUs that could not be mapped.")
    st.dataframe(st.session_state.unmapped_df) # Unmapped table is not typically edited here
    
    unmapped_csv = st.session_state.unmapped_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download Unmapped SKUs as CSV", data=unmapped_csv,
        file_name=f"unmapped_skus_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv", mime="text/csv",
        key="download_unmapped_table"
    )
elif st.session_state.data_processed_successfully and (st.session_state.unmapped_df is None or st.session_state.unmapped_df.empty):
    if st.session_state.data_processed_successfully : # Check if processing was attempted
         st.success("✅ All SKUs from processed sales reports were successfully mapped (or no SKUs needed mapping).")

logger.info("Generate Replenishment Table page loaded.")