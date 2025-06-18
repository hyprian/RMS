# RMS/pages/2_Generate_Replenishment_Table.py
import streamlit as st
import pandas as pd
import os
import sys
from datetime import datetime
import math

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
from data_processing.sku_mapper import SKUMapper
from utils.file_utils import (
    get_uploaded_sales_files, clear_uploaded_data_folders,
    save_session_dataframe, load_session_dataframe, delete_session_dataframe
)
from utils.table_utils import get_replenishment_table_column_config
import logging

logger = logging.getLogger(__name__)
st.set_page_config(page_title="Generate Replenishment Table - RMS", layout="wide")
st.title("📋 Generate Replenishment Table")

REPLENISHMENT_DF_FILENAME = "session_replenishment_df"
UNMAPPED_DF_FILENAME = "session_unmapped_df"
DATA_PROCESSED_FLAG_FILENAME = "session_data_processed_flag"

def load_persisted_data_into_session():
    if st.session_state.get('replenishment_df') is None:
        df = load_session_dataframe(REPLENISHMENT_DF_FILENAME, project_root)
        st.session_state.replenishment_df = df if df is not None else None
    if st.session_state.get('unmapped_df') is None:
        df = load_session_dataframe(UNMAPPED_DF_FILENAME, project_root)
        st.session_state.unmapped_df = df if df is not None else None
    if not st.session_state.get('data_processed_successfully', False) : # Check current session state first
        flag_file_path = os.path.join(project_root, ".session_data", f"{DATA_PROCESSED_FLAG_FILENAME}.flag")
        st.session_state.data_processed_successfully = os.path.exists(flag_file_path)

if 'replenishment_df' not in st.session_state: st.session_state.replenishment_df = None
if 'unmapped_df' not in st.session_state: st.session_state.unmapped_df = None
if 'data_processed_successfully' not in st.session_state: st.session_state.data_processed_successfully = False
if 'persisted_data_loaded_this_run' not in st.session_state:
    load_persisted_data_into_session()
    st.session_state.persisted_data_loaded_this_run = True

baserow_config = APP_CONFIG.get('baserow', {})
cache_config = APP_CONFIG.get('cache', {})
platforms_config = APP_CONFIG.get('platforms', [])
replenishment_config = APP_CONFIG.get('replenishment_table', {})
feature_date_format = replenishment_config.get('feature_date_format', "%d-%b-%Y")
platforms_needing_two_files_for_60day = ["meesho", "flipkart"] # Centralize this

@st.cache_resource
def get_data_tools():
    # ... (get_data_tools function remains the same) ...
    logger.info("Initializing BaserowFetcher and SKUMapper...")
    try:
        fetcher = BaserowFetcher(api_token=baserow_config.get('api_token'), base_url=baserow_config.get('base_url'))
        project_root_for_mapper = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sku_mapper = SKUMapper(baserow_fetcher=fetcher, sku_mapping_table_id=baserow_config.get('sku_mapping_table_id'),
                               combo_sku_table_id=baserow_config.get('combo_sku_table_id'), cache_config=cache_config,
                               project_root_dir=project_root_for_mapper, force_refresh_cache=st.session_state.get('force_refresh_baserow_cache', False))
        return fetcher, sku_mapper
    except Exception as e: logger.error(f"Error initializing data tools: {e}", exc_info=True); st.error(f"Error initializing data sources: {e}"); return None, None
fetcher, sku_mapper = get_data_tools()
if not fetcher or not sku_mapper: st.error("Could not initialize data processing tools."); st.stop()

col1, col3 = st.columns([2,1])
with col1: process_button_clicked = st.button("🚀 Process Data and Generate Table", key="process_data", use_container_width=True)
st.sidebar.checkbox("Force refresh Baserow cache", key="force_refresh_baserow_cache", help="Force refresh Baserow cache. Re-click 'Process Data'.")
with col3:
    if st.button("🔄 Reset All Data & Uploads", key="reset_all", type="primary", use_container_width=True):
        st.session_state.replenishment_df = None; st.session_state.unmapped_df = None
        st.session_state.data_processed_successfully = False
        if 'uploaded_file_paths' in st.session_state: st.session_state.uploaded_file_paths = {}
        clear_uploaded_data_folders(APP_CONFIG)
        delete_session_dataframe(REPLENISHMENT_DF_FILENAME, project_root)
        delete_session_dataframe(UNMAPPED_DF_FILENAME, project_root)
        flag_file_path = os.path.join(project_root, ".session_data", f"{DATA_PROCESSED_FLAG_FILENAME}.flag")
        if os.path.exists(flag_file_path): os.remove(flag_file_path)
        st.success("All data reset."); st.rerun()

st.info("This page processes uploaded sales data and inventory to generate the replenishment table.")
st.subheader("Uploaded Sales Data to be Processed:")
files_to_process_summary = []
at_least_one_file_pair_or_single_60day = False # Flag to enable process button

for platform_conf in platforms_config:
    p_slug = platform_conf['slug']; p_name = platform_conf['name']
    for acc_conf in platform_conf['accounts']:
        a_slug = acc_conf['slug']; a_name = acc_conf['name']
        
        # 30-day file
        key_30day = f"{p_slug}_{a_slug}_30day"
        file_30day_path_session = st.session_state.get('uploaded_file_paths', {}).get(key_30day)
        file_30day_path_disk = get_uploaded_sales_files(p_slug, a_slug, "30day", APP_CONFIG)
        file_30day_path = file_30day_path_session or file_30day_path_disk

        # 60-day file(s)
        is_two_part_60day = p_slug.lower() in platforms_needing_two_files_for_60day
        
        file_60day_m1_path, file_60day_m2_path, single_file_60day_path = None, None, None
        
        if is_two_part_60day:
            key_60day_m1 = f"{p_slug}_{a_slug}_60day_month1"
            key_60day_m2 = f"{p_slug}_{a_slug}_60day_month2"
            file_60day_m1_path_session = st.session_state.get('uploaded_file_paths', {}).get(key_60day_m1)
            file_60day_m2_path_session = st.session_state.get('uploaded_file_paths', {}).get(key_60day_m2)
            
            disk_files_60day = get_uploaded_sales_files(p_slug, a_slug, "60day", APP_CONFIG) # This now returns a dict for these platforms
            file_60day_m1_path_disk = disk_files_60day.get('month1') if isinstance(disk_files_60day, dict) else None
            file_60day_m2_path_disk = disk_files_60day.get('month2') if isinstance(disk_files_60day, dict) else None

            file_60day_m1_path = file_60day_m1_path_session or file_60day_m1_path_disk
            file_60day_m2_path = file_60day_m2_path_session or file_60day_m2_path_disk
        else:
            key_60day_single = f"{p_slug}_{a_slug}_60day"
            single_file_60day_path_session = st.session_state.get('uploaded_file_paths', {}).get(key_60day_single)
            single_file_60day_path_disk = get_uploaded_sales_files(p_slug, a_slug, "60day", APP_CONFIG) # Returns single path
            single_file_60day_path = single_file_60day_path_session or single_file_60day_path_disk

        # Build summary and check if processing is possible
        summary_30 = f"30-day ({os.path.basename(file_30day_path)})" if file_30day_path else "30-day (MISSING)"
        summary_60 = ""
        
        can_process_this_account = False
        if file_30day_path: # 30-day is always needed
            if is_two_part_60day:
                s_m1 = f"60d-M1 ({os.path.basename(file_60day_m1_path)})" if file_60day_m1_path else "60d-M1 (MISSING)"
                s_m2 = f"60d-M2 ({os.path.basename(file_60day_m2_path)})" if file_60day_m2_path else "60d-M2 (MISSING)"
                summary_60 = f"{s_m1}, {s_m2}"
                if file_60day_m1_path and file_60day_m2_path: # Both parts of 60-day needed
                    at_least_one_file_pair_or_single_60day = True
                    can_process_this_account = True
            else:
                summary_60 = f"60-day ({os.path.basename(single_file_60day_path)})" if single_file_60day_path else "60-day (MISSING)"
                if single_file_60day_path: # Single 60-day file needed
                    at_least_one_file_pair_or_single_60day = True
                    can_process_this_account = True
        
        files_to_process_summary.append(f"- {p_name} ({a_name}): {summary_30}, {summary_60}")


if not files_to_process_summary and st.session_state.replenishment_df is None :
    st.warning("No sales data uploaded. Please go to the 'Upload Sales Data' page.")
elif files_to_process_summary:
    for item in files_to_process_summary: st.markdown(item)

process_button_disabled = not at_least_one_file_pair_or_single_60day
if process_button_clicked and process_button_disabled:
    st.error("Required sales reports are missing for all accounts. Please upload them.")

def read_sales_data(file_path, sku_col_name_config):
    # ... (read_sales_data function remains the same) ...
    _, file_extension = os.path.splitext(file_path); df = None; dtype_spec = {sku_col_name_config: str} if sku_col_name_config else None
    try:
        if file_extension.lower() == ".csv": df = pd.read_csv(file_path, dtype=dtype_spec)
        elif file_extension.lower() == ".xlsx": df = pd.read_excel(file_path, dtype=dtype_spec, engine='openpyxl')
        else: st.error(f"Unsupported file format: {file_extension}"); return pd.DataFrame()
        return df
    except Exception as e: st.error(f"Error reading file {os.path.basename(file_path)}: {e}"); return pd.DataFrame()

def process_single_sales_file(file_path, sku_col_name, qty_cols, platform_name, account_name, report_desc, sales_dict, unmapped_list, processed_mskus_set, sku_mapper_instance):
    """Helper to process one sales file and update respective dictionaries."""
    if not file_path:
        logger.warning(f"No file path provided for {report_desc} of {platform_name} - {account_name}.")
        return
    logger.info(f"Processing {report_desc} for {platform_name} - {account_name} from {file_path}")
    try:
        df = read_sales_data(file_path, sku_col_name)
        if df.empty: logger.warning(f"{report_desc} data for {platform_name} - {account_name} is empty. Skipping."); return
        if sku_col_name not in df.columns: st.error(f"SKU col '{sku_col_name}' not in {report_desc} for {platform_name}-{account_name}."); return
        missing_q_cols = [q for q in qty_cols if q not in df.columns]
        if missing_q_cols: st.error(f"Missing Qty cols {missing_q_cols} in {report_desc} for {platform_name}-{account_name}."); return
        
        df['calculated_quantity'] = 0
        for q_col in qty_cols:
            if df[q_col].dtype == 'object': df[q_col] = df[q_col].astype(str).str.replace('₹', '', regex=False).str.replace(',', '', regex=False)
            df[q_col] = pd.to_numeric(df[q_col], errors='coerce').fillna(0)
            df['calculated_quantity'] += df[q_col]
        
        for _, row in df.iterrows():
            platform_sku = row[sku_col_name]; quantity = row['calculated_quantity']
            msku_result = sku_mapper_instance.map_sku_to_msku(platform_sku)
            if msku_result:
                mskus_to_update = [msku_result] if isinstance(msku_result, str) else msku_result
                for m_item in mskus_to_update:
                    if m_item: sales_dict[m_item] = sales_dict.get(m_item, 0) + quantity; processed_mskus_set.add(m_item)
            elif platform_sku and not pd.isna(platform_sku):
                unmapped_list.append({'Platform SKU': platform_sku, 'Platform': platform_name, 'Account': account_name, 'Report Type': report_desc})
    except Exception as e:
        st.error(f"Error processing {report_desc} for {platform_name}-{account_name}: {e}")
        logger.error(f"Error processing {report_desc} for {platform_name}-{account_name}: {e}", exc_info=True)


if process_button_clicked and not process_button_disabled:
    with st.spinner("Processing data... This may take a moment."):
        try:
            all_sales_data_30day = {}
            all_sales_data_60day = {}
            processed_mskus = set()
            unmapped_skus_collection = []
            current_date_for_code = datetime.now()
            fixed_generated_code = f"{current_date_for_code.strftime('%m-%d')}-001"
            logger.info("Starting sales data processing...")

            for platform_conf in platforms_config:
                p_slug = platform_conf['slug']; p_name = platform_conf['name']
                for acc_conf in platform_conf['accounts']:
                    a_slug = acc_conf['slug']; a_name = acc_conf['name']
                    sku_col_name = acc_conf.get('sku_column') or platform_conf.get('sku_column', APP_CONFIG.get('default_sales_report_columns',{}).get('sku_column','SKU'))
                    actual_qty_cols = []; qty_cols_list_acc = acc_conf.get('quantity_columns')
                    if qty_cols_list_acc and isinstance(qty_cols_list_acc, list): actual_qty_cols = qty_cols_list_acc
                    else:
                        qty_col_acc = acc_conf.get('quantity_column')
                        if qty_col_acc and isinstance(qty_col_acc, str): actual_qty_cols = [qty_col_acc]
                        else: default_qty_col = APP_CONFIG.get('default_sales_report_columns',{}).get('quantity_column','Quantity'); actual_qty_cols = [default_qty_col] if default_qty_col else []
                    if not actual_qty_cols: st.error(f"Qty col(s) not configured for {p_name}-{a_name}."); continue

                    # Get 30-day file path
                    key_30day = f"{p_slug}_{a_slug}_30day"
                    file_30day_path = st.session_state.get('uploaded_file_paths', {}).get(key_30day) or \
                                      get_uploaded_sales_files(p_slug, a_slug, "30day", APP_CONFIG)
                    process_single_sales_file(file_30day_path, sku_col_name, actual_qty_cols, p_name, a_name, "30-Day Sales",
                                              all_sales_data_30day, unmapped_skus_collection, processed_mskus, sku_mapper)

                    # Get 60-day file path(s)
                    is_two_part_60day = p_slug.lower() in platforms_needing_two_files_for_60day
                    paths_60day = get_uploaded_sales_files(p_slug, a_slug, "60day", APP_CONFIG)

                    if is_two_part_60day:
                        if isinstance(paths_60day, dict):
                            path_m1 = paths_60day.get('month1')
                            path_m2 = paths_60day.get('month2')
                            if not path_m1 and not path_m2: logger.warning(f"Both 60-day month files missing for {p_name}-{a_name}."); continue
                            
                            temp_sales_60_m1 = {}
                            temp_sales_60_m2 = {}
                            process_single_sales_file(path_m1, sku_col_name, actual_qty_cols, p_name, a_name, "60-Day Sales (Recent M)",
                                                      temp_sales_60_m1, unmapped_skus_collection, processed_mskus, sku_mapper)
                            process_single_sales_file(path_m2, sku_col_name, actual_qty_cols, p_name, a_name, "60-Day Sales (Prev M)",
                                                      temp_sales_60_m2, unmapped_skus_collection, processed_mskus, sku_mapper)
                            
                            # Combine month1 and month2 for this account
                            all_mskus_for_60day_calc = set(temp_sales_60_m1.keys()) | set(temp_sales_60_m2.keys())
                            for msku_item in all_mskus_for_60day_calc:
                                all_sales_data_60day[msku_item] = all_sales_data_60day.get(msku_item, 0) + \
                                                                temp_sales_60_m1.get(msku_item, 0) + \
                                                                temp_sales_60_m2.get(msku_item, 0)
                        else: logger.warning(f"Expected two 60-day files for {p_name}-{a_name}, but did not receive them in correct format.")
                    elif paths_60day: # Single 60-day file
                        process_single_sales_file(paths_60day, sku_col_name, actual_qty_cols, p_name, a_name, "60-Day Sales",
                                                  all_sales_data_60day, unmapped_skus_collection, processed_mskus, sku_mapper)
                    else:
                        logger.warning(f"60-day sales file(s) missing for {p_name}-{a_name}.")
            
            # ... (Inventory fetching and final table construction logic remains the same) ...
            inventory_df = pd.DataFrame(); final_data = [] # Simplified for brevity
            if processed_mskus:
                inventory_table_id = baserow_config.get('inventory_table_id')
                if inventory_table_id:
                    raw_inventory_df = fetcher.get_inventory_data(inventory_table_id)
                    if not raw_inventory_df.empty and 'MSKU' in raw_inventory_df.columns:
                        inventory_df = raw_inventory_df[raw_inventory_df['MSKU'].isin(list(processed_mskus))].copy()
            current_date_formatted = datetime.now().strftime(feature_date_format)
            all_mskus_for_table = set(inventory_df['MSKU'].tolist()) | processed_mskus
            if not all_mskus_for_table: st.warning("No MSKUs to process for the final table.")
            for msku_loop_var in sorted(list(all_mskus_for_table)):
                inv_row = inventory_df[inventory_df['MSKU'] == msku_loop_var]
                current_inventory = inv_row['Current Inventory'].iloc[0] if not inv_row.empty else 0
                images, category = "", ""; sales_30 = all_sales_data_30day.get(msku_loop_var, 0); sales_60 = all_sales_data_60day.get(msku_loop_var, 0)
                required_qty = math.ceil(sales_60 * 1.20); buffer_stock = math.ceil(required_qty * 0.20); marketing_stock = buffer_stock
                final_order_qty = required_qty + buffer_stock + marketing_stock
                shipment_status = "Sea" if current_inventory >= sales_30 else "Air"
                product_status, po_no, order_status, notes = "Fast Moving", "KI1589559", "", ""
                final_data.append({"Feature": current_date_formatted, "Msku": msku_loop_var, "Images": images, "Category": category, "Code": fixed_generated_code, "Current Inventory": current_inventory, "30 days sales": sales_30, "60 days sales": sales_60, "Required Qty": required_qty, "Buffer Stock": buffer_stock, "Marketing Stock": marketing_stock, "Final Order Qty": final_order_qty, "Shipment Status": shipment_status, "Product Status": product_status, "Po No.": po_no, "Order Status": order_status, "Notes": notes})


            # Store results in session state AND save to disk
            if final_data:
                df_to_save = pd.DataFrame(final_data)
                st.session_state.replenishment_df = df_to_save
                save_session_dataframe(df_to_save, REPLENISHMENT_DF_FILENAME, project_root)
                st.session_state.data_processed_successfully = True
                flag_file_path = os.path.join(project_root, ".session_data", f"{DATA_PROCESSED_FLAG_FILENAME}.flag")
                os.makedirs(os.path.dirname(flag_file_path), exist_ok=True)
                with open(flag_file_path, 'w') as f: f.write('processed')
            else:
                st.session_state.replenishment_df = None; delete_session_dataframe(REPLENISHMENT_DF_FILENAME, project_root)
                st.session_state.data_processed_successfully = True
                flag_file_path = os.path.join(project_root, ".session_data", f"{DATA_PROCESSED_FLAG_FILENAME}.flag")
                if os.path.exists(flag_file_path): os.remove(flag_file_path)

            if unmapped_skus_collection:
                unmapped_df_temp = pd.DataFrame(unmapped_skus_collection)
                unmapped_df_temp.drop_duplicates(subset=['Platform SKU', 'Platform', 'Account', 'Report Type'], inplace=True) # Added Report Type to drop_duplicates
                st.session_state.unmapped_df = unmapped_df_temp
                save_session_dataframe(unmapped_df_temp, UNMAPPED_DF_FILENAME, project_root)
            else:
                st.session_state.unmapped_df = None; delete_session_dataframe(UNMAPPED_DF_FILENAME, project_root)
            
            logger.info("Replenishment table generation process completed."); st.success("Data processing complete!"); st.rerun()
        except Exception as e:
            logger.error(f"Error during replenishment table generation: {e}", exc_info=True); st.error(f"An error occurred: {e}")
            st.session_state.replenishment_df = None; st.session_state.unmapped_df = None; st.session_state.data_processed_successfully = False
            delete_session_dataframe(REPLENISHMENT_DF_FILENAME, project_root); delete_session_dataframe(UNMAPPED_DF_FILENAME, project_root)
            flag_file_path = os.path.join(project_root, ".session_data", f"{DATA_PROCESSED_FLAG_FILENAME}.flag");
            if os.path.exists(flag_file_path): os.remove(flag_file_path)

# --- Display Logic (uses session state) ---
# ... (Display logic remains largely the same, using st.data_editor for replenishment_df) ...
if st.session_state.get('replenishment_df') is not None:
    st.subheader("Generated Replenishment Table (Editable)")
    column_config = get_replenishment_table_column_config()
    # Pass a copy to data_editor to ensure edits don't directly mutate the persisted session state
    # until explicitly saved back.
    df_for_editing = st.session_state.replenishment_df.copy()
    edited_df = st.data_editor(df_for_editing, column_config=column_config, num_rows="dynamic",
                               use_container_width=True, height=600, key="replenishment_editor")
    if not edited_df.equals(st.session_state.replenishment_df): # Check against the original in session
        st.session_state.replenishment_df = edited_df # Update session state with edits
        save_session_dataframe(edited_df, REPLENISHMENT_DF_FILENAME, project_root) # Persist edits
        # st.info("Changes saved to session and disk.") # Optional feedback

    if st.session_state.replenishment_df is not None and not st.session_state.replenishment_df.empty:
        csv_repl = st.session_state.replenishment_df.to_csv(index=False).encode('utf-8')
        st.download_button(label="Download Edited Table as CSV", data=csv_repl,
                           file_name=f"edited_replenishment_table_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv",
                           mime="text/csv", key="download_edited_repl_table")
elif st.session_state.data_processed_successfully and st.session_state.replenishment_df is None:
    st.warning("Processing complete, but no data to display in the replenishment table.")

if st.session_state.get('unmapped_df') is not None and not st.session_state.unmapped_df.empty:
    st.subheader("⚠️ Unmapped SKUs")
    st.warning(f"Found {len(st.session_state.unmapped_df)} unique platform SKUs that could not be mapped.")
    st.dataframe(st.session_state.unmapped_df)
    unmapped_csv = st.session_state.unmapped_df.to_csv(index=False).encode('utf-8')
    st.download_button(label="Download Unmapped SKUs as CSV", data=unmapped_csv,
                       file_name=f"unmapped_skus_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv",
                       mime="text/csv", key="download_unmapped_table")
elif st.session_state.data_processed_successfully and (st.session_state.unmapped_df is None or st.session_state.unmapped_df.empty):
    if st.session_state.data_processed_successfully :
         st.success("✅ All SKUs from processed sales reports were successfully mapped (or no SKUs needed mapping).")

logger.info("Generate Replenishment Table page loaded.")
if 'persisted_data_loaded_this_run' in st.session_state: del st.session_state.persisted_data_loaded_this_run