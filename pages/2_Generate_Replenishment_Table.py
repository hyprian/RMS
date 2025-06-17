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
from utils.file_utils import get_uploaded_sales_files
import logging

logger = logging.getLogger(__name__)

st.set_page_config(page_title="Generate Replenishment Table - RMS", layout="wide")
st.title("📋 Generate Replenishment Table")

# ... (Configuration and Initialization - get_data_tools, etc. - remain the same) ...
baserow_config = APP_CONFIG.get('baserow', {})
cache_config = APP_CONFIG.get('cache', {})
platforms_config = APP_CONFIG.get('platforms', [])
replenishment_config = APP_CONFIG.get('replenishment_table', {})
feature_date_format = replenishment_config.get('feature_date_format', "%d-%b-%Y")

@st.cache_resource
def get_data_tools():
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

st.sidebar.checkbox("Force refresh Baserow cache", key="force_refresh_baserow_cache",
                    help="If checked, SKU/Combo mappings will be fetched fresh from Baserow, ignoring local cache.")
st.info("This page will process the uploaded sales data and inventory to generate the replenishment table.")

st.subheader("Uploaded Sales Data to be Processed:")
files_to_process_summary = []
at_least_one_file_pair = False
for platform_conf in platforms_config:
    p_slug = platform_conf['slug']
    for acc_conf in platform_conf['accounts']:
        a_slug = acc_conf['slug']
        key_30day = f"{p_slug}_{a_slug}_30day"
        file_30day_path = st.session_state.uploaded_file_paths.get(key_30day) or \
                          get_uploaded_sales_files(p_slug, a_slug, "30day", APP_CONFIG)
        key_60day = f"{p_slug}_{a_slug}_60day"
        file_60day_path = st.session_state.uploaded_file_paths.get(key_60day) or \
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
if not files_to_process_summary:
    st.warning("No sales data uploaded yet or files not found. Please go to the 'Upload Sales Data' page.")
else:
    for item in files_to_process_summary:
        st.markdown(item)
if not at_least_one_file_pair:
    st.error("At least one platform account needs both 30-day and 60-day sales reports uploaded to proceed.")
    st.stop()


def read_sales_data(file_path, sku_col_name_config):
    """Reads sales data from CSV or XLSX file."""
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
            return pd.DataFrame() # Return empty DataFrame
        return df
    except Exception as e:
        st.error(f"Error reading file {os.path.basename(file_path)}: {e}")
        logger.error(f"Error reading file {file_path}: {e}", exc_info=True)
        return pd.DataFrame() # Return empty DataFrame on error


if st.button("🚀 Process Data and Generate Table", disabled=not at_least_one_file_pair):
    with st.spinner("Processing data... This may take a moment."):
        try:
            all_sales_data_30day = {}
            all_sales_data_60day = {}
            processed_mskus = set()

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
                        logger.error(f"Quantity column(s) not configured for {p_name} - {a_name}.")
                        continue

                    key_30day = f"{p_slug}_{a_slug}_30day"
                    file_30day_path = st.session_state.uploaded_file_paths.get(key_30day) or \
                                      get_uploaded_sales_files(p_slug, a_slug, "30day", APP_CONFIG)
                    key_60day = f"{p_slug}_{a_slug}_60day"
                    file_60day_path = st.session_state.uploaded_file_paths.get(key_60day) or \
                                      get_uploaded_sales_files(p_slug, a_slug, "60day", APP_CONFIG)

                    if not file_30day_path or not file_60day_path:
                        logger.warning(f"Skipping {p_name} - {a_name} due to missing one or both sales reports.")
                        continue
                    
                    # --- Process 30-day sales ---
                    logger.info(f"Processing 30-day sales for {p_name} - {a_name} from {file_30day_path} using SKU col '{sku_col_name}' and Qty cols {actual_qty_cols}")
                    try:
                        # Use the new read_sales_data function
                        sales_df_30 = read_sales_data(file_30day_path, sku_col_name)
                        if sales_df_30.empty:
                            logger.warning(f"30-day sales data for {p_name} - {a_name} is empty after reading. Skipping.")
                            continue

                        if sku_col_name not in sales_df_30.columns:
                            st.error(f"SKU column '{sku_col_name}' not found in 30-day report for {p_name} - {a_name}. Found: {sales_df_30.columns.tolist()}")
                            logger.error(f"SKU column error in 30-day report for {p_name} - {a_name}.")
                            continue
                        
                        missing_qty_cols = [q_col for q_col in actual_qty_cols if q_col not in sales_df_30.columns]
                        if missing_qty_cols:
                            st.error(f"Missing quantity columns {missing_qty_cols} in 30-day report for {p_name} - {a_name}. Required: {actual_qty_cols}. Found: {sales_df_30.columns.tolist()}")
                            logger.error(f"Quantity column error in 30-day report for {p_name} - {a_name}.")
                            continue
                        
                        sales_df_30['calculated_quantity'] = 0
                        for q_col in actual_qty_cols:
                            if sales_df_30[q_col].dtype == 'object':
                                sales_df_30[q_col] = sales_df_30[q_col].astype(str).str.replace('₹', '', regex=False).str.replace(',', '', regex=False)
                            sales_df_30[q_col] = pd.to_numeric(sales_df_30[q_col], errors='coerce').fillna(0)
                            sales_df_30['calculated_quantity'] += sales_df_30[q_col]

                        for _, row in sales_df_30.iterrows():
                            platform_sku = row[sku_col_name]
                            quantity = row['calculated_quantity']
                            msku_result = sku_mapper.map_sku_to_msku(platform_sku)
                            if msku_result:
                                mskus_to_update = [msku_result] if isinstance(msku_result, str) else msku_result
                                for msku in mskus_to_update:
                                    if msku:
                                        all_sales_data_30day[msku] = all_sales_data_30day.get(msku, 0) + quantity
                                        processed_mskus.add(msku)
                    except Exception as e:
                        st.error(f"Error processing 30-day report for {p_name} - {a_name}: {e}")
                        logger.error(f"Error processing 30-day report for {p_name} - {a_name}: {e}", exc_info=True)

                    # --- Process 60-day sales ---
                    logger.info(f"Processing 60-day sales for {p_name} - {a_name} from {file_60day_path} using SKU col '{sku_col_name}' and Qty cols {actual_qty_cols}")
                    try:
                        # Use the new read_sales_data function
                        sales_df_60 = read_sales_data(file_60day_path, sku_col_name)
                        if sales_df_60.empty:
                            logger.warning(f"60-day sales data for {p_name} - {a_name} is empty after reading. Skipping.")
                            continue
                            
                        if sku_col_name not in sales_df_60.columns:
                            st.error(f"SKU column '{sku_col_name}' not found in 60-day report for {p_name} - {a_name}. Found: {sales_df_60.columns.tolist()}")
                            logger.error(f"SKU column error in 60-day report for {p_name} - {a_name}.")
                            continue

                        missing_qty_cols_60 = [q_col for q_col in actual_qty_cols if q_col not in sales_df_60.columns]
                        if missing_qty_cols_60:
                            st.error(f"Missing quantity columns {missing_qty_cols_60} in 60-day report for {p_name} - {a_name}. Required: {actual_qty_cols}. Found: {sales_df_60.columns.tolist()}")
                            logger.error(f"Quantity column error in 60-day report for {p_name} - {a_name}.")
                            continue
                        
                        sales_df_60['calculated_quantity'] = 0
                        for q_col in actual_qty_cols:
                            if sales_df_60[q_col].dtype == 'object':
                                sales_df_60[q_col] = sales_df_60[q_col].astype(str).str.replace('₹', '', regex=False).str.replace(',', '', regex=False)
                            sales_df_60[q_col] = pd.to_numeric(sales_df_60[q_col], errors='coerce').fillna(0)
                            sales_df_60['calculated_quantity'] += sales_df_60[q_col]
                        
                        for _, row in sales_df_60.iterrows():
                            platform_sku = row[sku_col_name]
                            quantity = row['calculated_quantity']
                            msku_result = sku_mapper.map_sku_to_msku(platform_sku)
                            if msku_result:
                                mskus_to_update = [msku_result] if isinstance(msku_result, str) else msku_result
                                for msku in mskus_to_update:
                                     if msku:
                                        all_sales_data_60day[msku] = all_sales_data_60day.get(msku, 0) + quantity
                                        processed_mskus.add(msku)
                    except Exception as e:
                        st.error(f"Error processing 60-day report for {p_name} - {a_name}: {e}")
                        logger.error(f"Error processing 60-day report for {p_name} - {a_name}: {e}", exc_info=True)
            
            # ... (rest of the code for fetching inventory and generating final table remains the same) ...
            logger.info(f"Total unique MSKUs after sales processing: {len(processed_mskus)}")

            inventory_df = pd.DataFrame()
            if processed_mskus:
                logger.info(f"Fetching inventory data for {len(processed_mskus)} MSKUs...")
                inventory_table_id = baserow_config.get('inventory_table_id')
                if inventory_table_id:
                    raw_inventory_df = fetcher.get_inventory_data(inventory_table_id)
                    if not raw_inventory_df.empty and 'MSKU' in raw_inventory_df.columns:
                        inventory_df = raw_inventory_df[raw_inventory_df['MSKU'].isin(list(processed_mskus))].copy()
                        logger.info(f"Fetched inventory for {len(inventory_df)} matching MSKUs.")
                    else:
                        logger.warning("Inventory data from Baserow is empty or missing 'MSKU' column after processing by fetcher.")
                        st.warning("Could not fetch or process inventory data from Baserow correctly.")
                else:
                    st.error("Inventory Table ID not configured in settings.yaml.")
                    logger.error("Inventory Table ID not configured.")
            else:
                st.warning("No MSKUs found after processing sales data. Cannot fetch inventory.")

            final_data = []
            current_date = datetime.now()
            current_date_formatted = current_date.strftime(feature_date_format)
            code_counter = 1
            all_mskus_for_table = set(inventory_df['MSKU'].tolist()) | processed_mskus
            if not all_mskus_for_table:
                 st.warning("No MSKUs to process for the final table (neither from sales nor inventory).")

            for msku in sorted(list(all_mskus_for_table)):
                inv_row = inventory_df[inventory_df['MSKU'] == msku]
                current_inventory = inv_row['Current Inventory'].iloc[0] if not inv_row.empty else 0
                images = "" 
                category = "" 
                generated_code = f"{current_date.strftime('%m-%d')}-{str(code_counter).zfill(3)}"
                code_counter += 1
                sales_30 = all_sales_data_30day.get(msku, 0)
                sales_60 = all_sales_data_60day.get(msku, 0)
                required_qty = math.ceil(sales_60 * 1.20)
                buffer_stock = math.ceil(required_qty * 0.20)
                marketing_stock = buffer_stock
                final_order_qty = required_qty + buffer_stock + marketing_stock
                shipment_status = "Sea" if current_inventory >= sales_30 else "Air"
                product_status = "Fast Moving" 
                po_no = "KI1589559" 
                order_status = "" 
                notes = ""
                final_data.append({
                    "Feature": current_date_formatted, "Msku": msku, "Images": images,
                    "Category": category, "Code": generated_code, "Current Inventory": current_inventory,
                    "30 days sales": sales_30, "60 days sales": sales_60, "Required Qty": required_qty,
                    "Buffer Stock": buffer_stock, "Marketing Stock": marketing_stock,
                    "Final Order Qty": final_order_qty, "Shipment Status": shipment_status,
                    "Product Status": product_status, "Po No.": po_no, "Order Status": order_status, "Notes": notes
                })
            
            if final_data:
                replenishment_df = pd.DataFrame(final_data)
                st.subheader("Generated Replenishment Table (Preview)")
                st.dataframe(replenishment_df, height=600)
                csv = replenishment_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Replenishment Table as CSV", data=csv,
                    file_name=f"replenishment_table_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv", mime="text/csv",
                )
                st.session_state.replenishment_df = replenishment_df
            else:
                st.warning("No data to display in the replenishment table.")
            logger.info("Replenishment table generation process completed.")
            st.success("Replenishment table generated successfully!")
        except Exception as e:
            logger.error(f"Error during replenishment table generation: {e}", exc_info=True)
            st.error(f"An error occurred: {e}")
logger.info("Generate Replenishment Table page loaded.")