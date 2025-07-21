# RMS/pages/07_Settings_Editor.py
import streamlit as st
import pandas as pd
import os
import sys
import copy
import yaml

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG, save_app_config
from utils.gspread_client import get_gspread_client, update_settings_in_gsheet

st.set_page_config(page_title="Settings Editor - RMS", layout="wide")
st.title("⚙️ Settings Editor")

st.info("""
This page allows you to configure the application.
- **Baserow Table IDs** are saved instantly to a Google Sheet and are used by the live app.
- **All other settings** are saved to the `settings.yaml` file and require an application restart to take effect.
""")


gsheet_client = get_gspread_client()
# --- Initialize Tools ---
gsheet_settings = APP_CONFIG.get("google_sheet_settings", {})
spreadsheet_id = gsheet_settings.get("spreadsheet_id")
worksheet_name = gsheet_settings.get("worksheet_name")

# Use session state to hold the editable version of the config
if 'editable_config' not in st.session_state:
    st.session_state.editable_config = copy.deepcopy(APP_CONFIG)

config = st.session_state.editable_config

# --- Save Button Logic ---
if st.button("Save All Settings", type="primary"):
    # 1. Prepare the dictionary of table IDs to update in Google Sheets
    table_ids_to_update = {
        "sku_mapping_table_id": st.session_state.bs_sku_mapping_id,
        "combo_sku_table_id": st.session_state.bs_combo_sku_id,
        "inventory_table_id": st.session_state.bs_inventory_id,
        "processed_sales_data_table_id": st.session_state.bs_processed_sales_id,
        "category_table_id": st.session_state.bs_category_id,
        "purchase_orders_table_id": st.session_state.bs_po_id,
        "po_intake_log_table_id": st.session_state.bs_po_intake_id,
        "automated_outbound_table_id": st.session_state.bs_outbound_id,
        "packaging_inventory_table_id": st.session_state.bs_packaging_inv_id,
        "catalogue_table_id": st.session_state.bs_catalogue_id
    }
    
    # 2. Prepare the dictionary of static settings to save to YAML
    # Start with a deepcopy of the current state of the editable config
    config_to_save_yaml = copy.deepcopy(st.session_state.editable_config)
    
    # Update the static parts from the widgets
    config_to_save_yaml['baserow']['base_url'] = st.session_state.bs_base_url
    config_to_save_yaml['baserow']['api_token'] = st.session_state.bs_api_token
    
    try:
        warehouse_list = yaml.safe_load(st.session_state.bs_warehouse_cols)
        if isinstance(warehouse_list, list):
            config_to_save_yaml['baserow']['inventory_warehouse_columns'] = warehouse_list
        else:
            st.error("Warehouse Columns input is not a valid YAML list. Save aborted.")
            st.stop()
    except yaml.YAMLError as e:
        st.error(f"Error parsing Warehouse Columns YAML: {e}. Save aborted."); st.stop()

    config_to_save_yaml['cache']['expiry_days'] = st.session_state.cache_expiry
    config_to_save_yaml['logging']['level'] = st.session_state.log_level
    config_to_save_yaml['data_paths']['uploaded_sales_root'] = st.session_state.dp_sales_root
    config_to_save_yaml['replenishment_table']['feature_date_format'] = st.session_state.rt_date_format
    
    # Remove the dynamic table IDs from the YAML save data, as they are managed in GSheet
    for key in table_ids_to_update.keys():
        if key in config_to_save_yaml['baserow']:
            del config_to_save_yaml['baserow'][key]
            
    # 3. Perform the save operations
    with st.spinner("Saving settings..."):
        gsheet_success = update_settings_in_gsheet(gsheet_client, spreadsheet_id, worksheet_name, table_ids_to_update)
        yaml_success = save_app_config(config_to_save_yaml)

    if gsheet_success and yaml_success:
        st.success("Settings saved successfully!")
        
        # --- THIS IS THE CRUCIAL FIX ---
        # Manually update the live APP_CONFIG and the session state copy with the new values
        for key, value in table_ids_to_update.items():
            APP_CONFIG['baserow'][key] = value
            st.session_state.editable_config['baserow'][key] = value
        
        # Clear the main config cache to force a full reload on the next session
        st.cache_data.clear()
        st.cache_resource.clear()
        
        st.warning("Settings have been saved. A full page refresh (F5) or app restart is recommended to ensure all components use the new configuration.")
        # We can trigger a rerun to at least update the current page's view
        st.rerun()
        # --- END FIX ---
        
    else:
        st.error("Failed to save some or all settings. Check logs for details.")

st.divider()

# --- Baserow Settings ---
with st.expander("Baserow Settings", expanded=True):
    st.subheader("Static Settings (Saved to `settings.yaml`)")
    st.text_input("Baserow Base URL", key="bs_base_url", value=config.get('baserow', {}).get('base_url', ''))
    st.text_input("Baserow API Token", key="bs_api_token", value=config.get('baserow', {}).get('api_token', ''), type="password")
    
    st.markdown("---")
    st.subheader("Dynamic Table IDs (Saved to Google Sheet)")
    st.info("These values are loaded from your Google Sheet and can be updated here instantly for the deployed app.")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.number_input("SKU Mapping Table ID", key="bs_sku_mapping_id", value=int(config.get('baserow', {}).get('sku_mapping_table_id', 0)), step=1, format="%d")
        st.number_input("Combo SKU Table ID", key="bs_combo_sku_id", value=int(config.get('baserow', {}).get('combo_sku_table_id', 0)), step=1, format="%d")
        st.number_input("Inventory Table ID", key="bs_inventory_id", value=int(config.get('baserow', {}).get('inventory_table_id', 0)), step=1, format="%d")
    with col2:
        st.number_input("Processed Sales Data Table ID", key="bs_processed_sales_id", value=int(config.get('baserow', {}).get('processed_sales_data_table_id', 0)), step=1, format="%d")
        st.number_input("Category/Product Table ID", key="bs_category_id", value=int(config.get('baserow', {}).get('category_table_id', 0)), step=1, format="%d")
        st.number_input("Purchase Orders Table ID", key="bs_po_id", value=int(config.get('baserow', {}).get('purchase_orders_table_id', 0)), step=1, format="%d")
    with col3:
        st.number_input("PO Intake Log Table ID", key="bs_po_intake_id", value=int(config.get('baserow', {}).get('po_intake_log_table_id', 0)), step=1, format="%d")
        st.number_input("Automated Outbound Table ID", key="bs_outbound_id", value=int(config.get('baserow', {}).get('automated_outbound_table_id', 0)), step=1, format="%d")
        st.number_input("Packaging Inventory Table ID", key="bs_packaging_inv_id", value=int(config.get('baserow', {}).get('packaging_inventory_table_id', 0)), step=1, format="%d")
        st.number_input("Catalogue Table ID", key="bs_catalogue_id", value=int(config.get('baserow', {}).get('catalogue_table_id', 0)), step=1, format="%d")

    st.markdown("---")
    st.subheader("Other Baserow Settings (Saved to `settings.yaml`)")
    warehouse_cols_str = yaml.dump(config.get('baserow', {}).get('inventory_warehouse_columns', []), default_flow_style=False)
    st.text_area("Inventory Warehouse Columns (YAML format):", key="bs_warehouse_cols", value=warehouse_cols_str, height=250,
                 help="Enter each column name on a new line, preceded by a hyphen and a space (e.g., '- TLCQ').")

# --- Platform & Account Settings (Saved to `settings.yaml`) ---
with st.expander("Platform & Account Settings", expanded=False):
    # ... (This section remains the same as before, as it edits the config dict which is then saved to YAML) ...
    st.info("Edit platform names, slugs, and account details here. To add or remove a platform/account, you must edit the `settings.yaml` file directly for now.")
    platforms_list = config.get('platforms', [])
    editable_accounts_data = []
    for p_idx, platform in enumerate(platforms_list):
        for a_idx, account in enumerate(platform.get('accounts', [])):
            editable_accounts_data.append({
                "platform_idx": p_idx, "account_idx": a_idx,
                "Platform Name": platform.get('name'), "Platform Slug": platform.get('slug'),
                "Account Name": account.get('name'), "Account Slug": account.get('slug'),
                "SKU Column": account.get('sku_column'),
                "Quantity Column(s)": ", ".join(account.get('quantity_columns', [])) if 'quantity_columns' in account else account.get('quantity_column', '')
            })
    if editable_accounts_data:
        accounts_df = pd.DataFrame(editable_accounts_data)
        edited_accounts_df = st.data_editor(accounts_df, column_config={"platform_idx": None, "account_idx": None, "Platform Name": st.column_config.TextColumn(disabled=True), "Platform Slug": st.column_config.TextColumn(disabled=True)}, hide_index=True, use_container_width=True, key="platform_editor")
        if not accounts_df.equals(edited_accounts_df):
            new_platforms_config = copy.deepcopy(config['platforms'])
            for index, row in edited_accounts_df.iterrows():
                p_idx, a_idx = row['platform_idx'], row['account_idx']
                target_account = new_platforms_config[p_idx]['accounts'][a_idx]
                target_account['name'] = row['Account Name']; target_account['slug'] = row['Account Slug']; target_account['sku_column'] = row['SKU Column']
                qty_str = row["Quantity Column(s)"]
                if ',' in qty_str:
                    target_account['quantity_columns'] = [s.strip() for s in qty_str.split(',')];
                    if 'quantity_column' in target_account: del target_account['quantity_column']
                else:
                    target_account['quantity_column'] = qty_str.strip();
                    if 'quantity_columns' in target_account: del target_account['quantity_columns']
            config['platforms'] = new_platforms_config
            st.info("Platform changes are staged. Click 'Save All Settings' at the top to apply.")

# --- Other Application Settings (Saved to `settings.yaml`) ---
with st.expander("Other Application Settings", expanded=False):
    # ... (This section remains the same) ...
    col_other1, col_other2 = st.columns(2)
    with col_other1:
        st.subheader("Cache & Logging")
        st.number_input("Cache Expiry (days)", key="cache_expiry", value=config.get('cache', {}).get('expiry_days', 5), min_value=1, step=1)
        st.selectbox("Logging Level", key="log_level", options=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], index=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"].index(config.get('logging', {}).get('level', 'INFO')))
    with col_other2:
        st.subheader("Data & Formatting")
        st.text_input("Uploaded Sales Root Directory", key="dp_sales_root", value=config.get('data_paths', {}).get('uploaded_sales_root', 'uploaded_data'))
        st.text_input("Replenishment Date Format", key="rt_date_format", value=config.get('replenishment_table', {}).get('feature_date_format', '%d-%b-%Y'), help="Uses Python's strftime format codes. E.g., %Y-%m-%d or %d-%b-%Y.")