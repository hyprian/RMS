# RMS/pages/07_Settings_Editor.py
import streamlit as st
import pandas as pd
import os
import sys
import copy # To deepcopy the config for editing
import yaml # To handle list conversion from text area

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG, save_app_config

st.set_page_config(page_title="Settings Editor - RMS", layout="wide")
st.title("⚙️ Settings Editor")

st.warning("""
**Caution:** Editing these settings directly affects the application's core functionality. 
An incorrect change may require manual correction of the `settings.yaml` file.
**After saving, you must restart the Streamlit application for all changes to take full effect.**
""")

# Use session state to hold the editable version of the config
if 'editable_config' not in st.session_state:
    st.session_state.editable_config = copy.deepcopy(APP_CONFIG)

config = st.session_state.editable_config

# --- Save Button (at the top for easy access) ---
if st.button("Save All Settings to `settings.yaml`", type="primary"):
    # Before saving, update the config dict with the latest values from the widgets
    # This is done by reading from st.session_state where keys are used
    
    # Baserow section
    config['baserow']['base_url'] = st.session_state.bs_base_url
    config['baserow']['api_token'] = st.session_state.bs_api_token
    config['baserow']['sku_mapping_table_id'] = st.session_state.bs_sku_mapping_id
    config['baserow']['combo_sku_table_id'] = st.session_state.bs_combo_sku_id
    config['baserow']['inventory_table_id'] = st.session_state.bs_inventory_id
    config['baserow']['processed_sales_data_table_id'] = st.session_state.bs_processed_sales_id
    config['baserow']['category_table_id'] = st.session_state.bs_category_id
    config['baserow']['purchase_orders_table_id'] = st.session_state.bs_po_id
    
    # Warehouse columns from text area
    try:
        # Use YAML loader to safely parse the list from the text area
        warehouse_list = yaml.safe_load(st.session_state.bs_warehouse_cols)
        if isinstance(warehouse_list, list):
            config['baserow']['inventory_warehouse_columns'] = warehouse_list
        else:
            st.error("Warehouse Columns input is not a valid YAML list. Please check the format.")
            st.stop()
    except yaml.YAMLError as e:
        st.error(f"Error parsing Warehouse Columns YAML: {e}")
        st.stop()

    # Other sections
    config['cache']['expiry_days'] = st.session_state.cache_expiry
    config['logging']['level'] = st.session_state.log_level
    config['data_paths']['uploaded_sales_root'] = st.session_state.dp_sales_root
    config['replenishment_table']['feature_date_format'] = st.session_state.rt_date_format

    # The platform data is already updated in session state by the data_editor's logic
    
    if save_app_config(config):
        st.success("Settings saved successfully to `settings.yaml`!")
        st.warning("Please restart the Streamlit application for all changes to take full effect.")
        st.cache_resource.clear()
        st.cache_data.clear()
    else:
        st.error("Failed to save settings. Check the logs for more details.")

st.divider()

# --- Baserow Settings ---
with st.expander("Baserow Settings", expanded=True):
    st.text_input("Baserow Base URL", key="bs_base_url", value=config.get('baserow', {}).get('base_url', ''))
    st.text_input("Baserow API Token", key="bs_api_token", value=config.get('baserow', {}).get('api_token', ''), type="password")
    
    st.markdown("---")
    st.write("**Table IDs**")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.number_input("SKU Mapping Table ID", key="bs_sku_mapping_id", value=config.get('baserow', {}).get('sku_mapping_table_id', 0), step=1, format="%d")
        st.number_input("Combo SKU Table ID", key="bs_combo_sku_id", value=config.get('baserow', {}).get('combo_sku_table_id', 0), step=1, format="%d")
    with col2:
        st.number_input("Inventory Table ID", key="bs_inventory_id", value=config.get('baserow', {}).get('inventory_table_id', 0), step=1, format="%d")
        st.number_input("Processed Sales Data Table ID", key="bs_processed_sales_id", value=config.get('baserow', {}).get('processed_sales_data_table_id', 0), step=1, format="%d")
    with col3:
        st.number_input("Category/Product Table ID", key="bs_category_id", value=config.get('baserow', {}).get('category_table_id', 0), step=1, format="%d")
        st.number_input("Purchase Orders Table ID", key="bs_po_id", value=config.get('baserow', {}).get('purchase_orders_table_id', 0), step=1, format="%d")

    st.markdown("---")
    st.write("**Inventory Warehouse Columns**")
    # Use yaml.dump to format the list nicely for the text area
    warehouse_cols_str = yaml.dump(config.get('baserow', {}).get('inventory_warehouse_columns', []), default_flow_style=False)
    st.text_area(
        "List of Warehouse Columns for Inventory Summation (YAML format):",
        key="bs_warehouse_cols",
        value=warehouse_cols_str,
        height=250,
        help="Enter each column name on a new line, preceded by a hyphen and a space (e.g., '- TLCQ')."
    )

# --- Platform & Account Settings ---
with st.expander("Platform & Account Settings", expanded=False):
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
        edited_accounts_df = st.data_editor(
            accounts_df,
            column_config={
                "platform_idx": None, "account_idx": None,
                "Platform Name": st.column_config.TextColumn(disabled=True),
                "Platform Slug": st.column_config.TextColumn(disabled=True),
            },
            hide_index=True, use_container_width=True, key="platform_editor"
        )

        if not accounts_df.equals(edited_accounts_df):
            new_platforms_config = copy.deepcopy(config['platforms'])
            for index, row in edited_accounts_df.iterrows():
                p_idx, a_idx = row['platform_idx'], row['account_idx']
                target_account = new_platforms_config[p_idx]['accounts'][a_idx]
                target_account['name'] = row['Account Name']
                target_account['slug'] = row['Account Slug']
                target_account['sku_column'] = row['SKU Column']
                qty_str = row["Quantity Column(s)"]
                if ',' in qty_str:
                    target_account['quantity_columns'] = [s.strip() for s in qty_str.split(',')]
                    if 'quantity_column' in target_account: del target_account['quantity_column']
                else:
                    target_account['quantity_column'] = qty_str.strip()
                    if 'quantity_columns' in target_account: del target_account['quantity_columns']
            config['platforms'] = new_platforms_config
            st.info("Platform changes are staged. Click 'Save All Settings' at the top to apply.")

# --- Other Application Settings ---
with st.expander("Other Application Settings", expanded=False):
    col_other1, col_other2 = st.columns(2)
    with col_other1:
        st.subheader("Cache & Logging")
        st.number_input("Cache Expiry (days)", key="cache_expiry", value=config.get('cache', {}).get('expiry_days', 5), min_value=1, step=1)
        st.selectbox("Logging Level", key="log_level", options=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                     index=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"].index(config.get('logging', {}).get('level', 'INFO')))
    with col_other2:
        st.subheader("Data & Formatting")
        st.text_input("Uploaded Sales Root Directory", key="dp_sales_root", value=config.get('data_paths', {}).get('uploaded_sales_root', 'uploaded_data'))
        st.text_input("Replenishment Date Format", key="rt_date_format", value=config.get('replenishment_table', {}).get('feature_date_format', '%d-%b-%Y'),
                      help="Uses Python's strftime format codes. E.g., %Y-%m-%d or %d-%b-%Y.")