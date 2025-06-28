# RMS/pages/07_Settings_Editor.py
import streamlit as st
import pandas as pd
import os
import sys
import copy # To deepcopy the config for editing

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG, save_app_config

st.set_page_config(page_title="Settings Editor - RMS", layout="wide")
st.title("⚙️ Settings Editor")

st.warning("""
**Caution:** Editing these settings directly affects the application's core functionality, including API connections and file parsing. 
Changes are saved directly to the `settings.yaml` file. An incorrect change may require manual correction of the file.
**After saving, you may need to restart the Streamlit application for all changes to take effect.**
""")

# Use session state to hold the editable version of the config
# This prevents edits from being lost on every rerun
if 'editable_config' not in st.session_state:
    # Use deepcopy to avoid modifying the original APP_CONFIG object in memory
    st.session_state.editable_config = copy.deepcopy(APP_CONFIG)

config = st.session_state.editable_config

# --- Baserow Settings ---
with st.expander("Baserow Settings", expanded=True):
    config['baserow']['base_url'] = st.text_input(
        "Baserow Base URL", 
        value=config.get('baserow', {}).get('base_url', '')
    )
    config['baserow']['api_token'] = st.text_input(
        "Baserow API Token", 
        value=config.get('baserow', {}).get('api_token', ''),
        type="password" # Mask the token
    )
    st.markdown("---")
    st.write("**Table IDs**")
    col1, col2 = st.columns(2)
    with col1:
        config['baserow']['sku_mapping_table_id'] = st.number_input(
            "SKU Mapping Table ID", 
            value=config.get('baserow', {}).get('sku_mapping_table_id', 0),
            step=1, format="%d"
        )
        config['baserow']['combo_sku_table_id'] = st.number_input(
            "Combo SKU Table ID", 
            value=config.get('baserow', {}).get('combo_sku_table_id', 0),
            step=1, format="%d"
        )
    with col2:
        config['baserow']['inventory_table_id'] = st.number_input(
            "Inventory Table ID", 
            value=config.get('baserow', {}).get('inventory_table_id', 0),
            step=1, format="%d"
        )
        config['baserow']['processed_sales_data_table_id'] = st.number_input(
            "Processed Sales Data Table ID", 
            value=config.get('baserow', {}).get('processed_sales_data_table_id', 0),
            step=1, format="%d"
        )

# --- Platform & Account Settings ---
with st.expander("Platform & Account Settings", expanded=True):
    st.info("Edit platform names, slugs, and account details here. To add or remove a platform/account, you must edit the `settings.yaml` file directly for now.")
    
    # We need to structure this for st.data_editor. It works best with a flat list of dicts.
    # We'll create a temporary DataFrame for editing.
    
    platforms_list = config.get('platforms', [])
    editable_accounts_data = []
    for p_idx, platform in enumerate(platforms_list):
        for a_idx, account in enumerate(platform.get('accounts', [])):
            editable_accounts_data.append({
                "platform_idx": p_idx,
                "account_idx": a_idx,
                "Platform Name": platform.get('name'),
                "Platform Slug": platform.get('slug'),
                "Account Name": account.get('name'),
                "Account Slug": account.get('slug'),
                "SKU Column": account.get('sku_column'),
                "Quantity Column(s)": ", ".join(account.get('quantity_columns', [])) if 'quantity_columns' in account else account.get('quantity_column', '')
            })

    if editable_accounts_data:
        accounts_df = pd.DataFrame(editable_accounts_data)
        
        edited_accounts_df = st.data_editor(
            accounts_df,
            column_config={
                "platform_idx": None, # Hide helper columns
                "account_idx": None,
                "Platform Name": st.column_config.TextColumn(disabled=True),
                "Platform Slug": st.column_config.TextColumn(disabled=True),
            },
            hide_index=True,
            use_container_width=True,
            key="platform_editor"
        )

        # After editing, we need to reconstruct the original nested structure
        # This is the complex part of editing nested YAML/JSON in a flat table
        if not accounts_df.equals(edited_accounts_df):
            new_platforms_config = copy.deepcopy(config['platforms'])
            for index, row in edited_accounts_df.iterrows():
                p_idx = row['platform_idx']
                a_idx = row['account_idx']
                
                # Update the nested dictionary
                target_account = new_platforms_config[p_idx]['accounts'][a_idx]
                target_account['name'] = row['Account Name']
                target_account['slug'] = row['Account Slug']
                target_account['sku_column'] = row['SKU Column']
                
                # Handle single vs. multiple quantity columns
                qty_str = row["Quantity Column(s)"]
                if ',' in qty_str:
                    # It's a list
                    target_account['quantity_columns'] = [s.strip() for s in qty_str.split(',')]
                    if 'quantity_column' in target_account:
                        del target_account['quantity_column'] # Remove the other key to avoid confusion
                else:
                    # It's a single column
                    target_account['quantity_column'] = qty_str.strip()
                    if 'quantity_columns' in target_account:
                        del target_account['quantity_columns'] # Remove the other key
            
            # Update the config in session state
            config['platforms'] = new_platforms_config
            st.info("Platform changes are staged. Click 'Save Settings' below to apply.")


# --- Other Settings ---
with st.expander("Cache & Logging Settings"):
    config['cache']['expiry_days'] = st.number_input(
        "Cache Expiry (days)",
        value=config.get('cache', {}).get('expiry_days', 5),
        min_value=1, step=1
    )
    config['logging']['level'] = st.selectbox(
        "Logging Level",
        options=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        index=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"].index(config.get('logging', {}).get('level', 'INFO'))
    )

st.divider()

# --- Save Button ---
if st.button("Save All Settings", type="primary"):
    # Update the session state with the final values from widgets
    st.session_state.editable_config = config
    
    # Save the config from session state to the file
    if save_app_config(st.session_state.editable_config):
        st.success("Settings saved successfully to `settings.yaml`!")
        st.warning("Please restart the Streamlit application for all changes to take full effect.")
        # Clear some cached resources to force re-initialization on next run
        # Note: This won't restart the app, but will clear cached functions
        st.cache_resource.clear()
        st.cache_data.clear()
    else:
        st.error("Failed to save settings. Check the logs for more details.")