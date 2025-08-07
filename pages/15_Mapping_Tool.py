# RMS/pages/13_Mapping_Tool.py
import streamlit as st
import pandas as pd
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

# --- THIS IS THE KEY CHANGE ---
# Import the centrally loaded APP_CONFIG, just like all other pages
from utils.config_loader import APP_CONFIG
# --- END CHANGE ---
from data_processing.baserow_fetcher import BaserowFetcher
from data_processing.sku_mapper import SKUMapper

import logging
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Mapping Tool - RMS", layout="wide")

# --- NEW: Add the standard config error check at the top ---
if "error" in APP_CONFIG:
    st.error(f"CRITICAL CONFIGURATION ERROR: {APP_CONFIG['error']}")
    st.stop()
# --- END NEW ---

st.title("🛠️ Universal Mapping Tool")
st.markdown("Upload a CSV, specify your source column, and select the data you want to map to.")

# --- Initialize Tools ---
@st.cache_resource
def get_mapping_tools(force_refresh=False):
    """Initializes and caches the Baserow Fetcher and SKUMapper."""
    try:
        # Use the centrally loaded APP_CONFIG
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        
        # Ensure all required table IDs are present before initializing
        required_ids = ['sku_mapping_table_id', 'combo_sku_table_id', 'amazon_listing_table_id']
        if not all(key in APP_CONFIG['baserow'] for key in required_ids):
            st.error(f"Missing one or more required table IDs in configuration: {required_ids}")
            return None

        sku_mapper_instance = SKUMapper(
            baserow_fetcher=fetcher,
            sku_mapping_table_id=APP_CONFIG['baserow']['sku_mapping_table_id'],
            combo_sku_table_id=APP_CONFIG['baserow']['combo_sku_table_id'],
            amazon_listing_table_id=APP_CONFIG['baserow']['amazon_listing_table_id'],
            cache_config=APP_CONFIG.get('cache', {}),
            project_root_dir=project_root,
            force_refresh_cache=force_refresh
        )
        return sku_mapper_instance
    except Exception as e:
        st.error(f"Error initializing mapping tools: {e}")
        return None

# --- Sidebar Controls ---
st.sidebar.header("Mapping Controls")
if st.sidebar.button("🔄 Refresh Mapping Data from Baserow"):
    try:
        get_mapping_tools.clear() # Clear the cache of the function
        # Re-run the function with force_refresh=True
        sku_mapper = get_mapping_tools(force_refresh=True)
        if sku_mapper:
            st.sidebar.success("Mapping data cache has been refreshed!")
        else:
            st.sidebar.error("Failed to re-initialize mapper after refresh.")
    except Exception as e:
        st.sidebar.error(f"Failed to refresh cache: {e}")

# Get the mapper instance (it will be cached after the first run)
sku_mapper = get_mapping_tools()

if not sku_mapper:
    st.error("SKU Mapper could not be initialized. Please check your Baserow configuration and connection.")
    st.stop()

# --- Main Page UI ---
st.header("1. Upload Your File")
uploaded_file = st.file_uploader("Upload a CSV file", type="csv", key="mapper_file_uploader")

if 'mapper_input_df' not in st.session_state: st.session_state.mapper_input_df = None
if 'mapper_result_df' not in st.session_state: st.session_state.mapper_result_df = None

if uploaded_file is not None:
    try:
        input_df = pd.read_csv(uploaded_file, dtype=str).fillna('')
        st.session_state.mapper_input_df = input_df
        st.session_state.mapper_result_df = None
        st.success(f"Successfully loaded '{uploaded_file.name}' with {len(input_df)} rows.")
    except Exception as e:
        st.error(f"Error reading CSV file: {e}")
        st.session_state.mapper_input_df = None

if st.session_state.mapper_input_df is not None:
    st.divider()
    st.header("2. Configure Mapping")
    
    input_df = st.session_state.mapper_input_df
    column_options = [""] + input_df.columns.tolist()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        # --- NEW: Select what type of data you are mapping FROM ---
        map_from_type = st.selectbox(
            "What type of identifier are you mapping from?",
            options=["Platform SKU", "MSKU", "ASIN"],
            index=0
        )
    with col2:
        # --- MODIFIED: Select the source column ---
        source_column = st.selectbox(
            f"Which column contains the {map_from_type}s?",
            options=column_options
        )
    with col3:
        # --- NEW: Select what data you want to map TO ---
        # Get available columns from the mapper's DataFrame
        available_map_to_cols = set()
        if sku_mapper.mapping_df is not None:
            available_map_to_cols.update(sku_mapper.mapping_df.columns)
        if sku_mapper.asin_df is not None:
            available_map_to_cols.update(sku_mapper.asin_df.columns)
        
        # Define a standard default set
        default_cols = ['sku', 'msku', 'Panel', 'Status', 'asin']
        
        map_to_cols = st.multiselect(
            "What information do you want to get?",
            options=sorted(list(available_map_to_cols)),
            default=[col for col in default_cols if col in available_map_to_cols]
        )


    if st.button("Run Mapping", disabled=(not source_column or not map_to_cols)):
        with st.spinner("Mapping data..."):
            
            # --- MODIFIED: New Mapping Logic ---
            results_list = []
            
            # This loop is more explicit and easier to debug than .apply() for complex returns
            for index, row in input_df.iterrows():
                source_value = row[source_column]

                per_source_results = []
                
                if map_from_type == "Platform SKU":
                    mapped_details = sku_mapper.get_mapping_details_for_sku(source_value)
                    if mapped_details:
                        # For SKU -> MSKU, there's only one result
                        result_row = {source_column: source_value}
                        for col in map_to_cols:
                            result_row[col] = mapped_details.get(col, "N/A")
                        per_source_results.append(result_row)

                # --- THIS IS THE NEW LOGIC ---
                elif map_from_type == "MSKU":
                    # This function returns a LIST of matching dictionaries
                    mapped_details_list = sku_mapper.get_mapping_details_for_msku(source_value)
                    if mapped_details_list:
                        # Create a new row in the output for EACH mapped platform SKU
                        for details_dict in mapped_details_list:
                            result_row = {source_column: source_value} # The source MSKU is the same for all
                            for col in map_to_cols:
                                # We get the data from the dictionary for this specific platform SKU
                                result_row[col] = details_dict.get(col, "N/A")
                            per_source_results.append(result_row)
                # --- END NEW LOGIC ---

                elif map_from_type == "ASIN":
                    mapped_details = sku_mapper.get_mapping_details_for_asin(source_value)
                    if mapped_details:
                        result_row = {source_column: source_value}
                        for col in map_to_cols:
                            result_row[col] = mapped_details.get(col, "N/A")
                        per_source_results.append(result_row)

                # Handle cases where no mapping was found at all
                if not per_source_results:
                    result_row = {source_column: source_value}
                    for col in map_to_cols:
                        result_row[col] = "NOT FOUND"
                    per_source_results.append(result_row)
                
                # Add all the generated rows for this source value to the main results list
                results_list.extend(per_source_results)

            result_df = pd.DataFrame(results_list)
            st.session_state.mapper_result_df = result_df
            st.success("Mapping complete! See the results below.")

# --- Display Results ---
if st.session_state.mapper_result_df is not None:
    st.divider()
    st.header("3. Mapping Results")
    
    result_df = st.session_state.mapper_result_df
    st.dataframe(result_df, use_container_width=True, hide_index=True)
    
    # Check for unmapped items in the primary 'msku' column if it was selected
    if 'msku' in result_df.columns:
        unmapped_count = (result_df['msku'] == "NOT FOUND").sum()
        if unmapped_count > 0:
            st.warning(f"Found **{unmapped_count}** items that could not be mapped.")
        else:
            st.success("All items were successfully mapped!")

    st.download_button(
        label="Download Mapped Data as CSV",
        data=result_df.to_csv(index=False).encode('utf-8'),
        file_name="mapped_output.csv",
        mime="text/csv",
        type="primary"
    )