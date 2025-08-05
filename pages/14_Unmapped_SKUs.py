# RMS/pages/14_Unmapped_SKUs.py
import streamlit as st
import pandas as pd
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

st.set_page_config(page_title="Unmapped SKUs - RMS", layout="wide")
st.title("🚫 Unmapped SKUs Log")
st.markdown("This page lists all Platform SKUs that could not be mapped to an internal MSKU during data ingestion. Please add these mappings in your Baserow 'SKU Mapping' table.")

UNMAPPED_SKU_FILE = os.path.join(project_root, "unmapped_skus.csv")

# --- Load and Display Data ---
if os.path.exists(UNMAPPED_SKU_FILE):
    try:
        unmapped_df = pd.read_csv(UNMAPPED_SKU_FILE)
        
        if st.button("🔄 Refresh List"):
            # No action needed, st.button causes a rerun which re-reads the file
            pass

        if unmapped_df.empty:
            st.success("🎉 All SKUs are currently mapped!")
        else:
            st.info(f"Found {len(unmapped_df)} unique unmapped SKUs.")
            st.dataframe(unmapped_df, use_container_width=True, hide_index=True)
            
            # Provide a download button
            st.download_button(
                label="Download Unmapped SKUs as CSV",
                data=unmapped_df.to_csv(index=False).encode('utf-8'),
                file_name="unmapped_skus_to_fix.csv",
                mime="text/csv"
            )

    except Exception as e:
        st.error(f"Could not read the unmapped SKUs file. Error: {e}")
else:
    st.info("The unmapped SKUs log file does not exist yet. It will be created when you process a sales file with unmapped SKUs.")