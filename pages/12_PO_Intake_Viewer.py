# RMS/pages/09_PO_Intake_Viewer.py
import streamlit as st
import pandas as pd
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
from po_module.po_management import update_po_line_item, upload_file_to_baserow

st.set_page_config(page_title="PO Intake Viewer - RMS", layout="wide")
st.title("📋 Purchase Order Intake Viewer")
st.markdown("Review, edit, and process new POs submitted by the owner.")

# --- Initialize Tools & Load Data ---
@st.cache_resource
def get_intake_viewer_tools():
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        return fetcher
    except Exception as e:
        st.error(f"Error initializing Baserow connection: {e}"); return None

fetcher = get_intake_viewer_tools()
if not fetcher: st.error("Failed to initialize Baserow connection."); st.stop()

po_intake_table_id = APP_CONFIG['baserow'].get('po_intake_log_table_id')
if not po_intake_table_id:
    st.error("`po_intake_log_table_id` must be configured in settings.yaml.")
    st.stop()

# --- Data Loading ---
@st.cache_data(ttl=3600) # Cache data for 1 hour to reduce API calls on simple reruns
def load_intake_data():
    with st.spinner("Loading PO intake logs from Baserow..."):
        df = fetcher.get_table_data_as_dataframe(po_intake_table_id)
        if df is not None and not df.empty and 'Timestamp' in df.columns:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
            df.sort_values(by='Timestamp', ascending=False, inplace=True)
        return df

# --- Function to clear caches and session state ---
def clear_data_and_rerun():
    """Clears relevant caches and session state, then reruns the page."""
    st.cache_data.clear()
    st.session_state.pop('all_intake_df', None)
    st.rerun()

# --- Sidebar Filters ---
st.sidebar.header("Filter Logs")
if st.sidebar.button("Refresh Logs"):
    clear_data_and_rerun()

# Load data into session state if it's not there
if 'all_intake_df' not in st.session_state:
    st.session_state.all_intake_df = load_intake_data()

all_intake_df = st.session_state.all_intake_df

if all_intake_df is None or all_intake_df.empty:
    st.info("No PO intake logs found."); st.stop()

if 'edit_mode_id' not in st.session_state:
    st.session_state.edit_mode_id = None

status_options = ["All", "Pending", "Processed"]
selected_status = st.sidebar.selectbox("Filter by Status:", options=status_options, index=1)

filtered_df = all_intake_df.copy()
if selected_status != "All":
    filtered_df = filtered_df[filtered_df['Status'] == selected_status]

st.divider()
st.header(f"Displaying {len(filtered_df)} '{selected_status}' PO Logs")

if filtered_df.empty:
    st.info(f"No '{selected_status}' logs to display.")
else:
    for index, row in filtered_df.iterrows():
        po_num = row.get('Purchase Order No', 'N/A')
        vendor = row.get('Vendor Name', 'N/A')
        order_date = row.get('Order Date', 'N/A')
        row_id = row['id']
        
        with st.container(border=True):
            if st.session_state.edit_mode_id == row_id:
                # --- EDIT VIEW ---
                st.subheader(f"Editing PO: {po_num}")
                # ... (Input widgets for PO details) ...
                col1, col2, col3 = st.columns(3)
                with col1:
                    po_number_edit = st.text_input("Purchase Order No.", value=row.get('Purchase Order No', ''), key=f"po_num_{row_id}")
                    vendor_name_edit = st.text_input("Vendor Name", value=row.get('Vendor Name', ''), key=f"vendor_{row_id}")
                    forwarder_name_edit = st.text_input("Forwarder Name", value=row.get('Forwarder Name', ''), key=f"forwarder_{row_id}")
                with col2:
                    order_date_edit = st.date_input("Order Date", value=pd.to_datetime(row.get('Order Date')).date() if pd.notna(row.get('Order Date')) else None, key=f"date_{row_id}")
                    currency_edit = st.selectbox("Currency", options=["USD", "CNY"], index=["USD", "CNY"].index(row.get('Currency', 'USD')), key=f"currency_{row_id}")
                    foreign_amount_edit = st.number_input(f"Amount ({currency_edit})", value=float(row.get('Foreign Currency Amount', 0.0)), min_value=0.0, format="%.2f", key=f"foreign_amt_{row_id}")
                with col3:
                    inr_amount_edit = st.number_input("INR Amount (₹)", value=float(row.get('INR Amount', 0.0)), min_value=0.0, format="%.2f", key=f"inr_amt_{row_id}")
                    carrying_charges_edit = st.number_input("Carrying Charges (INR)", value=float(row.get('Carrying Charges', 0.0)), min_value=0.0, format="%.2f", key=f"carrying_{row_id}")
                    status_edit = st.selectbox("Status", options=["Pending", "Processed"], index=["Pending", "Processed"].index(row.get('Status', 'Pending')), key=f"status_{row_id}")
                notes_edit = st.text_area("Notes / Special Instructions", value=row.get('Notes', ''), key=f"notes_{row_id}")
                
                st.markdown("**Attachments**")
                file_fields_to_manage = {"China Invoice": "China Invoice", "Packing List": "Packing List", "Boxes Images": "Boxes Images", "Bills (IC Shipment)": "Bills (IC Shipment)"}
                if f"delete_files_{row_id}" not in st.session_state: st.session_state[f"delete_files_{row_id}"] = {}
                file_cols = st.columns(len(file_fields_to_manage))
                for i, (label, field_name) in enumerate(file_fields_to_manage.items()):
                    with file_cols[i]:
                        st.markdown(f"**{label}**")
                        existing_files = row.get(field_name, [])
                        if isinstance(existing_files, list):
                            for file_info in existing_files:
                                file_key = file_info['url']
                                if st.checkbox(f"Delete {file_info['name'][:20]}...", key=f"del_{row_id}_{file_key}"):
                                    st.session_state[f"delete_files_{row_id}"][file_key] = True
                                else:
                                    st.session_state[f"delete_files_{row_id}"][file_key] = False
                                st.link_button("View", url=file_info['url'])
                        st.file_uploader(f"Add {label}", accept_multiple_files=True, key=f"upload_{row_id}_{field_name}")

                btn_col1, btn_col2, _ = st.columns([1, 1, 5])
                with btn_col1:
                    if st.button("Save Changes", key=f"save_{row_id}", type="primary"):
                        with st.spinner("Saving changes..."):
                            update_payload = {
                                "Purchase Order No": st.session_state[f"po_num_{row_id}"], "Vendor Name": st.session_state[f"vendor_{row_id}"],
                                "Forwarder Name": st.session_state[f"forwarder_{row_id}"],
                                "Order Date": st.session_state[f"date_{row_id}"].strftime('%Y-%m-%d') if st.session_state[f"date_{row_id}"] else None,
                                "Currency": st.session_state[f"currency_{row_id}"], "Foreign Currency Amount": str(st.session_state[f"foreign_amt_{row_id}"]),
                                "INR Amount": str(st.session_state[f"inr_amt_{row_id}"]), "Carrying Charges": str(st.session_state[f"carrying_{row_id}"]),
                                "Status": st.session_state[f"status_{row_id}"], "Notes": st.session_state[f"notes_{row_id}"]
                            }
                            for label, field_name in file_fields_to_manage.items():
                                current_files = row.get(field_name, []);
                                if not isinstance(current_files, list): current_files = []
                                files_to_keep = [f for f in current_files if not st.session_state[f"delete_files_{row_id}"].get(f['url'])]
                                new_files_to_upload = st.session_state.get(f"upload_{row_id}_{field_name}", [])
                                if new_files_to_upload:
                                    for file in new_files_to_upload:
                                        uploaded_file_data = upload_file_to_baserow(fetcher, file.getvalue(), file.name)
                                        if uploaded_file_data: files_to_keep.append(uploaded_file_data)
                                update_payload[field_name] = files_to_keep
                            
                            success = update_po_line_item(fetcher, po_intake_table_id, row_id, update_payload)
                            if success:
                                st.success(f"PO Log for {po_num} updated.")
                                st.session_state.edit_mode_id = None
                                clear_data_and_rerun() # Use the helper function
                            else: st.error("Failed to update PO Log.")
                with btn_col2:
                    if st.button("Cancel", key=f"cancel_{row_id}"):
                        st.session_state.edit_mode_id = None
                        st.rerun()

            else:
                # --- READ-ONLY VIEW ---
                st.subheader(f"PO: {po_num} | Vendor: {vendor} | Order Date: {order_date}")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Foreign Currency Amt", f"{row.get('Foreign Currency Amount', 0)} ({row.get('Currency', 'N/A')})")
                    st.metric("INR Amount", f"₹{row.get('INR Amount', 0)}")
                    st.metric("Carrying Charges", f"₹{row.get('Carrying Charges', 0)}")
                with col2:
                    st.text(f"Forwarder: {row.get('Forwarder Name', 'N/A')}")
                    st.text(f"Submitter: {row.get('Submitter Email', 'N/A')}")
                    st.text(f"Submitted On: {row.get('Timestamp').strftime('%Y-%m-%d %H:%M') if pd.notna(row.get('Timestamp')) else 'N/A'}")
                with col3:
                    st.markdown("**Notes:**"); st.info(row.get('Notes', 'No notes provided.'))
                st.markdown("**Attachments:**")
                attachment_cols = st.columns(4)
                file_fields = {"China Invoice": "China Invoice", "Packing List": "Packing List", "Boxes Images": "Boxes Images", "Bills (IC Shipment)": "Bills (IC Shipment)"}
                for i, (label, field_name) in enumerate(file_fields.items()):
                    with attachment_cols[i]:
                        st.markdown(f"**{label}**")
                        files = row.get(field_name)
                        if isinstance(files, list) and files:
                            for file_info in files: st.link_button(f"View: {file_info['name']}", url=file_info['url'])
                        else: st.caption("Not attached.")
                
                st.markdown("---")
                action_col1, action_col2, action_col3, _ = st.columns([1, 2, 1, 5])
                with action_col1:
                    if st.button("Edit", key=f"edit_{row_id}"):
                        st.session_state.edit_mode_id = row_id
                        st.rerun()
                with action_col2:
                    if row.get('Status') == 'Pending':
                        if st.button("Mark as Processed", key=f"process_{row_id}", type="primary"):
                            with st.spinner("Updating status..."):
                                success = update_po_line_item(fetcher, po_intake_table_id, row_id, {"Status": "Processed"})
                                if success:
                                    st.success(f"PO Log for {po_num} marked as Processed.")
                                    clear_data_and_rerun() # Use the helper function
                                else: st.error("Failed to update status.")
                with action_col3:
                    if st.button("Delete", key=f"delete_{row_id}", type="secondary"):
                        st.session_state[f"confirm_delete_{row_id}"] = True
                
                if st.session_state.get(f"confirm_delete_{row_id}"):
                    st.warning(f"**Are you sure you want to permanently delete the PO log for '{po_num}'?**")
                    confirm_cols = st.columns(6)
                    with confirm_cols[0]:
                        if st.button("YES, DELETE", key=f"confirm_yes_{row_id}", type="primary"):
                            with st.spinner("Deleting log..."):
                                success = fetcher.delete_single_row(po_intake_table_id, row_id)
                                if success:
                                    st.success(f"PO Log for {po_num} deleted.")
                                    st.session_state.pop(f"confirm_delete_{row_id}", None)
                                    clear_data_and_rerun() # Use the helper function
                                else:
                                    st.error("Failed to delete log.")
                    with confirm_cols[1]:
                        if st.button("NO, CANCEL", key=f"confirm_no_{row_id}"):
                            st.session_state.pop(f"confirm_delete_{row_id}", None)
                            st.rerun()