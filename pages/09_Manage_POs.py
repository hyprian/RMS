# RMS/pages/09_Manage_POs.py
import streamlit as st
import pandas as pd
import os
import sys
from datetime import date, timedelta, datetime
import numpy as np

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
from po_module.po_management import get_all_pos, update_po_line_item , upload_file_to_baserow

import logging
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Manage Purchase Orders - RMS", layout="wide")
st.title("📦 Manage Purchase Orders")
st.markdown("View, update, and track all your purchase orders.")

# --- Initialize Tools & Load Data ---
@st.cache_resource
def get_po_manage_tools():
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        return fetcher
    except Exception as e:
        st.error(f"Error initializing Baserow connection: {e}"); return None

fetcher = get_po_manage_tools()
if not fetcher: st.error("Failed to initialize Baserow connection."); st.stop()

po_table_id = APP_CONFIG['baserow'].get('purchase_orders_table_id')
if not po_table_id:
    st.error("`purchase_orders_table_id` must be configured in settings.yaml.")
    st.stop()

# --- Data Loading and Caching ---
def load_po_data():
    with st.spinner("Loading all purchase orders from Baserow..."):
        po_df = get_all_pos(fetcher, po_table_id)
        # Sort by Order Date descending to show most recent POs first
        if 'Order Date' in po_df.columns and pd.api.types.is_datetime64_any_dtype(po_df['Order Date']):
            po_df.sort_values(by='Order Date', ascending=False, inplace=True)
        return po_df

if 'manage_po_all_pos_df' not in st.session_state:
    st.session_state.manage_po_all_pos_df = load_po_data()

# --- Sidebar Filters ---
st.sidebar.header("Filters")
if st.sidebar.button("Refresh PO List"):
    st.session_state.manage_po_all_pos_df = load_po_data()
    st.rerun()

all_pos_df = st.session_state.manage_po_all_pos_df

if all_pos_df.empty:
    st.info("No Purchase Orders found. You can create one on the 'Create Purchase Order' page.")
    st.stop()

# Get filter options from the loaded data
vendor_options = ["All Vendors"] + sorted(all_pos_df['Vendor Name'].dropna().unique().tolist())
status_options = ["All Statuses"] + sorted(all_pos_df['Status'].dropna().unique().tolist())

selected_vendor = st.sidebar.selectbox("Filter by Vendor", options=vendor_options)
selected_status = st.sidebar.selectbox("Filter by Status", options=status_options)

# --- NEW: Optional Date Filter ---
st.sidebar.divider()
use_date_filter = st.sidebar.checkbox("Enable Date Filter", value=False)

if use_date_filter:
    # Set default dates for the filter
    default_end_date = date.today()
    default_start_date = default_end_date - timedelta(days=89) # Default to last 90 days
    
    selected_start_date = st.sidebar.date_input("Order Date From:", value=default_start_date, key="po_manage_start_date")
    selected_end_date = st.sidebar.date_input("Order Date To:", value=default_end_date, key="po_manage_end_date")
    
    if selected_start_date > selected_end_date:
        st.sidebar.error("Start Date cannot be after End Date.")
        st.stop()
# --- END NEW ---


# Apply filters
filtered_pos_df = all_pos_df.copy()
if selected_vendor != "All Vendors":
    filtered_pos_df = filtered_pos_df[filtered_pos_df['Vendor Name'] == selected_vendor]
if selected_status != "All Statuses":
    filtered_pos_df = filtered_pos_df[filtered_pos_df['Status'] == selected_status]

# --- NEW: Apply Date Filter if Enabled ---
if use_date_filter:
    # Ensure 'Order Date' is in datetime format for comparison
    if 'Order Date' in filtered_pos_df.columns and pd.api.types.is_datetime64_any_dtype(filtered_pos_df['Order Date']):
        # Convert date objects from date_input to datetime for comparison with datetime column
        start_datetime = datetime.combine(selected_start_date, datetime.min.time())
        end_datetime = datetime.combine(selected_end_date, datetime.max.time())
        
        filtered_pos_df = filtered_pos_df[
            (filtered_pos_df['Order Date'] >= start_datetime) & 
            (filtered_pos_df['Order Date'] <= end_datetime)
        ]
    else:
        st.sidebar.warning("Could not apply date filter. 'Order Date' column is missing or not a valid date type.")
# --- END NEW ---


# --- Display POs ---
st.header("Purchase Order List")

if filtered_pos_df.empty:
    st.info("No Purchase Orders match the current filters.")
else:
    # Group by PO Number to create one expander per PO
    grouped_by_po = filtered_pos_df.groupby('Po No.')
    
    st.info(f"Found **{len(grouped_by_po)}** unique Purchase Orders matching your filters.")
    
    for po_number, po_group_df in grouped_by_po:
        # ... (The rest of the code inside the loop for displaying expanders, data_editor, and attachments remains exactly the same) ...
        header_info = po_group_df.iloc[0]
        total_qty = po_group_df['Quantity'].sum()
        total_inr = po_group_df['INR Amt'].sum()
        order_date_str = header_info['Order Date'].strftime('%d-%b-%Y') if pd.notna(header_info['Order Date']) else 'N/A'
        projection_code_str = header_info.get('Projection Code', 'N/A')
        expander_label = f"**{po_number}** | Vendor: **{header_info['Vendor Name']}** | Proj. Code: **{projection_code_str}** | Status: **{header_info['Status']}** | Date: {order_date_str} | Qty: {total_qty:,.0f}"        
        with st.expander(expander_label):
            st.subheader(f"Details for PO: {po_number}")
            st.subheader("Line Items")
            line_items_to_edit = po_group_df.copy()

            cost_cols = ['INR Amt', 'Carrying Amount', 'Porter Charges', 'Packaging and Other Charges', 'Quantity']
            for col in cost_cols:
                line_items_to_edit[col] = pd.to_numeric(line_items_to_edit[col], errors='coerce').fillna(0)
            
            # Formula 1: Final Cost (including porter and carrying)
            line_items_to_edit['Final Cost'] = np.where(
                line_items_to_edit['Quantity'] > 0,
                (line_items_to_edit['INR Amt'] + line_items_to_edit['Carrying Amount'] + line_items_to_edit['Porter Charges']) / line_items_to_edit['Quantity'],
                0
            )
            
            # Formula 2: Final Cost With Packaging
            line_items_to_edit['Final Cost With Packaging'] = np.where(
                line_items_to_edit['Quantity'] > 0,
                line_items_to_edit['Final Cost'] + (line_items_to_edit['Packaging and Other Charges'] / line_items_to_edit['Quantity']),
                line_items_to_edit['Final Cost'] # If no quantity, it's the same as Final Cost (which would be 0)
            )
            # --- END CALCULATION ---

            display_cols = ['id', 'Msku Code', 'Category','Projection Code','Status', 'Quantity', 'Actual Qty Received', 'Damage/Dust', 'Missing', 'Extra', 'USD Amt', 'INR Amt', 'Carrying Amount', 'Porter Charges', 'Packaging and Other Charges', 'Final Cost', 'per pcs price usd', 'Date Of Qc', 'GRN Status', 'Payment Status' , 'Actual Receiving Date']
            line_items_to_edit_subset  = line_items_to_edit[[col for col in display_cols if col in line_items_to_edit.columns]]

            edited_line_items_df = st.data_editor(
                line_items_to_edit_subset,
                column_config={
                    "id": None, # Hide the Baserow row ID
                    "Msku Code": st.column_config.TextColumn(disabled=True),
                    "Category": st.column_config.TextColumn(disabled=True),
                    
                    # --- MODIFICATION: Configure all columns for display/editing ---
                    "Projection Code": st.column_config.TextColumn("Proj. Code"),
                    "Status": st.column_config.SelectboxColumn("PO Status", options=["Draft", "Sent For Approval", "Final Invoice Received", "Dispatched", "In Transit", "On Hold", "Received", "Cancelled"], required=True),
                    "Quantity": st.column_config.NumberColumn("Qty Ordered", format="%d", disabled=True),
                    # --- NEW & MODIFIED: Configure new columns ---
                    "Carrying Amount": st.column_config.NumberColumn("Carrying Amt (INR)", format="₹%.2f"),
                    "Porter Charges": st.column_config.NumberColumn("Porter Charges (INR)", format="₹%.2f"),
                    "Packaging and Other Charges": st.column_config.NumberColumn("Packaging/Other (INR)", format="₹%.2f"),
                    "Final Cost": st.column_config.NumberColumn("Final Cost/pcs (INR)", format="₹%.2f", disabled=True, help="(INR Amt + Carrying + Porter) / Qty"),
                    "Final Cost With Packaging": st.column_config.NumberColumn("Landed Cost/pcs (INR)", format="₹%.2f", disabled=True, help="Final Cost + Packaging Cost per piece"),
                    # --- END NEW ---                    "Actual Qty Received": st.column_config.NumberColumn("Qty Rcvd", format="%d", disabled=True),
                    "Damage/Dust": st.column_config.NumberColumn(format="%d", disabled=True),
                    "Missing": st.column_config.NumberColumn(format="%d", disabled=True),
                    "Extra": st.column_config.NumberColumn(format="%d", disabled=True),
                    "USD Amt": st.column_config.NumberColumn(format="$%.2f", disabled=True),
                    "INR Amt": st.column_config.NumberColumn(format="₹%.2f"), # Allow editing INR amount
                    "per pcs price usd": st.column_config.NumberColumn("Price/pcs USD", format="$%.4f", disabled=True),
                    "Date Of Qc": st.column_config.TextColumn("QC Date", disabled=True),
                    "GRN Status": st.column_config.SelectboxColumn("GRN Status", options=["Pending", "In-Process", "GRN Completed", "On Hold"]),
                    "Payment Status": st.column_config.SelectboxColumn("Payment Status", options=["Unpaid", "Partially Paid", "Paid", "On Hold"]),
                    "Actual Receiving Date": st.column_config.DateColumn("Actual Receiving Date", format="DD-MMM-YYYY"),
                },
                hide_index=True, use_container_width=True, key=f"editor_{po_number}"
            )            

            # --- ROBUST CHANGE DETECTION LOGIC ---
            # Set index to 'id' for easy row-by-row comparison
            original_subset = line_items_to_edit.set_index('id')
            edited_subset = edited_line_items_df.set_index('id')
            
            # Find which rows have changed by comparing the two DataFrames
            # The ne() method compares element-wise and returns a boolean DataFrame
            # .any(axis=1) checks if any value in a row is True (meaning any cell changed)
            changed_mask = original_subset.ne(edited_subset).any(axis=1)
            
            if changed_mask.any():
                if st.button(f"Save Changes for PO {po_number}", key=f"save_{po_number}"):
                    # Get the IDs of the rows that actually changed
                    changed_ids = changed_mask[changed_mask].index.tolist()
                    
                    with st.spinner(f"Saving updates for PO {po_number}..."):
                        success_updates = 0
                        for row_id in changed_ids:
                            # Get the full row of new data from the edited DataFrame
                            update_data = edited_subset.loc[row_id].to_dict()
                            
                            # Format date correctly for Baserow
                            if 'Actual Receiving Date' in update_data and pd.notna(update_data['Actual Receiving Date']):
                                update_data['Actual Receiving Date'] = update_data['Actual Receiving Date'].strftime('%d-%b-%Y')
                            else:
                                update_data['Actual Receiving Date'] = None

                            # Prepare final payload with only the fields that are actually editable on this page
                            final_payload = {
                                "Status": update_data.get("Status"),
                                "Actual Receiving Date": update_data.get("Actual Receiving Date"),
                                "GRN Status": update_data.get("GRN Status"),
                                "Payment Status": update_data.get("Payment Status"),
                                "Projection Code": update_data.get("Projection Code"),
                                "INR Amt": str(update_data.get("INR Amt", 0)), # Convert numbers to strings
                                "Carrying Amount": str(update_data.get("Carrying Amount", 0)),
                                "Porter Charges": str(update_data.get("Porter Charges", 0)),
                                "Packaging and Other Charges": str(update_data.get("Packaging and Other Charges", 0))
    
                            }
                            
                            if update_po_line_item(fetcher, po_table_id, row_id, final_payload):
                                success_updates += 1
                            else:
                                st.error(f"Failed to update line item with ID: {row_id}")

                        if success_updates == len(changed_ids):
                            st.success("All changes saved successfully!")
                            st.session_state.manage_po_all_pos_df = load_po_data()
                            st.rerun()
                        else:
                            st.warning("Some changes could not be saved. Please check logs.")
            # --- END ROBUST CHANGE DETECTION ---

            # ... (Attachments Management section remains the same) ...
            st.markdown("---"); st.subheader("Attachments Management")

            allowed_invoice_types = ["pdf", "csv","jpg", "jpeg", "xlsx", "xls"]
            allowed_packing_list_types = ["png", "jpg", "jpeg", "pdf", "csv", "xlsx", "xls"]

            file_col1, file_col2 = st.columns(2)
            with file_col1:
                st.markdown("**Final Invoice(s)**")
                invoice_files = header_info.get('Final Invoice')
                if isinstance(invoice_files, list) and invoice_files:
                    for file_info in invoice_files: st.link_button(f"View: {file_info['name']}", url=file_info['url'], use_container_width=True)
                else: st.text("No invoice uploaded.")
                new_invoice_files = st.file_uploader("Upload/Replace Invoice(s)", type=allowed_invoice_types, accept_multiple_files=True, key=f"invoice_upload_{po_number}")

            with file_col2: 
                st.markdown("**Packing List(s)**")
                packing_list_files = header_info.get('Packing List')
                if isinstance(packing_list_files, list) and packing_list_files:
                    num_images = len(packing_list_files); max_cols = 3
                    img_cols = st.columns(max_cols)
                    for i, file_info in enumerate(packing_list_files):
                        with img_cols[i % max_cols]:
                            if file_info.get('is_image', False): st.image(file_info['url'], caption=file_info.get('name'), use_container_width=True)
                            else: st.link_button(f"View: {file_info['name']}", url=file_info['url'])
                else: st.text("No packing list uploaded.")
                new_packing_list_files = st.file_uploader("Upload/Replace Packing List(s)", type=allowed_packing_list_types, accept_multiple_files=True, key=f"packing_list_upload_{po_number}")
            if st.button("Update Attachments", key=f"update_files_{po_number}", disabled=(not new_invoice_files and not new_packing_list_files)):
                with st.spinner("Uploading new files and updating PO..."):
                    update_payload = {}
                    if new_invoice_files:
                        existing_invoices = header_info.get('Final Invoice', []);
                        if not isinstance(existing_invoices, list): existing_invoices = []
                        for file in new_invoice_files:
                            file_data = upload_file_to_baserow(fetcher, file.getvalue(), file.name)
                            if file_data: existing_invoices.append(file_data)
                        if existing_invoices: update_payload["Final Invoice"] = existing_invoices
                    if new_packing_list_files:
                        existing_packing_lists = header_info.get('Packing List', []);
                        if not isinstance(existing_packing_lists, list): existing_packing_lists = []
                        for file in new_packing_list_files:
                            file_data = upload_file_to_baserow(fetcher, file.getvalue(), file.name)
                            if file_data: existing_packing_lists.append(file_data)
                        if existing_packing_lists: update_payload["Packing List"] = existing_packing_lists
                    if update_payload:
                        update_success_count = 0
                        for row_id in po_group_df['id']:
                            if update_po_line_item(fetcher, po_table_id, row_id, update_payload):
                                update_success_count += 1
                        if update_success_count == len(po_group_df):
                            st.success("Attachments updated successfully!"); st.session_state.manage_po_all_pos_df = load_po_data(); st.rerun()
                        else: st.error("Failed to update attachments on some line items.")
