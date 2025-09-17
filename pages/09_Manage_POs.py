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
from utils.pdf_generator import generate_po_pdf
from data_processing.baserow_fetcher import BaserowFetcher
from po_module.po_management import get_all_pos, update_po_line_item , upload_file_to_baserow
from analytics_dashboard.data_loader import load_and_cache_analytics_data # NEW

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
        if 'Order Date' in po_df.columns and pd.api.types.is_datetime64_any_dtype(po_df['Order Date']):
            po_df.sort_values(by='Order Date', ascending=False, inplace=True)
        return po_df

if 'manage_po_all_pos_df' not in st.session_state:
    st.session_state.manage_po_all_pos_df = load_po_data()


all_catalogue_df = st.session_state.get('analytics_catalogue_df')
if all_catalogue_df is None:
    # This ensures catalogue data is loaded if it hasn't been already
    load_and_cache_analytics_data(fetcher, None, None, None, APP_CONFIG['baserow'].get('catalogue_table_id'))
    all_catalogue_df = st.session_state.get('analytics_catalogue_df')

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

st.sidebar.divider()
use_date_filter = st.sidebar.checkbox("Enable Date Filter", value=False)
if use_date_filter:
    default_end_date = date.today()
    default_start_date = default_end_date - timedelta(days=89)
    selected_start_date = st.sidebar.date_input("Order Date From:", value=default_start_date)
    selected_end_date = st.sidebar.date_input("Order Date To:", value=default_end_date)
    if selected_start_date > selected_end_date:
        st.sidebar.error("Start Date cannot be after End Date."); st.stop()

# Apply filters
filtered_pos_df = all_pos_df.copy()
if selected_vendor != "All Vendors":
    filtered_pos_df = filtered_pos_df[filtered_pos_df['Vendor Name'] == selected_vendor]
if selected_status != "All Statuses":
    filtered_pos_df = filtered_pos_df[filtered_pos_df['Status'] == selected_status]
if use_date_filter:
    if 'Order Date' in filtered_pos_df.columns and pd.api.types.is_datetime64_any_dtype(filtered_pos_df['Order Date']):
        start_datetime = datetime.combine(selected_start_date, datetime.min.time())
        end_datetime = datetime.combine(selected_end_date, datetime.max.time())
        filtered_pos_df = filtered_pos_df[(filtered_pos_df['Order Date'] >= start_datetime) & (filtered_pos_df['Order Date'] <= end_datetime)]

# --- Display POs ---
st.header("Purchase Order List")

if filtered_pos_df.empty:
    st.info("No Purchase Orders match the current filters.")
else:
    # --- NEW: Merge with catalogue data to get Image URLs ---
    if all_catalogue_df is not None and not all_catalogue_df.empty:
        # Rename 'MSKU' in catalogue to 'Msku Code' for merging
        catalogue_for_merge = all_catalogue_df.rename(columns={'MSKU': 'Msku Code'})
        filtered_pos_df = pd.merge(filtered_pos_df, catalogue_for_merge[['Msku Code', 'Image URL']], on='Msku Code', how='left')
        filtered_pos_df['Image URL'].fillna('', inplace=True)
    else:
        filtered_pos_df['Image URL'] = ''
    grouped_by_po = filtered_pos_df.groupby('Po No.')
    st.info(f"Found **{len(grouped_by_po)}** unique Purchase Orders matching your filters.")
    
    for po_number, po_group_df in grouped_by_po:
        header_info = po_group_df.iloc[0]
        
        unique_vendors = po_group_df['Vendor Name'].unique()
        vendor_display = unique_vendors[0] if len(unique_vendors) == 1 else f"{len(unique_vendors)} Vendors"
        total_qty = po_group_df['Quantity'].sum()
        total_inr = po_group_df['INR Amt'].sum()
        order_date_str = header_info['Order Date'].strftime('%d-%b-%Y') if pd.notna(header_info['Order Date']) else 'N/A'
        
        expander_label = f"**PO #{po_number}** | Vendors: **{vendor_display}** | Date: {order_date_str} | Total Qty: {total_qty:,.0f} | Total Value: ₹{total_inr:,.2f}"
        
        with st.expander(expander_label):

            # --- NEW: Clean Header with Actions ---
            header_cols = st.columns([2, 1, 1]) # Ratios for Title, PDF Gen, Delete
            with header_cols[1]:
                # --- Dynamic PDF Generation UI ---
                if len(unique_vendors) == 1:
                    # If only one vendor, show a simple button
                    vendor_for_pdf = unique_vendors[0]
                    if st.button(f"Generate PDF for {vendor_for_pdf}", key=f"pdf_gen_single_{po_number}"):
                        pdf_df = po_group_df[['Image URL', 'Msku Code', 'Quantity', 'Shipment Route', 'Arrive by']].copy()
                        with st.spinner("Generating PDF..."):
                            pdf_bytes = generate_po_pdf(po_number, vendor_for_pdf, order_date_str, pdf_df)
                            st.session_state[f"pdf_data_{po_number}_{vendor_for_pdf}"] = pdf_bytes
                else:
                    # If multiple vendors, show a dropdown first
                    vendor_for_pdf = st.selectbox("Select Vendor for PDF:", options=unique_vendors, key=f"pdf_vendor_select_{po_number}")
                    if st.button(f"Generate PDF for {vendor_for_pdf}", key=f"pdf_gen_multi_{po_number}"):
                        vendor_specific_df = po_group_df[po_group_df['Vendor Name'] == vendor_for_pdf]
                        pdf_df = vendor_specific_df[['Image URL', 'Msku Code', 'Quantity', 'Shipment Route', 'Arrive by']].copy()
                        with st.spinner("Generating PDF..."):
                            pdf_bytes = generate_po_pdf(po_number, vendor_for_pdf, order_date_str, pdf_df)
                            st.session_state[f"pdf_data_{po_number}_{vendor_for_pdf}"] = pdf_bytes
                
                # Universal Download Button (appears after generation)
                pdf_key = f"pdf_data_{po_number}_{vendor_for_pdf}"
                if pdf_key in st.session_state and st.session_state[pdf_key]:
                    st.download_button(
                        label=f"📥 Download PDF for {vendor_for_pdf}",
                        data=st.session_state[pdf_key],
                        file_name=f"PO_{po_number}_{vendor_for_pdf.replace(' ', '_')}.pdf",
                        mime="application/pdf",
                        key=f"dl_{pdf_key}"
                    )

            with header_cols[2]:
                # --- Delete Popover ---
                delete_popover = st.popover("🗑️ Delete PO", use_container_width=True)
                with delete_popover:
                    st.error(f"**Are you sure you want to permanently delete all {len(po_group_df)} items for PO #{po_number}?**")
                    st.warning("This action cannot be undone.")
                    if st.button("YES, DELETE THIS PO", key=f"confirm_delete_{po_number}", type="primary", use_container_width=True):
                        with st.spinner(f"Deleting all items for PO #{po_number}..."):
                            ids_to_delete = po_group_df['id'].tolist()
                            delete_success = fetcher.batch_delete_rows(po_table_id, ids_to_delete)
                            if delete_success:
                                st.success(f"PO #{po_number} deleted.")
                                st.session_state.manage_po_all_pos_df = load_po_data()
                                st.rerun()
                            else:
                                st.error("Failed to delete PO. Check logs.")
            # --- END NEW HEADER ---
            #             
            st.subheader(f"Details for PO: {po_number}")
            
            # --- NEW: Toggle for detailed view ---
            show_all_columns = st.toggle("Show All Columns", key=f"toggle_{po_number}", value=False)
            
            grouped_by_vendor = po_group_df.groupby('Vendor Name')
            
            for vendor_name, vendor_group_df in grouped_by_vendor:
                with st.container(border=True):
                    # vendor_header_cols = st.columns([3, 1])
                    # with vendor_header_cols[0]:
                    st.markdown(f"#### Vendor: **{vendor_name}**")
                    # with vendor_header_cols[1]:
                    #     # Prepare the DataFrame needed for the PDF, but don't generate the PDF yet.
                    #     pdf_df = vendor_group_df[['Image URL', 'Msku Code', 'Quantity', 'Shipment Route', 'Arrive by']].copy()
                        
                    #     # Use a unique key for the button to store its state
                    #     pdf_button_key = f"pdf_btn_{po_number}_{vendor_name.replace(' ', '_')}"
                        
                    #     if st.button("Generate PDF", key=pdf_button_key):
                    #         with st.spinner("Generating PDF..."):
                    #             pdf_bytes = generate_po_pdf(po_number, vendor_name, order_date_str, pdf_df)
                    #             # Store the generated PDF in session state with a unique key
                    #             st.session_state[f"pdf_data_{pdf_button_key}"] = pdf_bytes

                    #     # Check if the PDF has been generated and is ready for download
                    #     if f"pdf_data_{pdf_button_key}" in st.session_state:
                    #         pdf_bytes_to_download = st.session_state[f"pdf_data_{pdf_button_key}"]
                    #         if pdf_bytes_to_download:
                    #             st.download_button(
                    #                 label="📥 Download PDF",
                    #                 data=pdf_bytes_to_download,
                    #                 file_name=f"PO_{po_number}_{vendor_name.replace(' ', '_')}.pdf",
                    #                 mime="application/pdf",
                    #                 key=f"dl_{pdf_button_key}"
                    #             )
                    #         else:
                    #             st.error("PDF generation failed.")
                    line_items_to_edit = vendor_group_df.copy() 

                    # Calculation logic
                    cost_cols = ['INR Amt', 'Carrying Amount', 'Porter Charges', 'Packaging and Other Charges', 'Quantity']
                    for col in cost_cols:
                        if col in line_items_to_edit.columns:
                            line_items_to_edit[col] = pd.to_numeric(line_items_to_edit[col], errors='coerce').fillna(0)
                        else: line_items_to_edit[col] = 0
                    line_items_to_edit['Final Cost'] = np.where(line_items_to_edit['Quantity'] > 0, (line_items_to_edit['INR Amt'] + line_items_to_edit['Carrying Amount'] + line_items_to_edit['Porter Charges']) / line_items_to_edit['Quantity'], 0)
                    packaging_per_piece = np.where(line_items_to_edit['Quantity'] > 0, line_items_to_edit['Packaging and Other Charges'] / line_items_to_edit['Quantity'], 0)
                    line_items_to_edit['Final Cost With Packaging'] = line_items_to_edit['Final Cost'] + packaging_per_piece

                    # --- NEW: Conditional column display ---
                    if show_all_columns:
                        # Show all relevant columns
                        display_cols = [
                            'id', 'Msku Code', 'Category', 'Projection Code', 'Status', 'Quantity', 
                            'Actual Qty Received', 'Damage/Dust', 'Missing', 'Extra',
                            'Currency', 'per pcs price usd', 'USD Amt', 'INR Amt', 
                            'Carrying Amount', 'Porter Charges', 'Packaging and Other Charges',
                            'Final Cost', 'Final Cost With Packaging',
                            'Date Of Qc', 'GRN Status', 'Payment Status', 'Actual Receiving Date'
                        ]
                    else:
                        # Show a curated, standard set of columns
                        display_cols = [
                            'id', 'Msku Code', 'Category', 'Status', 'Quantity', 
                            'Actual Qty Received', 'INR Amt', 'Final Cost With Packaging',
                            'GRN Status', 'Payment Status', 'Actual Receiving Date'
                        ]
                    
                    line_items_to_edit_subset = line_items_to_edit[[col for col in display_cols if col in line_items_to_edit.columns]]

                    # The full column_config remains, so editing works seamlessly in both views
                    edited_line_items_df = st.data_editor(
                        line_items_to_edit_subset,
                        column_config={
                            "id": None, "Msku Code": st.column_config.TextColumn(disabled=True),
                            "Category": st.column_config.TextColumn(disabled=True),
                            "Projection Code": st.column_config.TextColumn("Proj. Code"),
                            "Status": st.column_config.SelectboxColumn("PO Status", options=["Draft", "Sent For Approval", "Final Invoice Received", "Dispatched", "In Transit", "On Hold", "Received", "Cancelled"], required=True),
                            "Quantity": st.column_config.NumberColumn("Qty Ordered", format="%d", disabled=True),
                            "Actual Qty Received": st.column_config.NumberColumn("Qty Rcvd", format="%d", disabled=True),
                            "Damage/Dust": st.column_config.NumberColumn(format="%d", disabled=True),
                            "Missing": st.column_config.NumberColumn(format="%d", disabled=True),
                            "Extra": st.column_config.NumberColumn(format="%d", disabled=True),
                            "Currency": st.column_config.TextColumn(width="small", disabled=True),
                            "per pcs price usd": st.column_config.NumberColumn("Price/pcs Foreign", format="%.4f"),
                            "USD Amt": st.column_config.NumberColumn("Total Foreign Amt", format="%.2f", disabled=True),
                            "INR Amt": st.column_config.NumberColumn("Total INR Amt", format="₹%.2f"),
                            "Carrying Amount": st.column_config.NumberColumn("Carrying Amt (INR)", format="₹%.2f"),
                            "Porter Charges": st.column_config.NumberColumn("Porter Charges (INR)", format="₹%.2f"),
                            "Packaging and Other Charges": st.column_config.NumberColumn("Packaging/Other (INR)", format="₹%.2f"),
                            "Final Cost": st.column_config.NumberColumn("Final Cost/pcs", format="₹%.2f", disabled=True),
                            "Final Cost With Packaging": st.column_config.NumberColumn("Landed Cost/pcs", format="₹%.2f", disabled=True),
                            "Date Of Qc": st.column_config.TextColumn("QC Date", disabled=True),
                            "GRN Status": st.column_config.SelectboxColumn("GRN Status", options=["Pending", "In-Process", "GRN Completed", "On Hold"]),
                            "Payment Status": st.column_config.SelectboxColumn("Payment Status", options=["Unpaid", "Partially Paid", "Paid", "On Hold"]),
                            "Actual Receiving Date": st.column_config.DateColumn("Actual Receiving Date", format="DD-MMM-YYYY"),
                        },
                        hide_index=True, use_container_width=True, key=f"editor_{po_number}_{vendor_name}"
                    )
                    
                    # --- Save Changes Logic (Updated to handle all editable fields) ---
                    if not edited_line_items_df.equals(line_items_to_edit_subset):
                        if st.button(f"Save Changes for {vendor_name}", key=f"save_{po_number}_{vendor_name}"):
                            original_subset = line_items_to_edit.set_index('id') # Use the full df for comparison
                            edited_subset = edited_line_items_df.set_index('id')
                            changed_mask = original_subset.ne(edited_subset).any(axis=1)
                            
                            if changed_mask.any():
                                changed_ids = changed_mask[changed_mask].index.tolist()
                                with st.spinner(f"Saving updates for {vendor_name}..."):
                                    success_updates = 0
                                    for row_id in changed_ids:
                                        # Get the full row of new data from the original edited df
                                        update_data = edited_line_items_df[edited_line_items_df['id'] == row_id].iloc[0].to_dict()
                                        
                                        if 'Actual Receiving Date' in update_data and pd.notna(update_data['Actual Receiving Date']):
                                            update_data['Actual Receiving Date'] = update_data['Actual Receiving Date'].strftime('%d-%b-%Y')
                                        else:
                                            update_data['Actual Receiving Date'] = None

                                        # Prepare final payload with ALL potentially editable fields
                                        final_payload = {
                                            "Status": update_data.get("Status"),
                                            "Actual Receiving Date": update_data.get("Actual Receiving Date"),
                                            "GRN Status": update_data.get("GRN Status"),
                                            "Payment Status": update_data.get("Payment Status"),
                                            "Projection Code": update_data.get("Projection Code"),
                                            "INR Amt": str(update_data.get("INR Amt", 0)),
                                            "Carrying Amount": str(update_data.get("Carrying Amount", 0)),
                                            "Porter Charges": str(update_data.get("Porter Charges", 0)),
                                            "Packaging and Other Charges": str(update_data.get("Packaging and Other Charges", 0)),
                                            "per pcs price usd": str(update_data.get("per pcs price usd", 0))
                                        }
                                        
                                        if update_po_line_item(fetcher, po_table_id, row_id, final_payload):
                                            success_updates += 1
                                    if success_updates == len(changed_ids):
                                        st.success("Changes saved!"); st.session_state.manage_po_all_pos_df = load_po_data(); st.rerun()
                                    else:
                                        st.warning("Some changes could not be saved.")

            # Attachments Management is PO-level, so it stays outside the vendor loop
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
