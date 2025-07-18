# RMS/pages/11_Quick_PO_Intake.py
import streamlit as st
from datetime import date
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
from po_module.po_management import upload_file_to_baserow # Re-use this

import logging
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Quick PO Intake Form - RMS", layout="centered") # Centered for a form-like feel
st.title("🚀 Quick PO Intake Form")
st.markdown("Quickly log a new purchase order. An employee will process the details later.")

# --- Initialize Tools ---
@st.cache_resource
def get_intake_tools():
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        return fetcher
    except Exception as e:
        st.error(f"Error initializing Baserow connection: {e}"); return None

fetcher = get_intake_tools()
if not fetcher: st.error("Failed to initialize Baserow connection."); st.stop()

po_intake_table_id = APP_CONFIG['baserow'].get('po_intake_log_table_id')
if not po_intake_table_id:
    st.error("`po_intake_log_table_id` must be configured in settings.yaml.")
    st.stop()

# --- Intake Form ---
with st.form("quick_po_form", clear_on_submit=True):
    st.subheader("Order Details")
    
    # Using columns for a compact layout
    col1, col2 = st.columns(2)
    with col1:
        po_number = st.text_input("Purchase Order No.")
        vendor_name = st.text_input("Vendor Name")
        forwarder_name = st.text_input("Forwarder Name")
    with col2:
        order_date = st.date_input("Order Date (Payment Date)", value=date.today())
        submitter_email = st.text_input("Your Email Address (Optional)")
        
    st.subheader("Financials")
    fin_col1, fin_col2, fin_col3 = st.columns(3)
    with fin_col1:
        currency = st.selectbox("Currency", ["USD", "CNY"])
    with fin_col2:
        foreign_amount = st.number_input(f"Amount ({currency})", min_value=0.0, format="%.2f")
    with fin_col3:
        inr_amount = st.number_input("INR Amount (₹)", min_value=0.0, format="%.2f")
    
    carrying_charges = st.number_input("Carrying Charges (INR)", min_value=0.0, format="%.2f")
    
    notes = st.text_area("Notes / Special Instructions")

    st.subheader("Attachments")
    invoice_files = st.file_uploader("China Invoice", accept_multiple_files=True)
    packing_list_files = st.file_uploader("Packing List", accept_multiple_files=True)
    boxes_images = st.file_uploader("Boxes Images", type=['png', 'jpg', 'jpeg'], accept_multiple_files=True)
    ic_bills = st.file_uploader("Bills (If IC Shipment)", accept_multiple_files=True)

    submitted = st.form_submit_button("Submit New PO Log", type="primary")

    if submitted:
        with st.spinner("Submitting PO log..."):
            # 1. Upload all files and get their data objects
            def handle_uploads(files):
                if not files: return None
                uploaded_data = []
                for file in files:
                    file_data = upload_file_to_baserow(fetcher, file.getvalue(), file.name)
                    if file_data: uploaded_data.append(file_data)
                return uploaded_data if uploaded_data else None

            invoice_data = handle_uploads(invoice_files)
            packing_list_data = handle_uploads(packing_list_files)
            boxes_images_data = handle_uploads(boxes_images)
            ic_bills_data = handle_uploads(ic_bills)

            # 2. Prepare the record for Baserow
            record_data = {
                "Submitter Email": submitter_email,
                "Purchase Order No": po_number,
                "Order Date": order_date.strftime('%Y-%m-%d'),
                "Vendor Name": vendor_name,
                "Foreign Currency Amount": str(foreign_amount),
                "Currency": currency,
                "INR Amount": str(inr_amount),
                "Forwarder Name": forwarder_name,
                "Carrying Charges": str(carrying_charges),
                "Notes": notes,
                "Status": "Pending" # Always starts as Pending
            }
            
            # Add file fields only if files were uploaded
            if invoice_data: record_data["China Invoice"] = invoice_data
            if packing_list_data: record_data["Packing List"] = packing_list_data
            if boxes_images_data: record_data["Boxes Images"] = boxes_images_data
            if ic_bills_data: record_data["Bills (IC Shipment)"] = ic_bills_data

            # 3. Create the row in Baserow
            success = fetcher.batch_create_rows(po_intake_table_id, [record_data])

            if success:
                st.success("PO Log submitted successfully! Your team will be notified.")
                st.balloons()
            else:
                st.error("Failed to submit PO log. Please check the logs or try again.")