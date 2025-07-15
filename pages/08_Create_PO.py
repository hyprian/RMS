# RMS/pages/08_Create_PO.py
import streamlit as st
import pandas as pd
import os
import sys
from datetime import date, timedelta
import copy

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
if project_root not in sys.path: sys.path.insert(0, project_root)

from utils.config_loader import APP_CONFIG
from data_processing.baserow_fetcher import BaserowFetcher
from analytics_dashboard.data_loader import load_and_cache_analytics_data
from po_module.po_management import (
    get_all_pos, get_distinct_values, get_msku_details,
    upload_file_to_baserow, create_po_line_item, generate_po_number,
    get_msku_cost_details
)

import logging
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Create Purchase Order - RMS", layout="wide")
st.title("📝 Create New Purchase Order")

# --- All initialization code remains the same ---
@st.cache_resource
def get_po_tools():
    try:
        fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'], base_url=APP_CONFIG['baserow'].get('base_url'))
        return fetcher
    except Exception as e: st.error(f"Error initializing Baserow connection: {e}"); return None
fetcher = get_po_tools()
if not fetcher: st.error("Failed to initialize Baserow connection."); st.stop()
po_table_id = APP_CONFIG['baserow'].get('purchase_orders_table_id')
category_table_id = APP_CONFIG['baserow'].get('category_table_id')
if not po_table_id or not category_table_id:
    st.error("`purchase_orders_table_id` and `category_table_id` must be configured in settings.yaml.")
    st.stop()
if 'po_all_pos_df' not in st.session_state:
    st.session_state.po_all_pos_df = get_all_pos(fetcher, po_table_id)
if 'analytics_category_df' not in st.session_state:
    load_and_cache_analytics_data(fetcher, None, None, category_table_id,None)
all_pos_df = st.session_state.get('po_all_pos_df', pd.DataFrame())
all_category_df = st.session_state.get('analytics_category_df', pd.DataFrame())
if 'po_draft_items' not in st.session_state:
    st.session_state.po_draft_items = []
if 'po_header_initialized' not in st.session_state:
    st.session_state.po_header_po_number = generate_po_number()
    st.session_state.po_header_vendor_select = ""
    st.session_state.po_header_vendor_text = ""
    st.session_state.po_header_order_date = date.today()
    st.session_state.po_header_arrive_by = date.today() + timedelta(days=45)
    st.session_state.po_header_forwarder_select = ""
    st.session_state.po_header_forwarder_text = ""
    st.session_state.po_header_shipment_route = "Air"
    st.session_state.po_header_projection_code = "00-00-001"
    st.session_state.po_header_carrying_amount = 0.0
    st.session_state.po_header_porter_charges = 0.0
    st.session_state.po_header_packaging_charges = 0.0

    st.session_state.po_header_initialized = True

if 'line_item_msku' not in st.session_state:
    st.session_state.line_item_msku = ""
if 'line_item_qty' not in st.session_state:
    st.session_state.line_item_qty = 1
if 'line_item_price_foreign' not in st.session_state: # Renamed for clarity
    st.session_state.line_item_price_foreign = 0.0
if 'line_item_total_inr' not in st.session_state:
    st.session_state.line_item_total_inr = 0.0
if 'line_item_currency' not in st.session_state: # New state for currency
    st.session_state.line_item_currency = "USD"

# --- Callback Functions ---
def update_line_item_details():
    selected_msku = st.session_state.line_item_msku
    cost_details = get_msku_cost_details(all_category_df, selected_msku)
    # For now, we assume the cost in the table is USD. This could be enhanced later.
    st.session_state.line_item_price_foreign = float(cost_details.get('usd_cost', 0.0))

def add_item_to_draft():
    msku = st.session_state.line_item_msku
    if not msku:
        st.warning("Please select an MSKU before adding."); return
    
    msku_details = get_msku_details(all_category_df, msku)
    total_foreign_amt = float(st.session_state.line_item_qty) * float(st.session_state.line_item_price_foreign)
    
    new_item = {
        "MSKU": msku,
        "Category": msku_details.get('Category', ''),
        "Quantity": st.session_state.line_item_qty,
        "Currency": st.session_state.line_item_currency, # Add currency to draft
        "Price/pcs": st.session_state.line_item_price_foreign, # Generic name
        "Foreign Currency Amt": total_foreign_amt, # Generic name
        "INR Amt": st.session_state.line_item_total_inr,
        "HSN Code": msku_details.get('HSN Code', '')
    }
    st.session_state.po_draft_items.append(new_item)
    st.success(f"Added {st.session_state.line_item_qty} x {msku} to the draft.")
    
    # Reset form fields
    st.session_state.line_item_msku = ""
    st.session_state.line_item_qty = 1
    st.session_state.line_item_price_foreign = 0.0
    st.session_state.line_item_total_inr = 0.0
    # Do not reset currency, user might be adding multiple items in the same currency

# --- Form UI ---
st.header("1. PO Header Information")
# ... (Header section remains exactly the same) ...
vendor_options = [""] + get_distinct_values(all_pos_df, 'Vendor Name')
forwarder_options = [""] + get_distinct_values(all_pos_df, 'Forwarder')
with st.container(border=True):
    col1, col2, col3 = st.columns(3)
    with col1:
        st.text_input("PO Number", key="po_header_po_number", help="A unique PO number.")
        vendor_name_select = st.selectbox("Vendor Name", options=vendor_options, key="po_header_vendor_select")
        vendor_name_text = st.text_input("Or, Enter New Vendor Name:", key="po_header_vendor_text")
    with col2:
        st.date_input("Order Date", key="po_header_order_date")
        st.date_input("Arrive by Date", key="po_header_arrive_by")
    with col3:
        forwarder_select = st.selectbox("Forwarder", options=forwarder_options, key="po_header_forwarder_select")
        forwarder_text = st.text_input("Or, Enter New Forwarder:", key="po_header_forwarder_text")
        st.selectbox("Shipment Route", options=["Air", "Sea"], key="po_header_shipment_route")
    col4, col5, col6 = st.columns(3)
    with col4:
        st.number_input("Carrying Amount (INR)", key="po_header_carrying_amount", min_value=0.0, format="%.2f")
    with col5:
        st.number_input("Porter Charges (INR)", key="po_header_porter_charges", min_value=0.0, format="%.2f")
    with col6:
        st.number_input("Packaging/Other Charges (INR)", key="po_header_packaging_charges", min_value=0.0, format="%.2f")
    
    st.text_input("Projection Code", key="po_header_projection_code", help="Enter the code in XX-XX-001 format.")

st.divider()
st.header("2. Add Line Items")
with st.container(border=True):
    msku_options = [""] + (sorted(all_category_df['MSKU'].unique()) if all_category_df is not None and not all_category_df.empty else [])
    
    item_col1, item_col2, item_col3 = st.columns([3, 2, 1])
    with item_col1:
        st.selectbox("Select MSKU", options=msku_options, key="line_item_msku", on_change=update_line_item_details)
    msku_details = get_msku_details(all_category_df, st.session_state.line_item_msku)
    with item_col2:
        st.text_input("Category", value=msku_details.get('Category', ''), disabled=True, key="line_item_category_display")
    with item_col3:
        st.text_input("HSN Code", value=msku_details.get('HSN Code', ''), disabled=True, key="line_item_hsn_display")
    
    st.markdown("---")
    
    # --- MODIFIED SECTION FOR CURRENCY ---
    calc_col1, calc_col2, calc_col3, calc_col4, calc_col5 = st.columns([1, 2, 2, 2, 2])
    with calc_col1:
        st.number_input("Quantity", min_value=1, value=1, step=1, key="line_item_qty")
    with calc_col2:
        # Currency selector
        st.radio("Currency", options=["USD", "CNY"], key="line_item_currency", horizontal=True)
    with calc_col3:
        # Price per piece in selected currency
        currency_symbol = "$" if st.session_state.line_item_currency == "USD" else "¥"
        st.number_input(f"Price/pcs ({st.session_state.line_item_currency})", min_value=0.0, format="%.4f", key="line_item_price_foreign")
    
    total_foreign_amt = float(st.session_state.line_item_qty) * float(st.session_state.line_item_price_foreign)
    with calc_col4:
        st.metric(f"Total {st.session_state.line_item_currency} Amt", f"{currency_symbol}{total_foreign_amt:,.2f}")
    with calc_col5:
        st.number_input("Total INR Amt", min_value=0.0, format="%.2f", key="line_item_total_inr")

    st.button("Add Item to PO Draft", on_click=add_item_to_draft, use_container_width=True)
    # --- END MODIFIED SECTION ---

st.divider()
st.header("3. PO Draft")
if st.session_state.po_draft_items:
    # Rename columns for display in the draft table
    draft_df = pd.DataFrame(st.session_state.po_draft_items)
    display_draft_df = draft_df.rename(columns={
        "Price/pcs": f"Price/pcs ({draft_df['Currency'].iloc[0]})" if not draft_df.empty else "Price/pcs",
        "Foreign Currency Amt": f"Total Amt ({draft_df['Currency'].iloc[0]})" if not draft_df.empty else "Total Amt"
    })
    st.dataframe(display_draft_df, use_container_width=True)
    
    total_draft_value_inr = draft_df['INR Amt'].sum()
    st.metric("Total Draft Value", f"₹{total_draft_value_inr:,.2f}")
    if st.button("Clear Draft Items", key="clear_draft"):
        st.session_state.po_draft_items = []
        st.rerun()
else:
    st.info("No items have been added to the PO draft yet.")

st.divider()
st.header("4. Attachments & Final Submission")
allowed_invoice_types = ["pdf","jpg", "jpeg", "csv", "xlsx", "xls"]
allowed_packing_list_types = ["png", "jpg", "jpeg", "pdf", "csv", "xlsx", "xls"]

col_attach1, col_attach2 = st.columns(2)
with col_attach1:
    invoice_file = st.file_uploader(
        "Upload Final Invoice", 
        type=allowed_invoice_types, 
        accept_multiple_files=False # Assuming one final invoice, change to True if needed
    )
with col_attach2:
    packing_list_file = st.file_uploader(
        "Upload Packing List", 
        type=allowed_packing_list_types, 
        accept_multiple_files=True # Keep this as True to allow multiple images/docs
    )
# --- Submission Logic ---
if st.button("Create Purchase Order in Baserow", type="primary", disabled=not st.session_state.po_draft_items):
    # ... (Reading final header values remains the same) ...
    final_po_number = st.session_state.po_header_po_number
    final_vendor_name = st.session_state.po_header_vendor_text if st.session_state.po_header_vendor_select == "" else st.session_state.po_header_vendor_select
    final_order_date = st.session_state.po_header_order_date
    final_arrive_by_date = st.session_state.po_header_arrive_by
    final_forwarder = st.session_state.po_header_forwarder_text if st.session_state.po_header_forwarder_select == "" else st.session_state.po_header_forwarder_select
    final_shipment_route = st.session_state.po_header_shipment_route
    final_projection_code = st.session_state.po_header_projection_code
    final_carrying_amount = st.session_state.po_header_carrying_amount
    final_porter_charges = st.session_state.po_header_porter_charges
    final_packaging_charges = st.session_state.po_header_packaging_charges

    if not final_po_number or not final_vendor_name:
        st.error("PO Number and Vendor Name are required in the Header section.")
    else:
        with st.spinner("Creating Purchase Order..."):
            # ... (File upload logic remains the same) ...
            invoice_file_data, packing_list_data = None, None
            if invoice_file:
                invoice_file_data = upload_file_to_baserow(fetcher, invoice_file.getvalue(), invoice_file.name)
                if not invoice_file_data: st.error("Failed to upload Invoice PDF."); st.stop()
            if packing_list_file:
                packing_list_data = upload_file_to_baserow(fetcher, packing_list_file.getvalue(), packing_list_file.name)
                if not packing_list_data: st.error("Failed to upload Packing List image."); st.stop()

            total_items = len(st.session_state.po_draft_items)
            success_count = 0
            
            for item in st.session_state.po_draft_items:
                # --- MODIFIED: Prepare data dictionary with new currency fields ---
                line_item_data = {
                    "Po No.": final_po_number,
                    "Vendor Name": final_vendor_name,
                    "Order Date": final_order_date.strftime('%d-%b-%Y'),
                    "Arrive by": final_arrive_by_date.strftime('%d-%b-%Y'),
                    "Forwarder": final_forwarder,
                    "Shipment Route": final_shipment_route,
                    "Projection Code": final_projection_code,
                    "Carrying Amount": str(final_carrying_amount),
                    "Porter Charges": str(final_porter_charges),
                    "Packaging and Other Charges": str(final_packaging_charges),
                    "Msku Code": item['MSKU'],
                    "Category": item['Category'],
                    "Quantity": str(item['Quantity']),
                    "Currency": item['Currency'], # NEW
                    "USD Amt": str(item['Foreign Currency Amt']), # Re-using USD Amt field for the foreign currency total
                    "per pcs price usd": str(item['Price/pcs']), # Re-using per pcs price usd field
                    "INR Amt": str(item['INR Amt']),
                    "HSN Code": item['HSN Code'],
                    "Status": "Draft", "Payment Status": "Unpaid", "GRN Status": "Pending"
                }
                if invoice_file_data: line_item_data["Final Invoice"] = [invoice_file_data]
                if packing_list_data: line_item_data["Packing List"] = [packing_list_data]
                
                if create_po_line_item(fetcher, po_table_id, line_item_data):
                    success_count += 1
                else: st.error(f"Failed to create line item for MSKU: {item['MSKU']}.")
            
            if success_count == total_items:
                st.success(f"Successfully created Purchase Order {final_po_number} with {success_count} line item(s)!")
                st.session_state.po_draft_items = []
                st.session_state.po_header_initialized = False 
                st.balloons()
                st.rerun()
            else:
                st.error(f"Partially created PO. {success_count} of {total_items} items were created.")