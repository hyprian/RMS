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
st.title("📝 Purchase Order Workspace")

# --- Initialize Tools & Load Data ---
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
    load_and_cache_analytics_data(fetcher, None, None, category_table_id, None)

all_pos_df = st.session_state.get('po_all_pos_df', pd.DataFrame())
all_category_df = st.session_state.get('analytics_category_df', pd.DataFrame())

# --- Initialize Session State ---
if 'po_draft_items' not in st.session_state:
    st.session_state.po_draft_items = []
if 'pending_replenishment_plans' not in st.session_state:
    st.session_state.pending_replenishment_plans = {}
if 'po_header_initialized' not in st.session_state:
    st.session_state.po_header_initialized = False
if 'line_item_msku' not in st.session_state:
    st.session_state.line_item_msku = ""
if 'line_item_vendor_select' not in st.session_state:
    st.session_state.line_item_vendor_select = ""
if 'line_item_vendor_text' not in st.session_state:
    st.session_state.line_item_vendor_text = ""
if 'line_item_forwarder_select' not in st.session_state:
    st.session_state.line_item_forwarder_select = ""
if 'line_item_forwarder_text' not in st.session_state:
    st.session_state.line_item_forwarder_text = ""
if 'line_item_shipment_route' not in st.session_state:
    st.session_state.line_item_shipment_route = "Air"
if 'line_item_arrive_by' not in st.session_state:
    st.session_state.line_item_arrive_by = date.today() + timedelta(days=45)
if 'line_item_qty' not in st.session_state:
    st.session_state.line_item_qty = 1
if 'line_item_price_foreign' not in st.session_state:
    st.session_state.line_item_price_foreign = 0.0
if 'line_item_total_inr' not in st.session_state:
    st.session_state.line_item_total_inr = 0.0
if 'line_item_currency' not in st.session_state:
    st.session_state.line_item_currency = "USD"
if 'usd_to_inr_rate' not in st.session_state:
    st.session_state.usd_to_inr_rate = 83.50
if 'cny_to_inr_rate' not in st.session_state:
    st.session_state.cny_to_inr_rate = 11.50

# --- Callback Functions ---
def initialize_header():
    st.session_state.po_header_po_number = generate_po_number()
    st.session_state.po_header_order_date = date.today()
    st.session_state.po_header_projection_code = "00-00-001"
    st.session_state.po_header_carrying_amount = 0.0
    st.session_state.po_header_porter_charges = 0.0
    st.session_state.po_header_packaging_charges = 0.0
    st.session_state.po_header_initialized = True

def update_line_item_details():
    selected_msku = st.session_state.line_item_msku
    cost_details = get_msku_cost_details(all_category_df, selected_msku)
    if st.session_state.line_item_currency == "INR":
        st.session_state.line_item_price_foreign = float(cost_details.get('inr_cost', 0.0))
    else:
        st.session_state.line_item_price_foreign = float(cost_details.get('usd_cost', 0.0))

def add_item_to_draft():
    msku = st.session_state.line_item_msku
    vendor = st.session_state.line_item_vendor_text if st.session_state.line_item_vendor_select == "" else st.session_state.line_item_vendor_select
    forwarder = st.session_state.line_item_forwarder_text if st.session_state.line_item_forwarder_select == "" else st.session_state.line_item_forwarder_select
    shipment_route = st.session_state.line_item_shipment_route
    arrive_by_date = st.session_state.line_item_arrive_by
    if not msku: st.warning("Please select an MSKU."); return
    if not vendor: st.warning("Please select or enter a Vendor Name."); return
    
    msku_details = get_msku_details(all_category_df, msku)
    qty = st.session_state.line_item_qty
    price = st.session_state.line_item_price_foreign
    currency = st.session_state.line_item_currency
    total_foreign = float(qty) * float(price)
    total_inr = st.session_state.line_item_total_inr
    if currency == "INR": total_inr = total_foreign

    new_item = {
        "MSKU": msku, "Vendor Name": vendor, "Forwarder": forwarder,
        "Shipment Route": shipment_route, "Arrive by": arrive_by_date,
        "Category": msku_details.get('Category', ''), "Quantity": qty,
        "Currency": currency, "per pcs price usd": price,
        "USD Amt": total_foreign, "INR Amt": total_inr, "HSN Code": msku_details.get('HSN Code', '')
    }
    st.session_state.po_draft_items.append(new_item)
    st.success(f"Added {qty} x {msku} to the draft.")
    st.session_state.line_item_msku = ""; st.session_state.line_item_qty = 1
    st.session_state.line_item_price_foreign = 0.0; st.session_state.line_item_total_inr = 0.0

if not st.session_state.po_header_initialized:
    initialize_header()

# --- Load Plan from Replenishment Planner ---
if st.session_state.pending_replenishment_plans:
    st.header("📥 Load Plan from Replenishment Planner")
    with st.container(border=True):
        plan_keys = list(st.session_state.pending_replenishment_plans.keys())
        selected_plan_key = st.selectbox("Select a plan to load into the draft:", options=[""] + plan_keys)
        if selected_plan_key:
            if st.button(f"Load Plan: {selected_plan_key}", type="primary"):
                items_to_load = st.session_state.pending_replenishment_plans[selected_plan_key]
                draft_items = []
                for item in items_to_load:
                    qty = item.get('Order Quantity', 1); price = item.get('Unit Cost', 0.0)
                    currency = item.get('Currency', 'USD'); total_foreign = float(qty) * float(price)
                    calculated_inr = 0.0
                    if currency == "USD": calculated_inr = total_foreign * st.session_state.usd_to_inr_rate
                    elif currency == "CNY": calculated_inr = total_foreign * st.session_state.cny_to_inr_rate
                    elif currency == "INR": calculated_inr = total_foreign
                    draft_items.append({
                        "MSKU": item.get('MSKU'), "Vendor Name": item.get('Vendor Name'),
                        "Forwarder": "", "Shipment Route": "Air", "Arrive by": date.today() + timedelta(days=45),
                        "Category": item.get('Category'), "Quantity": qty, "Currency": currency,
                        "per pcs price usd": price, "USD Amt": total_foreign,
                        "INR Amt": calculated_inr, "HSN Code": item.get('HSN Code')
                    })
                st.session_state.po_draft_items.extend(draft_items)
                del st.session_state.pending_replenishment_plans[selected_plan_key]
                st.success(f"Plan '{selected_plan_key}' loaded into the draft below.")
                st.rerun()
    st.divider()

# --- Form UI ---
st.header("1. PO Header Information")
with st.container(border=True):
    col1, col2 = st.columns(2)
    with col1:
        st.text_input("PO Number", key="po_header_po_number")
        st.text_input("Projection Code", key="po_header_projection_code")
    with col2:
        st.date_input("Order Date", key="po_header_order_date")
    st.subheader("Additional PO-Level Charges (Optional)")
    charge_col1, charge_col2, charge_col3 = st.columns(3)
    with charge_col1: st.number_input("Carrying Amount (INR)", key="po_header_carrying_amount", min_value=0.0, format="%.2f")
    with charge_col2: st.number_input("Porter Charges (INR)", key="po_header_porter_charges", min_value=0.0, format="%.2f")
    with charge_col3: st.number_input("Packaging/Other Charges (INR)", key="po_header_packaging_charges", min_value=0.0, format="%.2f")

st.divider()
st.header("2. Add Line Items Manually")
st.sidebar.header("Currency Conversion")
st.sidebar.number_input("USD to INR Rate", key="usd_to_inr_rate", min_value=0.0, format="%.2f")
st.sidebar.number_input("CNY to INR Rate", key="cny_to_inr_rate", min_value=0.0, format="%.2f")

with st.container(border=True):
    msku_options = [""] + (sorted(all_category_df['MSKU'].unique()) if all_category_df is not None and not all_category_df.empty else [])
    vendor_options = [""] + get_distinct_values(all_pos_df, 'Vendor Name')
    forwarder_options = [""] + get_distinct_values(all_pos_df, 'Forwarder')
    item_col1, item_col2, item_col3 = st.columns([3, 2, 2])
    with item_col1: st.selectbox("Select MSKU", options=msku_options, key="line_item_msku", on_change=update_line_item_details)
    msku_details = get_msku_details(all_category_df, st.session_state.line_item_msku)
    with item_col2: st.text_input("Category", value=msku_details.get('Category', ''), disabled=True, key="line_item_category_display")
    with item_col3: st.text_input("HSN Code", value=msku_details.get('HSN Code', ''), disabled=True, key="line_item_hsn_display")
    item_colA, item_colB = st.columns(2)
    with item_colA:
        st.selectbox("Vendor Name", options=vendor_options, key="line_item_vendor_select")
        st.text_input("Or, Enter New Vendor Name:", key="line_item_vendor_text")
    with item_colB:
        st.selectbox("Forwarder", options=forwarder_options, key="line_item_forwarder_select")
        st.text_input("Or, Enter New Forwarder:", key="line_item_forwarder_text")
    date_col1, date_col2 = st.columns(2)
    with date_col1: st.selectbox("Shipment Route", options=["Air", "Sea"], key="line_item_shipment_route")
    with date_col2: st.date_input("Arrive by Date", value=date.today() + timedelta(days=45), key="line_item_arrive_by")
    st.markdown("---")
    calc_col1, calc_col2, calc_col3, calc_col4, calc_col5 = st.columns([1, 2, 2, 2, 2])
    with calc_col1: st.number_input("Quantity", min_value=1, value=1, step=1, key="line_item_qty")
    with calc_col2: st.radio("Currency", options=["USD", "CNY", "INR"], key="line_item_currency", horizontal=True, on_change=update_line_item_details)
    currency = st.session_state.line_item_currency
    currency_symbol = {"USD": "$", "CNY": "¥", "INR": "₹"}.get(currency, "")
    with calc_col3: st.number_input(f"Price/pcs ({currency})", min_value=0.0, format="%.4f", key="line_item_price_foreign")
    total_foreign_amt = float(st.session_state.line_item_qty) * float(st.session_state.line_item_price_foreign)
    with calc_col4: st.metric(f"Total {currency} Amt", f"{currency_symbol}{total_foreign_amt:,.2f}")
    calculated_inr = 0.0
    if currency == "USD": calculated_inr = total_foreign_amt * st.session_state.usd_to_inr_rate 
    elif currency == "CNY": calculated_inr = total_foreign_amt * st.session_state.cny_to_inr_rate 
    elif currency == "INR": calculated_inr = total_foreign_amt
    with calc_col5: st.number_input("Total INR Amt", min_value=0.0, value=calculated_inr, format="%.2f", key="line_item_total_inr", disabled=(currency == "INR"))
    st.button("Add Item to PO Draft", on_click=add_item_to_draft, use_container_width=True)

st.divider()
st.header("3. PO Draft")
if st.session_state.po_draft_items:
    draft_df = pd.DataFrame(st.session_state.po_draft_items)
    st.info("Review and make final edits to the draft below before creating the PO.")
    recalculated_items = []
    for item in draft_df.to_dict('records'):
        qty = item.get('Quantity', 1); price = item.get('per pcs price usd', 0.0); currency = item.get('Currency', 'USD')
        item['USD Amt'] = float(qty) * float(price)
        if currency == "USD": item['INR Amt'] = item['USD Amt'] * st.session_state.usd_to_inr_rate
        elif currency == "CNY": item['INR Amt'] = item['USD Amt'] * st.session_state.cny_to_inr_rate
        elif currency == "INR": item['INR Amt'] = item['USD Amt']
        recalculated_items.append(item)
    draft_df_recalculated = pd.DataFrame(recalculated_items)
    
    # edited_po_draft_df = st.data_editor(
    #     draft_df_recalculated,
    #     column_config={
    #         "MSKU": st.column_config.TextColumn(disabled=True),
    #         "Vendor Name": st.column_config.SelectboxColumn(options=vendor_options, required=True),
    #         "Forwarder": st.column_config.SelectboxColumn(options=forwarder_options, required=False),
    #         "Shipment Route": st.column_config.SelectboxColumn(options=["Air", "Sea"], required=True),
    #         "Arrive by": st.column_config.DateColumn(format="DD-MMM-YYYY", required=True),
    #         "Category": st.column_config.TextColumn(disabled=True),
    #         "Quantity": st.column_config.NumberColumn(required=True),
    #         "Currency": st.column_config.SelectboxColumn(options=["USD", "CNY", "INR"], required=True),
    #         "per pcs price usd": st.column_config.NumberColumn("Price/pcs", format="%.4f", required=True),
    #         "USD Amt": st.column_config.NumberColumn("Total Foreign Amt", format="%.2f", disabled=True),
    #         "INR Amt": st.column_config.NumberColumn("Total INR Amt", format="%.2f", required=True),
    #         "HSN Code": st.column_config.TextColumn(disabled=True),
    #     },
    #     hide_index=True, use_container_width=True, key="final_po_draft_editor", num_rows="dynamic"
    # )
    # st.session_state.po_draft_items = edited_po_draft_df.to_dict('records')
    
    # total_draft_value_inr = edited_po_draft_df['INR Amt'].sum()
    # st.metric("Total Draft Value", f"₹{total_draft_value_inr:,.2f}")
    # if st.button("Clear Draft Items", key="clear_draft"):
    #     st.session_state.po_draft_items = []
    #     st.rerun()
    edited_po_draft_df = st.data_editor(
    draft_df_recalculated,
    column_config={
        "MSKU": st.column_config.TextColumn(disabled=True),
        "Vendor Name": st.column_config.SelectboxColumn(options=vendor_options, required=True),
        "Forwarder": st.column_config.SelectboxColumn(options=forwarder_options, required=False),
        "Shipment Route": st.column_config.SelectboxColumn(options=["Air", "Sea"], required=True),
        "Arrive by": st.column_config.DateColumn(format="DD-MMM-YYYY", required=True),
        "Category": st.column_config.TextColumn(disabled=True),
        "Quantity": st.column_config.NumberColumn(required=True),
        "Currency": st.column_config.SelectboxColumn(options=["USD", "CNY", "INR"], required=True),
        "per pcs price usd": st.column_config.NumberColumn("Price/pcs", format="%.4f", required=True),
        "USD Amt": st.column_config.NumberColumn("Total Foreign Amt", format="%.2f", disabled=True),
        "INR Amt": st.column_config.NumberColumn("Total INR Amt", format="%.2f", required=True),
        "HSN Code": st.column_config.TextColumn(disabled=True),
    },
    hide_index=True,
    use_container_width=True,
    key="final_po_draft_editor",
    num_rows="dynamic"
)

    # Show total value from current edits
    total_draft_value_inr = edited_po_draft_df['INR Amt'].sum()
    st.metric("Total Draft Value", f"₹{total_draft_value_inr:,.2f}")

    # Add Save button to update session state
    if st.button("✅ Save Draft Changes", key="save_draft_items"):
        st.session_state.po_draft_items = edited_po_draft_df.to_dict('records')
        st.success("PO Draft saved successfully.")

    # Warning to users
    st.caption("⚠️ Make sure to click **'Save Draft Changes'** after editing, otherwise changes may be lost.")

    # Button to clear draft
    if st.button("🗑️ Clear Draft Items", key="clear_draft"):
        st.session_state.po_draft_items = []
        st.rerun()

else:
    st.info("No items in draft. Add items manually or load a plan from the Replenishment Planner.")
    
st.divider()
st.header("4. Attachments & Final Submission")
# ... (Attachments UI remains the same) ...
col_attach1, col_attach2 = st.columns(2)
with col_attach1:
    invoice_file = st.file_uploader("Upload Final Invoice", type=["pdf","jpg", "jpeg", "csv", "xlsx", "xls"], accept_multiple_files=True)
with col_attach2:
    packing_list_file = st.file_uploader("Upload Packing List", type=['png', 'jpg', 'jpeg', 'pdf', 'csv', 'xlsx', 'xls'], accept_multiple_files=True)

# --- Submission Logic ---
if st.button("Create Purchase Order in Baserow", type="primary", disabled=not st.session_state.po_draft_items):
    # --- CORRECTED: Read only header values from session state ---
    final_po_number = st.session_state.po_header_po_number
    final_order_date = st.session_state.po_header_order_date
    final_projection_code = st.session_state.po_header_projection_code
    final_carrying_amount = st.session_state.po_header_carrying_amount
    final_porter_charges = st.session_state.po_header_porter_charges
    final_packaging_charges = st.session_state.po_header_packaging_charges

    if not final_po_number:
        st.error("PO Number is required in the Header section.")
    else:
        with st.spinner("Creating Purchase Order..."):
            invoice_file_data_list, packing_list_data_list = [], []
            if invoice_file:
                for file in invoice_file:
                    file_data = upload_file_to_baserow(fetcher, file.getvalue(), file.name)
                    if file_data: invoice_file_data_list.append(file_data)
            if packing_list_file:
                for file in packing_list_file:
                    file_data = upload_file_to_baserow(fetcher, file.getvalue(), file.name)
                    if file_data: packing_list_data_list.append(file_data)

            total_items = len(st.session_state.po_draft_items)
            success_count = 0
            
            for item in st.session_state.po_draft_items:
                # --- CORRECTED: Get line-item specific values from the item dict ---
                line_item_data = {
                    "Po No.": final_po_number,
                    "Order Date": final_order_date.strftime('%d-%b-%Y'),
                    "Projection Code": final_projection_code,
                    "Carrying Amount": str(final_carrying_amount),
                    "Porter Charges": str(final_porter_charges),
                    "Packaging and Other Charges": str(final_packaging_charges),
                    
                    "Vendor Name": item.get('Vendor Name'),
                    "Forwarder": item.get('Forwarder'),
                    "Shipment Route": item.get('Shipment Route'),
                    "Arrive by": item.get('Arrive by').strftime('%d-%b-%Y') if pd.notna(item.get('Arrive by')) else None,
                    
                    "Msku Code": item.get('MSKU'),
                    "Category": item.get('Category'),
                    "Quantity": str(item.get('Quantity')),
                    "Currency": item.get('Currency'),
                    "USD Amt": str(item.get('USD Amt')),
                    "per pcs price usd": str(item.get('per pcs price usd')),
                    "INR Amt": str(item.get('INR Amt')),
                    "HSN Code": item.get('HSN Code'),
                    
                    "Status": "Draft",
                    "Payment Status": "Unpaid",
                    "GRN Status": "Pending"
                }
                if invoice_file_data_list: line_item_data["Final Invoice"] = invoice_file_data_list
                if packing_list_data_list: line_item_data["Packing List"] = packing_list_data_list
                
                if create_po_line_item(fetcher, po_table_id, line_item_data):
                    success_count += 1
                else: st.error(f"Failed to create line item for MSKU: {item.get('MSKU')}.")
            
            if success_count == total_items:
                st.success(f"Successfully created Purchase Order {final_po_number} with {success_count} line item(s)!")
                st.session_state.po_draft_items = []
                st.session_state.po_header_initialized = False 
                st.balloons()
                st.rerun()
            else:
                st.error(f"Partially created PO. {success_count} of {total_items} items were created.")