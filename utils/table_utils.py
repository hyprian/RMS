# RMS/utils/table_utils.py
import streamlit as st

def get_replenishment_table_column_config():
    """
    Defines the column configuration for the st.data_editor
    for the replenishment table.
    """
    column_config = {
        "Feature": st.column_config.TextColumn(
            label="Feature Date",
            disabled=True, # Usually not editable
            help="Date of report generation."
        ),
        "Msku": st.column_config.TextColumn(
            label="MSKU",
            disabled=True, # MSKU is a key, should not be edited here
            help="Master Stock Keeping Unit."
        ),
        "Images": st.column_config.TextColumn(
            label="Images",
            # disabled=True, # Or make it editable if users can paste URLs
            help="Image URLs (leave blank for now or allow editing)."
        ),
        "Category": st.column_config.TextColumn(
            label="Category",
            # disabled=True, # Or make it editable
            help="Product category (leave blank for now or allow editing)."
        ),
        "Code": st.column_config.TextColumn(
            label="Internal Code",
            disabled=True, # Generated code, usually not editable
            help="Internal tracking code."
        ),
        "Current Inventory": st.column_config.NumberColumn(
            label="Current Inv.",
            disabled=True, # Fetched from system
            format="%d",
            help="Current inventory level."
        ),
        "30 days sales": st.column_config.NumberColumn(
            label="30d Sales",
            disabled=True, # Calculated from reports
            format="%d",
            help="Sales in the last 30 days."
        ),
        "60 days sales": st.column_config.NumberColumn(
            label="60d Sales",
            disabled=True, # Calculated from reports
            format="%d",
            help="Sales in the last 60 days."
        ),
        "Required Qty": st.column_config.NumberColumn(
            label="Required Qty",
            # disabled=True, # Calculated, but could be overridden
            format="%d",
            min_value=0,
            step=1,
            help="Calculated required quantity. Can be manually adjusted."
        ),
        "Buffer Stock": st.column_config.NumberColumn(
            label="Buffer Stock",
            # disabled=True, # Calculated, but could be overridden
            format="%d",
            min_value=0,
            step=1,
            help="Calculated buffer stock. Can be manually adjusted."
        ),
        "Marketing Stock": st.column_config.NumberColumn(
            label="Mkt. Stock",
            # disabled=True, # Calculated, but could be overridden
            format="%d",
            min_value=0,
            step=1,
            help="Calculated marketing stock. Can be manually adjusted."
        ),
        "Final Order Qty": st.column_config.NumberColumn(
            label="Final Order Qty",
            # disabled=True, # Calculated, but could be overridden
            format="%d",
            min_value=0,
            step=1,
            help="Final quantity to order. Can be manually adjusted."
        ),
        "Shipment Status": st.column_config.SelectboxColumn(
            label="Shipment",
            options=["Air", "Sea", "Road", "Pending"], # Add more options as needed
            default="Pending", # Default value for new rows if any
            required=True,
            help="Select the shipment method."
        ),
        "Product Status": st.column_config.SelectboxColumn(
            label="Product Status",
            options=["Fast Moving", "Slow Moving", "New", "Discontinued", "Watchlist"], # Add more
            default="Fast Moving",
            required=True,
            help="Current status of the product."
        ),
        "Po No.": st.column_config.TextColumn(
            label="PO Number",
            help="Purchase Order number, if applicable."
        ),
        "Order Status": st.column_config.SelectboxColumn(
            label="Order Status",
            options=["", "PO Raised", "Shipped", "Received", "Cancelled"], # Add more
            default="",
            help="Status of the purchase order."
        ),
        "Notes": st.column_config.TextColumn(
            label="Notes",
            width="large", # Make notes column wider
            help="Any relevant notes for this item."
        )
    }
    return column_config