# RMS/utils/pdf_generator.py
from fpdf import FPDF
import pandas as pd
from datetime import datetime
import requests
from io import BytesIO
import logging
import numpy as np

logger = logging.getLogger(__name__)

class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 15)
        self.cell(0, 10, 'Replenishment Plan', 0, 1, 'C')
        self.set_font('Arial', '', 10)
        self.cell(0, 10, f"Generated on: {datetime.now().strftime('%d-%b-%Y %H:%M')}", 0, 0, 'R')
        self.ln(15)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}/{{nb}}', 0, 0, 'C')

def generate_replenishment_pdf(dataframe: pd.DataFrame):
    """
    Generates a PDF from the Replenishment Plan Draft.
    """
    if dataframe is None or dataframe.empty:
        logger.warning("PDF_GEN: DataFrame is empty, cannot generate PDF.")
        return None

    try:
        pdf = PDF(orientation='L', unit='mm', format='A4')
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.set_left_margin(5)
        pdf.set_right_margin(5)
        pdf.alias_nb_pages()
        pdf.add_page()
        
        # --- NEW: Define columns for the Plan Draft ---
        # Ensure these column names match the DataFrame you pass in
        col_widths = {
            "Image URL": 25, 
            "MSKU": 70, 
            "Category": 45, 
            "HSN Code": 30,
            "Order Quantity": 30,
            "Notes": 70 # Optional notes column
        }
        headers = list(col_widths.keys())
        header_height = 12
        row_height = 25 # Increased height for images
        line_height = 5

        # --- Draw Table Header ---
        pdf.set_font('Arial', 'B', 9)
        pdf.set_fill_color(224, 235, 255)
        for header in headers:
            display_header = header.replace(" URL", "")
            pdf.cell(col_widths[header], header_height, display_header, 1, 0, 'C', 1)
        pdf.ln(header_height)

        # --- Draw Table Rows ---
        pdf.set_font('Arial', '', 9)
        pdf.set_draw_color(128, 128, 128)
        
        for index, row in dataframe.iterrows():
            if pdf.get_y() + row_height > pdf.page_break_trigger:
                pdf.add_page()
                # ... (redraw header on new page) ...
            
            y_start_row = pdf.get_y()
            x_start_row = pdf.get_x()
            
            # Draw Image
            image_url = row.get("Image URL", "")
            if image_url and isinstance(image_url, str) and image_url.startswith('http'):
                try:
                    response = requests.get(image_url, timeout=5)
                    response.raise_for_status()
                    img = BytesIO(response.content)
                    pdf.image(img, x=x_start_row + 1, y=y_start_row + 1, w=col_widths["Image URL"] - 2, h=row_height - 2)
                except Exception as e:
                    logger.warning(f"PDF_GEN: Could not load image for {row['MSKU']}.")
            
            # Draw the row boxes
            current_x = x_start_row
            for header in headers:
                pdf.rect(current_x, y_start_row, col_widths[header], row_height)
                current_x += col_widths[header]

            # Fill in text
            current_x = x_start_row + col_widths["Image URL"] # Start after image
            for header in headers[1:]: # Skip image column for text
                value = row.get(header, '')
                cell_text = str(value)
                
                pdf.set_xy(current_x + 2, y_start_row + (row_height / 2) - (line_height / 2)) # Vertically center
                pdf.multi_cell(col_widths[header] - 4, line_height, cell_text, border=0, align='C')
                current_x += col_widths[header]

            pdf.set_y(y_start_row + row_height)

        pdf_output_as_bytearray = pdf.output()
        return bytes(pdf_output_as_bytearray)

    except Exception as e:
        logger.error(f"PDF_GEN: Failed to generate PDF. Error: {e}", exc_info=True)
        return None