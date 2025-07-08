# RMS/utils/pdf_generator.py
from fpdf import FPDF
import pandas as pd
from datetime import datetime
import requests
from io import BytesIO
import logging

logger = logging.getLogger(__name__)

class PDF(FPDF):
    def header(self):
        # Logo or Title
        self.set_font('Arial', 'B', 15)
        self.cell(0, 10, 'Replenishment Plan', 0, 1, 'C')
        # Report Date
        self.set_font('Arial', '', 10)
        self.cell(0, 10, f"Generated on: {datetime.now().strftime('%d-%b-%Y %H:%M')}", 0, 0, 'R')
        # Line break
        self.ln(20)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}/{{nb}}', 0, 0, 'C')

def generate_replenishment_pdf(dataframe: pd.DataFrame):
    """
    Generates a PDF document from the replenishment plan DataFrame.
    """
    if dataframe is None or dataframe.empty:
        logger.warning("PDF_GEN: DataFrame is empty, cannot generate PDF.")
        return None

    try:
        pdf = PDF(orientation='L', unit='mm', format='A4') # Landscape orientation
        pdf.alias_nb_pages()
        pdf.add_page()
        pdf.set_font('Arial', '', 8)

        # Define column widths and headers
        # Adjust widths as needed. Total should be around 277 for A4 Landscape.
        col_widths = {
            "Image URL": 20, "MSKU": 40, "Category": 30, "Status": 25,
            "Current Inventory": 20, "Avg Daily Sales": 20, "DOS": 15,
            "Reorder Point": 20, "Order Qty Override": 25
        }
        
        headers = list(col_widths.keys())

        # Draw Table Header
        pdf.set_font('Arial', 'B', 8)
        pdf.set_fill_color(200, 220, 255) # Light blue background
        for header in headers:
            display_header = header.replace(" URL", "").replace(" Override", "") # Clean up header names for display
            pdf.cell(col_widths[header], 7, display_header, 1, 0, 'C', 1)
        pdf.ln()

        # Draw Table Rows
        pdf.set_font('Arial', '', 8)
        for index, row in dataframe.iterrows():
            # Store current y position to align all cells in the row
            y_pos_start = pdf.get_y()
            
            # --- Image Handling ---
            image_url = row.get("Image URL", "")
            image_cell_x = pdf.get_x()
            if image_url and isinstance(image_url, str) and image_url.startswith('http'):
                try:
                    # Fetch image from URL
                    response = requests.get(image_url, timeout=5)
                    response.raise_for_status()
                    img = BytesIO(response.content)
                    # Draw image in the cell, maintaining aspect ratio
                    pdf.image(img, x=image_cell_x + 1, y=y_pos_start + 1, w=col_widths["Image URL"] - 2, h=0)
                except Exception as e:
                    logger.warning(f"PDF_GEN: Could not load image for {row['MSKU']}. URL: {image_url}. Error: {e}")
            
            # Draw the border for the image cell
            pdf.cell(col_widths["Image URL"], 20, '', 1, 0, 'C')
            
            # --- Text Data Handling ---
            # Move to the next cell position
            pdf.set_xy(image_cell_x + col_widths["Image URL"], y_pos_start)
            
            for header in headers[1:]: # Skip Image URL as it's handled
                # Clean status icons for PDF
                cell_text = str(row.get(header, ''))
                if header == "Status":
                    cell_text = cell_text.encode('latin-1', 'replace').decode('latin-1').replace("?", "")
                
                pdf.multi_cell(col_widths[header], 20, cell_text, 1, 'C', ln=3) # Use multi_cell to center vertically
                # Move to the start of the next cell in the same row
                pdf.set_xy(pdf.get_x() + col_widths[header], y_pos_start)

            # Move to the next line after the row is complete
            pdf.ln(20)

        # Return PDF as bytes
        pdf_output_as_bytearray = pdf.output(dest='S')
        return bytes(pdf_output_as_bytearray)

    except Exception as e:
        logger.error(f"PDF_GEN: Failed to generate PDF. Error: {e}", exc_info=True)
        return None