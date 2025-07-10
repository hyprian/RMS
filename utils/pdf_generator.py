# # RMS/utils/pdf_generator.py
# from fpdf import FPDF
# import pandas as pd
# from datetime import datetime
# import requests
# from io import BytesIO
# import logging
# import numpy as np # For np.isreal

# logger = logging.getLogger(__name__)

# class PDF(FPDF):
#     def header(self):
#         self.set_font('Arial', 'B', 15)
#         self.cell(0, 10, 'Replenishment Plan', 0, 1, 'C')
#         self.set_font('Arial', '', 10)
#         self.cell(0, 10, f"Generated on: {datetime.now().strftime('%d-%b-%Y %H:%M')}", 0, 0, 'R')
#         self.ln(15) # A bit more space

#     def footer(self):
#         self.set_y(-15)
#         self.set_font('Arial', 'I', 8)
#         self.cell(0, 10, f'Page {self.page_no()}/{{nb}}', 0, 0, 'C')

# def generate_replenishment_pdf(dataframe: pd.DataFrame):
#     """
#     Generates a well-formatted PDF document from the replenishment plan DataFrame.
#     """
#     if dataframe is None or dataframe.empty:
#         logger.warning("PDF_GEN: DataFrame is empty, cannot generate PDF.")
#         return None

#     try:
#         pdf = PDF(orientation='L', unit='mm', format='A4')
#         pdf.alias_nb_pages()
#         pdf.add_page()
        
#         # --- Define Table Properties ---
#         # A4 Landscape width is 297mm. Margins are 10mm each side. Printable width = 277mm.
#         # Let's adjust widths to fit and give more space to MSKU/Category.
#         col_widths = {
#             "Image URL": 22, "MSKU": 55, "Category": 35, "Status": 25,
#             "Current Inventory": 20, "Avg Daily Sales": 20, "DOS": 15,
#             "Reorder Point": 20, "Order Qty Override": 25
#         }
#         headers = list(col_widths.keys())
#         line_height = 8 # Height for a single line of text
#         image_height = 20 # Fixed height for the image and row

#         # --- Draw Table Header ---
#         pdf.set_font('Arial', 'B', 9)
#         pdf.set_fill_color(224, 235, 255) # Lighter blue
#         pdf.set_text_color(0)
#         pdf.set_draw_color(128, 128, 128) # Grey for borders
        
#         for header in headers:
#             display_header = header.replace(" URL", "").replace(" Override", " Qty")
#             pdf.cell(col_widths[header], 10, display_header, 1, 0, 'C', 1)
#         pdf.ln()

#         # --- Draw Table Rows ---
#         pdf.set_font('Arial', '', 9)
        
#         for index, row in dataframe.iterrows():
#             # Check if there is enough space for the row, if not, add a new page and header
#             if pdf.get_y() + image_height > pdf.page_break_trigger:
#                 pdf.add_page()
#                 # Re-draw header on new page
#                 pdf.set_font('Arial', 'B', 9)
#                 for header in headers:
#                     display_header = header.replace(" URL", "").replace(" Override", " Qty")
#                     pdf.cell(col_widths[header], 10, display_header, 1, 0, 'C', 1)
#                 pdf.ln()
#                 pdf.set_font('Arial', '', 9)

#             # Store current y position to align all cells in the row
#             y_pos_start = pdf.get_y()
#             x_pos_start = pdf.get_x()
            
#             # --- Image Handling ---
#             image_url = row.get("Image URL", "")
#             if image_url and isinstance(image_url, str) and image_url.startswith('http'):
#                 try:
#                     response = requests.get(image_url, timeout=5)
#                     response.raise_for_status()
#                     img = BytesIO(response.content)
#                     # Draw image, centered in its cell box
#                     pdf.image(img, x=x_pos_start + 1, y=y_pos_start + 1, w=col_widths["Image URL"] - 2, h=image_height - 2)
#                 except Exception as e:
#                     logger.warning(f"PDF_GEN: Could not load image for {row['MSKU']}. URL: {image_url}.")
            
#             # --- Text Data Handling ---
#             # We will use cell() with ln=0 to draw cells side-by-side
#             current_x = x_pos_start
#             for header in headers:
#                 # Get the value and format it
#                 value = row.get(header, '')
                
#                 # Format numbers to 2 decimal places, integers to 0
#                 if isinstance(value, (int, float)) or np.isreal(value):
#                     if header == 'Avg Daily Sales':
#                         cell_text = f"{value:.2f}"
#                     else:
#                         cell_text = f"{value:,.0f}"
#                 else:
#                     cell_text = str(value)

#                 # Clean status icons
#                 if header == "Status":
#                     cell_text = cell_text.encode('latin-1', 'replace').decode('latin-1').replace("?", "").strip()
                
#                 # For the image column, we just draw the box and move on
#                 if header == "Image URL":
#                     pdf.cell(col_widths[header], image_height, '', 1, 0, 'C')
#                 else:
#                     # For text columns, draw the cell with text centered
#                     pdf.cell(col_widths[header], image_height, cell_text, 1, 0, 'C')
            
#             # Move to the next line after the row is complete
#             pdf.ln(image_height)

#         # Return PDF as bytes
#         pdf_output_as_bytearray = pdf.output() # Default dest is 'S' which returns bytearray
#         return bytes(pdf_output_as_bytearray)

#     except Exception as e:
#         logger.error(f"PDF_GEN: Failed to generate PDF. Error: {e}", exc_info=True)
#         return None


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
    Generates a well-formatted PDF document with vertically centered text.
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
        
        col_widths = {
            "Image URL": 22, "MSKU": 60, "Category": 40, "Status": 25,
            "Current Inventory": 28, "Avg Daily Sales": 28, "DOS": 15,
            "Reorder Point": 22, "Order Qty Override": 30
        }
        headers = list(col_widths.keys())
        header_height = 12
        row_height = 22
        line_height = 5

        # --- Draw Table Header ---
        pdf.set_font('Arial', 'B', 9)
        pdf.set_fill_color(224, 235, 255)
        
        x_start_header = pdf.get_x()
        for header in headers:
            display_header = header.replace(" URL", "").replace(" Override", " Qty")
            # Use multi_cell to calculate text height for vertical centering
            # but draw with cell for background fill
            pdf.cell(col_widths[header], header_height, display_header, 1, 0, 'C', 1)
        pdf.ln(header_height)

        # --- Draw Table Rows ---
        pdf.set_font('Arial', '', 9)
        pdf.set_draw_color(128, 128, 128)
        
        for index, row in dataframe.iterrows():
            if pdf.get_y() + row_height > pdf.page_break_trigger:
                pdf.add_page()
                pdf.set_font('Arial', 'B', 9)
                for header in headers:
                    display_header = header.replace(" URL", "").replace(" Override", " Qty")
                    pdf.cell(col_widths[header], header_height, display_header, 1, 0, 'C', 1)
                pdf.ln(header_height)
                pdf.set_font('Arial', '', 9)

            y_start_row = pdf.get_y()
            x_start_row = pdf.get_x()
            
            # --- Draw Image Cell ---
            image_url = row.get("Image URL", "")
            if image_url and isinstance(image_url, str) and image_url.startswith('http'):
                try:
                    response = requests.get(image_url, timeout=5)
                    response.raise_for_status()
                    img = BytesIO(response.content)
                    pdf.image(img, x=x_start_row + 1, y=y_start_row + 1, w=col_widths["Image URL"] - 2, h=row_height - 2)
                except Exception as e:
                    logger.warning(f"PDF_GEN: Could not load image for {row['MSKU']}.")
            
            # --- Draw the row of empty boxes first ---
            pdf.rect(x_start_row, y_start_row, sum(col_widths.values()), row_height)
            current_x = x_start_row
            for header in headers:
                pdf.line(current_x, y_start_row, current_x, y_start_row + row_height)
                current_x += col_widths[header]

            # --- Go back and fill in the text for each cell ---
            current_x = x_start_row
            for header in headers[1:]: # Skip the image column for text
                current_x += col_widths[headers[headers.index(header)-1]] # Move to start of current cell
                
                value = row.get(header, '')
                if isinstance(value, (int, float)) or np.isreal(value):
                    if header in ['Avg Daily Sales', 'DOS']:
                        cell_text = f"{value:.2f}"
                    else:
                        cell_text = f"{value:,.0f}"
                else:
                    cell_text = str(value)

                if header == "Status":
                    cell_text = cell_text.encode('latin-1', 'replace').decode('latin-1').replace("?", "").strip()
                
                # Set cursor to the start of the cell
                pdf.set_xy(current_x, y_start_row)
                
                # Use multi_cell to draw the text, now it will be contained within the pre-drawn box
                pdf.multi_cell(col_widths[header], line_height, cell_text, border=0, align='C')

            # Move cursor down for the next row
            pdf.set_y(y_start_row + row_height)

        pdf_output_as_bytearray = pdf.output()
        return bytes(pdf_output_as_bytearray)

    except Exception as e:
        logger.error(f"PDF_GEN: Failed to generate PDF. Error: {e}", exc_info=True)
        return None