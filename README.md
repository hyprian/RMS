# RMS: Replenishment Management System

![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg) ![Streamlit](https://img.shields.io/badge/Streamlit-1.30%2B-red.svg) ![Pandas](https://img.shields.io/badge/Pandas-2.0%2B-green.svg) ![Baserow](https://img.shields.io/badge/Database-Baserow-orange.svg)

## 1. Overview

The **Replenishment Management System (RMS)** is a comprehensive, multi-platform inventory and purchase order management dashboard built with Streamlit. It is designed to centralize sales data from various e-commerce channels (Amazon, Flipkart, Meesho, Shopify, FirstCry), provide insightful analytics, and generate intelligent replenishment recommendations for both products and packaging materials.

The system moves beyond simple inventory tracking by implementing an advanced replenishment engine that considers sales velocity, volatility, lead times, and business strategy (**FOCUSED** vs. **NON-FOCUSED** products) to suggest optimal order quantities and shipping methods.

### Core Features:

-   **Multi-Platform Data Ingestion:** A dedicated interface to upload sales reports from various channels, which are then parsed, standardized, and stored centrally in a Baserow database.
-   **Advanced Analytics Dashboard:** A suite of interactive pages to visualize key performance indicators (KPIs), analyze sales trends, and drill down into product and platform-specific performance.
-   **Intelligent Replenishment Planner:**
    -   A sophisticated engine that calculates sales statistics (velocity, volatility) and uses configurable business parameters to generate actionable replenishment plans.
    -   Separate planning modules for core products and packaging materials.
    -   **On-Order Visibility** to prevent over-ordering by accounting for items already in transit.
-   **Purchase Order (PO) Management:**
    -   A "Quick PO Intake" form for business owners to rapidly log new orders.
    -   A comprehensive "PO Workspace" for employees to create detailed, multi-vendor purchase orders from scratch or from replenishment plans.
    -   A "Manage POs" view to track, edit, and update the status of all purchase orders.
-   **Goods Received Note (GRN) Module:** An interface to log the receipt of goods against purchase orders, tracking quantities, damages, and status.
-   **Automated WhatsApp Notifications:** A standalone, webhook-triggered notification system that sends proactive alerts for critical events like low stock and stale data via a WAHA server.
-   **Dynamic Configuration:** Core settings like Baserow table IDs and replenishment parameters are managed externally in a Google Sheet, allowing for real-time updates to the deployed application without code changes.

### High-Level Architecture

The application is built on a modular architecture to ensure scalability and maintainability:

1.  **Frontend (UI):** Built entirely with Streamlit, organized into logical pages.
2.  **Backend Logic:** Python modules separated by concern:
    -   `data_processing`: Core data fetching from Baserow.
    -   `data_ingestion`: Platform-specific parsers for standardizing sales data.
    -   `analytics_dashboard`: Caching and KPI calculation logic.
    -   `replenishment_engine`: The advanced replenishment calculation core.
    -   `po_module`: Logic for creating and managing Purchase Orders.
    -   `packaging_module`: Logic for packaging replenishment.
    -   `notifications`: Logic for generating and sending WhatsApp alerts.
3.  **Data Storage:**
    -   **Baserow:** Acts as the primary database for all processed data (Sales, Inventory, POs, etc.).
    -   **Google Sheets:** Manages dynamic application settings like table IDs and replenishment parameters.
    -   **Local File Cache:** Uses `.parquet` files to cache Baserow data, ensuring fast app startup and performance.
4.  **Automation:**
    -   A standalone Flask server (`webhook_server.py`) provides an endpoint for external triggers.
    -   An external scheduler like **n8n** (using a Cron node) calls this webhook to run the notification script (`run_notifications.py`) automatically.

---

## 2. Project Structure
```
RMS/
├── .streamlit/
│ └── secrets.toml # Local secrets (API keys, credentials)
├── analytics_dashboard/
│ ├── charts.py # Functions for Plotly charts
│ ├── data_loader.py # Central data loading and caching logic
│ └── kpi_calculations.py # Functions for calculating KPIs
├── data_ingestion/
│ ├── amazon_parser.py # Parsers for each platform...
│ ├── flipkart_parser.py
│ ├── meesho_parser.py
│ ├── shopify_parser.py
│ └── firstcry_parser.py
├── data_processing/
│ └── baserow_fetcher.py # Core class for all Baserow API interactions
├── notifications/
│ ├── message_generator.py # Creates formatted WhatsApp message strings
│ ├── notification_engine.py # Logic to check for alerts and orchestrate sending
│ └── waha_sender.py # Class to communicate with the WAHA server
├── packaging_module/
│ └── packaging_logic.py # Calculations for packaging replenishment
├── po_module/
│ └── po_management.py # Functions for creating/managing POs
├── replenishment_engine/
│ └── core.py # The advanced replenishment calculation engine
├── pages/
│ ├── 01_Sales_Data_Ingestion.py
│ ├── 02_Analytics_Overview.py
│ ├── ... (other dashboard pages) ...
├── utils/
│ ├── config_loader.py # Loads settings from YAML and Google Sheets
│ ├── gspread_client.py # Manages Google Sheets connection
│ └── pdf_generator.py # Creates PDF documents for POs/Plans
├── app.py # Main Streamlit entry point, handles webhook
├── run_notifications.py # Standalone script for scheduled notifications
├── webhook_server.py # Flask server for the n8n webhook
├── settings.yaml # Static configuration
└── requirements.txt # Python dependencies
```

---

## 3. Key Modules & Functions Explained

### `utils/config_loader.py`

*   **Purpose:** Centralizes all application configuration.
*   **`load_and_merge_configs()`:** This is the main function, cached by Streamlit. It first loads static settings from `settings.yaml`, then connects to the Google Sheet specified in the YAML to fetch dynamic settings (like table IDs and replenishment parameters), and merges them into a single, comprehensive configuration dictionary (`APP_CONFIG`) that is used throughout the app.

### `data_processing/baserow_fetcher.py`

*   **Purpose:** Contains the `BaserowFetcher` class, which is the sole interface for all communication with the Baserow API.
*   **Key Methods:**
    -   `get_table_data_as_dataframe(table_id)`: Fetches all rows from a given table and returns a Pandas DataFrame. Handles pagination automatically.
    -   `get_inventory_data(table_id)`: Fetches and aggregates inventory from multiple warehouse columns defined in `settings.yaml`.
    -   `batch_create_rows(table_id, records_list)`: Efficiently creates multiple new rows in a table.
    -   `batch_delete_rows(table_id, row_ids_to_delete)`: Efficiently deletes multiple rows by their IDs, handling the API's 200-item limit by chunking.
    -   `update_po_line_item(table_id, row_id, data_dict)`: Updates a single row with new data using a `PATCH` request.

### `replenishment_engine/core.py`

*   **Purpose:** The "brain" of the application. It contains the advanced, purely computational logic for replenishment.
*   **Key Functions:**
    -   `calculate_sales_stats(...)`: Takes daily sales data and computes key statistics like 30-day average, standard deviation, coefficient of variation (volatility), and a velocity score.
    -   `run_replenishment_engine(...)`: The main orchestrator. It takes DataFrames of product info, sales stats, and open POs, along with global parameters. It then executes the entire chain of calculations (dynamic safety stock, lead times, coverage, order quantities for FOCUSED vs. NON-FOCUSED products) and returns a single, comprehensive DataFrame with all results and status classifications.

### `notifications/notification_engine.py`

*   **Purpose:** Contains the logic for the standalone notification script.
*   **`run_all_checks()`:** This function is designed to be run "headlessly" (without a UI). It loads all necessary data from Baserow, runs the replenishment engine to get the latest product statuses, checks for conditions (e.g., low stock, stale data), and triggers the `WahaSender` to send alerts.

---

## 4. Setup and Usage

### Prerequisites

-   Python 3.10+
-   A running Baserow instance.
-   A running WAHA (WhatsApp HTTP API) server instance.
-   A Google Cloud Platform project with a Service Account, with the Google Drive and Google Sheets APIs enabled.

### Local Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd RMS
    ```
2.  **Create and activate a Python virtual environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Linux/macOS
    venv\Scripts\activate    # On Windows
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Configure Secrets:** Create a file at `RMS/.streamlit/secrets.toml` and add your `[google_credentials]` from the downloaded service account JSON file.
5.  **Configure Settings:**
    -   Create a Google Sheet named "APP Settings" with two tabs: "Config" (for table IDs) and "Parameters" (for replenishment logic).
    -   Share this sheet with the service account's email address.
    -   Create `settings.yaml` in the root directory. Populate it with your Baserow API token, the Google Sheet ID, and other static settings.
6.  **Run the Streamlit app:**
    ```bash
    streamlit run app.py
    ```

### Deployment & Automation

-   **Streamlit App:** The application is designed to be deployed on Streamlit Cloud. The `google_credentials` must be added to the Streamlit Cloud secrets manager.
-   **Webhook Server:** The `webhook_server.py` script should be deployed on a persistent server (e.g., the same server as your WAHA instance). It can be run in the background using a process manager like `tmux` or `systemd`.
    ```bash
    # Example using tmux
    tmux new -s rms_backend
    # Inside tmux session:
    source /path/to/venv/bin/activate
    python webhook_server.py
    ```
-   **Automation:** An external scheduler like **n8n** (using a Cron node) should be configured to send a `GET` request to the webhook server's endpoint (`http://YOUR_SERVER_IP:49176/trigger-notifications?key=YOUR_SECRET_KEY`) on a regular schedule.

---

## 5. Future Extensibility (Placeholders)

This application is designed to be extensible. Here are areas for future development:

-   **[Placeholder] COGS and Profitability:**
    -   The `Category` table in Baserow can be enhanced with more detailed cost fields (e.g., `Cost`, `Shipping per Unit`, `Platform Fee %`).
    -   The analytics and replenishment engines can be updated to calculate and display true Gross Profit, Net Profit, and ROI metrics.
-   **[Placeholder] Advertising Analytics:**
    -   A new data ingestion parser can be created for advertising reports (e.g., from Amazon Ads).
    -   A new Baserow table would store standardized ad data (Spend, Clicks, Ad Sales per MSKU per day).
    -   The analytics dashboard could be expanded with a "PPC Performance" page showing metrics like ACOS and TACOS.
-   **[Placeholder] Per-Supplier Parameters:**
    -   A `Suppliers` table in Baserow can be created and linked to the `Products` table.
    -   This table can hold supplier-specific `Lead Time` and `MOQ` fields.
    -   The replenishment engine can be updated to use these specific values instead of the global defaults for more accurate planning.
-   **[Placeholder] Inventory Update from GRN:**
    -   The "Receive Stock/GRN" page's submission logic can be re-enabled to automatically update the main `Inventory` table in Baserow when goods are marked as received. This would require adding functions to the `po_module` to handle inventory adjustments.
