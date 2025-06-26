from data_processing.baserow_fetcher import BaserowFetcher
from analytics_dashboard.kpi_calculations import get_sales_data
from utils.config_loader import APP_CONFIG # Assuming APP_CONFIG is loaded
from datetime import date

fetcher = BaserowFetcher(api_token=APP_CONFIG['baserow']['api_token'])
table_id = APP_CONFIG['baserow']['processed_sales_data_table_id']
start = date(2025, 6, 1) # Example date
end = date(2025, 6, 17)  # Example date

sales_df = get_sales_data(fetcher, table_id, start, end)
if sales_df is not None:
    print(f"Fetched {len(sales_df)} daily records.")
    print(sales_df.head())
    # Further checks:
    # print(sales_df[sales_df['Platform'] == 'Amazon'])
    print(sales_df.groupby(['Sale Date', 'Platform'])['Quantity Sold'].sum())