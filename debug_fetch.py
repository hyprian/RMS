# # RMS/debug_fetch.py
# import requests
# import json

# # --- CONFIGURATION ---
# # Use the exact credentials you tested with curl
# # BASEROW_URL = "https://api.baserow.io"
# # API_TOKEN = "7AB494siWdT7we5b09jyVVBpIh8nsg5t"
# # TABLE_ID = 616275 # The problematic table ID

# BASEROW_URL = "http://129.159.18.115:49161"
# API_TOKEN = "PfMJRl1I6CVwHPPLQ578vHmxvCR6fVMB"
# TABLE_ID = 697 # The problematic table ID 

# # --- TEST FUNCTION ---
# def test_fetch_rows(table_id):
#     """A minimal function to test the raw API call."""
#     headers = {
#         "Authorization": f"Token {API_TOKEN}",
#     }
#     # Construct the exact URL the app uses
#     url = f"{BASEROW_URL}/api/database/rows/table/{table_id}/?user_field_names=true&size=5" # Fetch only 5 rows to test

#     print(f"--- Testing Fetch for Table ID: {table_id} ---")
#     print(f"Requesting URL: {url}")
    
#     response = None
#     try:
#         response = requests.get(url, headers=headers)
#         print(f"Response Status Code: {response.status_code}")
        
#         # Raise an error if the status code is bad (4xx or 5xx)
#         response.raise_for_status()
        
#         # Try to parse the JSON response
#         data = response.json()
        
#         print("\n--- SUCCESS ---")
#         print(f"API call successful. Found {data.get('count', 'N/A')} total rows.")
        
#         results = data.get('results', [])
#         print(f"Fetched {len(results)} rows in the first page.")
        
#         if results:
#             print("\nSample of the first row fetched:")
#             # Pretty print the first result
#             print(json.dumps(results[0], indent=2))
            
#             # Get the column names (keys) from the first result
#             columns = list(results[0].keys())
#             print("\nColumn Names found in the data:")
#             print(columns)
#         else:
#             print("\nWARNING: The API returned 0 rows. Is the table empty or are there permission issues?")

#     except requests.exceptions.HTTPError as http_err:
#         print("\n--- HTTP ERROR ---")
#         print(f"An HTTP error occurred: {http_err}")
#         if response is not None:
#             print("Response Body:")
#             print(response.text)
#     except Exception as e:
#         print(f"\n--- OTHER ERROR ---")
#         print(f"An unexpected error occurred: {e}")

# # --- RUN THE TEST ---
# if __name__ == "__main__":
#     test_fetch_rows(TABLE_ID)


# baserow_speed_test.py
import requests
import time

# --- CONFIGURATION ---

# SaaS Baserow
SAAS_URL = "https://api.baserow.io"
SAAS_TOKEN = "7AB494siWdT7we5b09jyVVBpIh8nsg5t"
SAAS_TABLE_ID = 616396  # SaaS table ID

# Self-hosted Baserow
SELF_URL = "http://129.159.18.115:49161"
SELF_TOKEN = "PfMJRl1I6CVwHPPLQ578vHmxvCR6fVMB"
SELF_TABLE_ID = 697  # Self-hosted table ID

# Number of rows to fetch in test
PAGE_SIZE  = 100


def fetch_all_rows(base_url, token, table_id):
    """Fetch ALL rows from a table and measure total time."""
    headers = {
        "Authorization": f"Token {token}",
    }
    url = f"{base_url}/api/database/rows/table/{table_id}/?user_field_names=true&size={PAGE_SIZE}"
    all_rows = []
    page = 1

    start_time = time.perf_counter()

    while True:
        paginated_url = f"{url}&page={page}"
        response = requests.get(paginated_url, headers=headers)
        elapsed = time.perf_counter()

        try:
            response.raise_for_status()
        except Exception:
            print(f"❌ Error fetching page {page}: Status {response.status_code}")
            break

        data = response.json()
        results = data.get("results", [])
        all_rows.extend(results)

        if not data.get("next"):
            break  # No more pages
        page += 1

    total_time = time.perf_counter() - start_time
    return total_time, len(all_rows)


if __name__ == "__main__":
    print("=== Full Table Fetch Speed Test ===\n")

    # SaaS
    print("[SaaS Baserow]")
    saas_time, saas_rows = fetch_all_rows(SAAS_URL, SAAS_TOKEN, SAAS_TABLE_ID)
    print(f"Total Rows Fetched: {saas_rows}")
    print(f"Total Time Taken: {saas_time:.4f} seconds\n")

    # Self-hosted
    print("[Self-hosted Baserow]")
    self_time, self_rows = fetch_all_rows(SELF_URL, SELF_TOKEN, SELF_TABLE_ID)
    print(f"Total Rows Fetched: {self_rows}")
    print(f"Total Time Taken: {self_time:.4f} seconds\n")

    # Comparison
    if saas_time and self_time:
        if saas_time < self_time:
            print(f"✅ SaaS is faster by {self_time - saas_time:.4f} seconds.")
        elif self_time < saas_time:
            print(f"✅ Self-hosted is faster by {saas_time - self_time:.4f} seconds.")
        else:
            print("⚖️ Both are equally fast.")