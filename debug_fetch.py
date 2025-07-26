# RMS/debug_fetch.py
import requests
import json

# --- CONFIGURATION ---
# Use the exact credentials you tested with curl
BASEROW_URL = "https://api.baserow.io"
API_TOKEN = "7AB494siWdT7we5b09jyVVBpIh8nsg5t"
TABLE_ID = 616396 # The problematic table ID

# --- TEST FUNCTION ---
def test_fetch_rows(table_id):
    """A minimal function to test the raw API call."""
    headers = {
        "Authorization": f"Token {API_TOKEN}",
    }
    # Construct the exact URL the app uses
    url = f"{BASEROW_URL}/api/database/rows/table/{table_id}/?user_field_names=true&size=5" # Fetch only 5 rows to test

    print(f"--- Testing Fetch for Table ID: {table_id} ---")
    print(f"Requesting URL: {url}")
    
    response = None
    try:
        response = requests.get(url, headers=headers)
        print(f"Response Status Code: {response.status_code}")
        
        # Raise an error if the status code is bad (4xx or 5xx)
        response.raise_for_status()
        
        # Try to parse the JSON response
        data = response.json()
        
        print("\n--- SUCCESS ---")
        print(f"API call successful. Found {data.get('count', 'N/A')} total rows.")
        
        results = data.get('results', [])
        print(f"Fetched {len(results)} rows in the first page.")
        
        if results:
            print("\nSample of the first row fetched:")
            # Pretty print the first result
            print(json.dumps(results[0], indent=2))
            
            # Get the column names (keys) from the first result
            columns = list(results[0].keys())
            print("\nColumn Names found in the data:")
            print(columns)
        else:
            print("\nWARNING: The API returned 0 rows. Is the table empty or are there permission issues?")

    except requests.exceptions.HTTPError as http_err:
        print("\n--- HTTP ERROR ---")
        print(f"An HTTP error occurred: {http_err}")
        if response is not None:
            print("Response Body:")
            print(response.text)
    except Exception as e:
        print(f"\n--- OTHER ERROR ---")
        print(f"An unexpected error occurred: {e}")

# --- RUN THE TEST ---
if __name__ == "__main__":
    test_fetch_rows(TABLE_ID)