import os
import requests
import pandas as pd

def fetch_breweries_data(api_url, page, per_page):
    """Fetch breweries data from the API."""
    response = requests.get(api_url, params={'page': page, 'per_page': per_page})
    if response.status_code != 200:
        raise Exception(f"Error while extracting data from {api_url}: HTTP {response.status_code}")
    return response.json()

def save_to_parquet(df, stage_path, file_name='breweries_data.parquet'):
    """Save the DataFrame to a Parquet file."""
    if not os.path.exists(stage_path):
        os.makedirs(stage_path)

    file_path = os.path.join(stage_path, file_name)
    try:
        df.to_parquet(file_path, index=False)
        print(f"Data extracted and saved at {file_path}")
    except Exception as e:
        print(f"Error saving DataFrame to Parquet: {e}")

def create_dummy_file(stage_path):
    """Create a dummy file if no data is fetched."""
    dummy_file_path = os.path.join(stage_path, 'dummy_file.txt')
    with open(dummy_file_path, 'w') as dummy_file:
        dummy_file.write("No data fetched from API.")
    print(f"Dummy file created at {dummy_file_path}")

def extract_breweries_data(api_url, stage_path):
    """Extract breweries data from API and save it to a Parquet file."""
    all_breweries = []
    page = 1
    per_page = 50

    while True:
        breweries = fetch_breweries_data(api_url, page, per_page)
        all_breweries.extend(breweries)
        print(f"Total breweries fetched: {len(all_breweries)}")

        if len(breweries) < per_page:
            break
        page += 1

    if not all_breweries:
        create_dummy_file(stage_path)
        return

    df = pd.DataFrame(all_breweries)
    save_to_parquet(df, stage_path)