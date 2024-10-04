import glob
import pandas as pd
import os

def save_breweries_to_silver_by_country(input_path, output_path):
    try:
        parquet_files = glob.glob(f"{input_path}/*.parquet")
        if not parquet_files:
            raise FileNotFoundError("No parquet files found in the input path.")
        df = pd.concat(pd.read_parquet(file) for file in parquet_files)
    except Exception as e:
        raise RuntimeError(f"Failed to read parquet files from {input_path}: {e}")

    countries = df['country'].unique()
    breweries_path = os.path.join(output_path, 'breweries')
    os.makedirs(breweries_path, exist_ok=True)

    for country in countries:
        country_df = df[df['country'] == country]
        country_folder = os.path.join(breweries_path, country)
        os.makedirs(country_folder, exist_ok=True)

        file_name = f"breweries_{country}.parquet"
        file_path = os.path.join(country_folder, file_name)

        try:
            country_df.to_parquet(file_path, index=False)
            print(f"Saved data for {country} at {file_path}")
        except Exception as e:
            print(f"Failed to save data for {country}: {e}")