import pandas as pd
import os
import glob

def create_gold_view(input_path, output_path):
    input_path_breweries = os.path.join(input_path, 'breweries', '**', '*.parquet')
    parquet_files = glob.glob(input_path_breweries, recursive=True)

    if not parquet_files:
        raise FileNotFoundError("No parquet files found in the breweries subfolders.")

    df = pd.concat(pd.read_parquet(file) for file in parquet_files)
    print(f"DataFrame columns: {df.columns.tolist()}")
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        print(f"Output directory created at: {output_path}")

    df_output_path = os.path.join(output_path, 'original_breweries.parquet')
    df.to_parquet(df_output_path, index=False) 
    print(f"Original DataFrame written and saved at {df_output_path}")

    gold_df = df.groupby(['brewery_type', 'state']).agg(brewery_count=('id', 'count')).reset_index()

    gold_output_path = os.path.join(output_path, 'gold_view.parquet')  
    gold_df.to_parquet(gold_output_path, index=False) 

    print(f"Gold view created and saved at {gold_output_path}")