import pandas as pd
import os
import logging

def save_to_bronze(data, bronze_path='data/bronze/'):
    if not os.path.exists(bronze_path):
        os.makedirs(bronze_path)
    
    file_name = f"breweries_raw_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    file_path = os.path.join(bronze_path, file_name)
    
    df = pd.DataFrame(data)
    df.to_parquet(file_path, index=False)
    logging.info(f"Data extracted and saved on {file_name}")