import os
import pandas as pd
import logging

import os
import pandas as pd
import logging

def save_to_silver_partitioned_by_location(df, silver_path='data/silver/'):
    if not os.path.exists(silver_path):
        os.makedirs(silver_path)
    
    for country, country_df in df.groupby('country'):
        country_folder = os.path.join(silver_path, country)
        if not os.path.exists(country_folder):
            os.makedirs(country_folder)
        
        file_name = f"breweries_{country}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        file_path = os.path.join(country_folder, file_name)
        
        country_df.to_parquet(file_path, index=False)
        logging.info(f"Saved data for {country} at {file_path}")