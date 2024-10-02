import requests
from save_to_bronze import save_to_bronze
import pandas as pd
from transform_breweries import transform_to_silver
from partition_by_country import save_to_silver_partitioned_by_location
from sqlalchemy import create_engine

def extract_breweries_data(api_url: str):
    all_breweries = []
    page = 1
    per_page = 50
    while True:
        response = requests.get(api_url, params={'page': page, 'per_page': per_page})
        if response.status_code == 200:
            breweries = response.json()
            all_breweries.extend(breweries)
            
            print(len(all_breweries))
            
            if len(breweries) < per_page:
                break
            page += 1
        else:
            raise Exception(f"Error while extracting data from {api_url}: HTTP {response.status_code}")
        
    df = pd.DataFrame(all_breweries)
    return df
    
#save_to_bronze(extract_breweries_data('https://api.openbrewerydb.org/breweries'))

#df = pd.read_parquet('data/bronze/breweries_raw_20240930_215445.parquet')
#df.to_excel('teste.xlsx')

# df_silver = transform_to_silver(df)
# save_to_silver_partitioned_by_location(df_silver)

# df_usa = pd.read_parquet('data/silver/United States/breweries_United States_20241001_194805.parquet')
# engine = create_engine('sqlite:///:memory:')
# df_usa.to_sql('breweries', con=engine, if_exists='replace', index=False)
# query = "SELECT * FROM breweries;"
# closed_breweries = pd.read_sql_query(query, engine)
# print(closed_breweries)