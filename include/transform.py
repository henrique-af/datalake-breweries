import pandas as pd
import os

def transform(input_path: str, output_path: str) -> None:
    dataframes = []

    for file in os.listdir(input_path):
        if file.endswith('.parquet'):
            file_path = os.path.join(input_path, file)
            try:
                df = pd.read_parquet(file_path)
                dataframes.append(df)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")

    if not dataframes:
        raise ValueError("No parquet files found in the specified input path.")

    df = pd.concat(dataframes, ignore_index=True)

    df.fillna({"address_1": "", "address_2": "", "street": "", "state_province": ""}, inplace=True)
    
    df['address'] = df[['address_1', 'address_2', 'street']].agg(lambda x: ', '.join(pd.Series(x[x != ""]).unique()), axis=1)

    df['state'] = df.apply(lambda x: x['state'] if x['state'] != x['state_province'] else x['state'], axis=1)

    df.drop(columns=['address_1', 'address_2', 'address_3', 'street', 'state_province'], inplace=True)

    os.makedirs(output_path, exist_ok=True)

    output_file_path = os.path.join(output_path, 'breweries_staged.parquet')
    try:
        df.to_parquet(output_file_path, index=False)
        print(f"Transformed data saved to {output_file_path}")
    except Exception as e:
        print(f"Error saving DataFrame to parquet: {e}")
