import pandas as pd

def consolidate_address(df):
    df = df.drop(['address_1', 'address_2', 'address_3', 'state_province'], axis=1)
    return df

def remove_closed_breweries(df):
    mask = df['brewery_type'] != 'closed'
    df = df[mask]
    return df