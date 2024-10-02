import pandas as pd

def transform_to_silver(df):
    df['address'] = df.apply(
        lambda x: ', '.join(filter(None, [x['address_1'], x['address_2'], x['street']])) 
        if x['address_1'] != x['street'] else ', '.join(filter(None, [x['address_2'], x['street']])),
        axis=1
    )
    
    df = df.drop(['address_1', 'address_2', 'address_3', 'street', 'state_province'], axis=1)
    
    return df
