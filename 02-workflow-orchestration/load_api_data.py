import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    df_list = []
    for m in range(10,13):
        url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_"+str(2020)+"-"+str(m)+".csv.gz"
        parse_dates=['lpep_pickup_datetime','lpep_dropoff_datetime']
        df=pd.read_csv(url, compression='gzip',sep=',',parse_dates=parse_dates)
        df_list.append(df)
    return pd.concat(df_list,ignore_index=True)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
