import pandas as pd
import requests
import boto3
import awswrangler as wr


def export_data_from_api(source):
    """
    This function takes the data from an api endpoint and returns it as a df.
    source: api endpoint
    """
    try:
        boto3.resource('s3')

        r = requests.get(source)

        return pd.DataFrame(r.json())
    except Exception as e:
        print(f'Data could not be extracted because of {e}')
        return None


def export_df_to_s3(df, destination):
    """
    This function takes in a dataframe and stores it as parquet in a cloud storage # noqa
    df: datframe to be exported
    destination: s3 bucket storage link
    """
    try:
        wr.s3.to_parquet(
            df=df.copy(),
            path=destination
        )
    except Exception as e:
        print(f'Data could not be exported to s3 because of {e}')
        return None


def extract_from_s3(path):
    """
    This function takes in an s3 path to a dataset(parquet/csv) and stores the output as a dataframe # noqa
    """
    return wr.s3.read_parquet(
        path=path
    )
