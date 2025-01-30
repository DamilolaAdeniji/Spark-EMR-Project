import requests # type: ignore
import logging
import pandas as pd

from s3_functions import export_df_to_s3

# Configure logging so we would always see it regardless of the enviorment being used
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def data_generator(max_records: int) -> list[dict]:
    """Fetches 311-calls data from the API in batches until max_records is reached.

    It can generate up tp 7.3M rows of data.
    You can preview the data here --> https://data.sfgov.org/City-Infrastructure/311-Cases/vw6y-z8j6/data_preview
    """
    
    data = []
    offset = 0
    iteration = 0

    while True:
        url = f'https://data.sfgov.org/resource/vw6y-z8j6.json?$limit=5000&$offset={offset}'
        response = requests.get(url)
        
        if response.status_code == 200:
            batch = response.json()
            data.extend(batch)
            iteration += 1
            offset += 1000
            logging.info(f'Currently on {iteration} iteration: Retrieved {len(batch)} records, Total records: {len(data)}')
        else:
            logging.error(f'Failed to fetch data at offset {offset}, Status Code: {response.status_code}')
            break
        if len(data) >= max_records:
            logging.info(f'Reached max_records limit: {max_records} ----- breaking now')
            break
    
    return data

if __name__ == "__main__":
    data = data_generator(20000)

    data = pd.DataFrame(data)

    export_df_to_s3(data, 's3://emr-cluster-project/raw/311-calls-data.parquet')