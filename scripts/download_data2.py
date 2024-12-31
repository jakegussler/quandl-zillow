import requests
from dotenv import load_dotenv
import os
import pandas as pd
import nasdaqdatalink as ndl
import logging
from sqlalchemy import create_engine
import datetime
import time
import gc

###This version of download_data uses the NASDAQ Data Link library to download the entire 
###Zillow dataset from the NASDAQ Data Link API as a CSV file.

##Requires NASDAQ_DATA_LINK_API_KEY to be set in the environment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_nasdaq_data(database_code, dataset_code, max_retries=10, retry_delay=5):

    #Set the API Key
    ndl.ApiConfig.api_key = os.getenv('NASDAQ_DATA_LINK_API_KEY')
    
    data_code = f"{database_code}/{dataset_code}"

    #Get the data from the API as a ZIP file
    for attempt in range(max_retries):
        try:
            #Make the request to the API
            data = ndl.export_table(data_code)
            break
        except Exception as e:
            logger.info(f'Attempt {attempt + 1} of {max_retries} failed: {str(e)}')
            if attempt < max_retries:
                time.sleep(retry_delay)
            else:
                raise



def download_zillow_tables():

    #base_url =  "https://data.nasdaq.com/api/v3/datatables/ZILLOW/"
    database_code = "ZILLOW"
    dataset_codes = ['DATA', 'INDICATORS', 'REGIONS']

    #Iterate through the tables
    for dataset_code in dataset_codes:

        #Get the data from the API
        logger.info(f"Downloading data for {dataset_code}")
        get_nasdaq_data(database_code, dataset_code)


if __name__ == "__main__":
    
    #Load the environment variables
    load_dotenv()
    
    #Download the zillow tables
    download_zillow_tables()

    
