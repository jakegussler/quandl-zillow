from dotenv import load_dotenv
import pandas as pd
import nasdaqdatalink as ndl
from sqlalchemy import create_engine
import datetime
import time
from config import API_CONFIG
from logger_config import setup_logging


#Load the environment variables
load_dotenv()

logger = setup_logging()

timestamps = {
    "start_time": None,
    "download_end_time": None,
    "ingest_end_time": None
}

def get_nasdaq_data_zip(database_code, dataset_code, zip_filepath, max_retries=10, retry_delay=5) -> None:

    """
    Get the data from the NASDAQ Data Link API as a ZIP file
    """

    logger.info(f"Downloading {database_code}_{dataset_code}.zip...")

    #Set the API Key
    ndl.ApiConfig.api_key = API_CONFIG['api_key']
    

    data_code = f"{database_code}/{dataset_code}"
    filename = f"{zip_filepath}/{database_code}_{dataset_code}.zip"

    timestamps["start_time"] = datetime.datetime.now()
    logger.info(f"Download start time: {timestamps['start_time']}")

    #Get the data from the API as a ZIP file
    for attempt in range(max_retries):
        try:
            #Make the request to the API
            data = ndl.export_table(data_code, filename=filename)

            timestamps["download_end_time"] = datetime.datetime.now()
            logger.info(f"Download end time: {timestamps['download_end_time']}")
            break
        except Exception as e:
            logger.info(f'Attempt {attempt + 1} of {max_retries} failed: {str(e)}')
            if attempt < max_retries:
                time.sleep(retry_delay)
            else:
                logger.warning("Max retries reached. Returning None")
                raise

def download_nasdaq_database_zips(database_code: str, dataset_codes: list, zip_filepath: str) -> None:
    
    """
    Download the Zillow tables from the NASDAQ Data Link API
    """

    #Iterate through the tables
    for dataset_code in dataset_codes:

        #Get the data from the API
        get_nasdaq_data_zip(database_code, dataset_code)

def main() -> None:

    ##Set the parameters for Zillow downloading, change to generic later and move Zillow info to zillow ETL script
    zip_filepath = "/Users/jakegussler/Projects/quandl-zillow/data/raw"
    database_code = "ZILLOW"
    dataset_codes = ['DATA', 'INDICATORS', 'REGIONS']

    #Download the zillow tables
    download_nasdaq_database_zips(database_code, dataset_codes, zip_filepath)

if __name__ == "__main__":
    main()


    
