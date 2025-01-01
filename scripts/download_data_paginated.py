import requests
from dotenv import load_dotenv
import os
import pandas as pd
from logger_config import setup_logging
from sqlalchemy import create_engine
import datetime
import time
import gc
from database import get_engine
from config import DB_CONFIG, API_CONFIG


logger = setup_logging()


def paginated_getter(url):

    #Data chunk for keeping track of the number of data chunks, not passed into API
    page_number = 0

    table_name = url.split('/')[-1].lower()

    #Database connection
    engine = get_engine()

    with requests.session() as s:
        try:
            while(True):
    
                page_number+=1
                logger.info(f"Getting data chunk {page_number}---------------------------------")

                #Start time for the request
                start_time = datetime.datetime.now()

                response = get_response(url, API_CONFIG, s)

                json_response = response.json()

                request_end_time = datetime.datetime.now()

                df = process_response(json_response)
                if df is None:
                    logger.error(f"Failed to process data chunk {page_number}")
                    continue
                
                ingest_df_to_postgres(df, table_name, engine)

                logger.info(f"Data chunk {page_number} successfully ingested\n")

                ingest_end_time = datetime.datetime.now()

                log_processing_times(response, start_time, request_end_time, ingest_end_time, page_number)
                            
                #Check if there is a next cursor id
                if not json_response["meta"]["next_cursor_id"]:
                    break
                else:
                    #Add/Update the cursor id in the API config
                    API_CONFIG['qopts.cursor_id'] = json_response["meta"]["next_cursor_id"]
                    
        except requests.exceptions.HTTPError as e:
            logger.error(f"An error occured at page {page_number}: {str(e)}")
            raise

def log_processing_times(response, start_time, request_end_time, ingest_end_time, page_number):

    #Calculate the performance metrics
    response_size_kb = len(response.content)/1024
    kb_per_second = response_size_kb/(ingest_end_time-start_time).total_seconds()

    #Log the time taken to retrieve the data chunk
    logger.info(f"Request time: {request_end_time-start_time} seconds")
    logger.info(f"Processing time: {ingest_end_time-request_end_time} seconds")
    logger.info(f"{page_number} total time: {ingest_end_time-start_time} seconds")
    logger.info(f"Response size in KB: {response_size_kb} KB")
    logger.info(f"Response kb/s: {kb_per_second} KB/s\n")


def get_response(url, url_parameters,  request_session, max_retries=10, retry_delay=5, timeout=30):

    #Initial retry delay
    initial_retry_delay = retry_delay

    #Retry loop
    for attempt in range(max_retries):
        try:
            #Make the request to the API
            response = request_session.get(url, params=url_parameters, timeout=timeout)
            response.raise_for_status() #Raise an error for bad HTTP responses
            #Exit attempt loop if successful
            if response.status_code == 200:
                #Reset the retry delay
                retry_delay = initial_retry_delay
                return response
        
        except requests.exceptions.RequestException as e:
            logger.info(f'Attempt {attempt + 1} of {max_retries} failed: {str(e)}')
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                
                #Exponential backoff
                if attempt > 0:
                    retry_delay = round(retry_delay * 1.5, 0)
                
                #Wait for the retry delay
                time.sleep(retry_delay)
            else:
                print("Max retries reached. Returning None")
                return None

def process_response(json_response, max_retries=3):
    
    for attempt in range(max_retries):
        try:
            #Extract the data and columns from the json response
            data = json_response["datatable"]["data"]
            columns = [col["name"].lower() for col in json_response["datatable"]["columns"]]
            
            #Create a dataframe from the data and columns
            df = pd.DataFrame(data, columns=columns)
            return df
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} of {max_retries} failed: {str(e)}")
            if attempt == max_retries-1:
                logger.error("Max retries reached. Returning None")
                return None


def ingest_df_to_postgres(df, table_name, engine):
    
    schema = 'raw'
    try:
        #Ingest the data into the database
        df.to_sql(table_name, engine, if_exists='append', schema=schema, index=False)
    except Exception as e:
        logger.error(f"An error occured while ingesting data into {table_name}: {str(e)}")
        raise

def download_zillow_tables():

    base_url =  "https://data.nasdaq.com/api/v3/datatables/ZILLOW/"
    tables = [ 'INDICATORS', 'REGIONS', 'DATA' ]


    #Iterate through the tables
    for table in tables:
        url = base_url + table

        #Get the data from the API
        logger.info(f"Downloading data from {url}")
        paginated_getter(url)


def main() -> None:
    #Load the environment variables
    load_dotenv()



    #Download the zillow tables
    download_zillow_tables()

if __name__ == "__main__":
    main()

    
