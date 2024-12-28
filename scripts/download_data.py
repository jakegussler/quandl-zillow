import requests
from dotenv import load_dotenv
import os
import pandas as pd
import logging
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def paginated_getter(url):

    #Data chunk for keeping track of the number of data chunks, not passed into API
    page_number = 0
    
    #Initialise an empty list to store all the dataframes
    all_dfs = []
    
    #Parameters to be passed to the API
    url_parameters = {
        'api_key':os.getenv('QUANDL_API_KEY'),
    }
    try:
        while(True):
            
            #Log statement to keep track of the data chunk
            page_number+=1
            logger.info(f"Getting data chunk {page_number}")
            
            #Make the request to the API
            response = requests.get(url, params=url_parameters)
            
            #Check if the request was successful
            response.raise_for_status()
            
            #Convert the response to a json
            json_response = response.json()
            
            #Check if the response is empty
            if not json_response["datatable"]["data"]:
                break

            #Process the response
            df = process_response(json_response)
            
            #Append the dataframe to the list
            all_dfs.append(df)
            
            #Check if there is a next cursor id
            if not json_response["meta"]["next_cursor_id"]:
                break
            else:
                #Add/Update the cursor id in the URL parameters
                url_parameters['qopts.cursor_id'] = json_response["meta"]["next_cursor_id"]
    except requests.exceptions.HTTPError as e:
        logger.error(f"An error occured at page {page_number}: {str(e)}")
        raise
    finally:
        if all_dfs:
            logger.info(f"Total pages of data retrieved: {len(all_dfs)}")
            logger.info("Combining all data into a single dataframe...")
            final_df = pd.concat(all_dfs, ignore_index=True)
            return final_df
        else:
            logger.error("No data was retrieved")
            return None


def process_response(json_response, max_retries=3):
    
    for attempt in range(max_retries):
        try:
            #Extract the data and columns from the json response
            data = json_response["datatable"]["data"]
            columns = (col["name"] for col in json_response["datatable"]["columns"])
            
            #Create a dataframe from the data and columns
            df = pd.DataFrame(data, columns=columns)
            return df
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} of {max_retries} failed: {str(e)}")
            if attempt == max_retries-1:
                logger.error("Max retries reached. Returning None")
                return None


def ingest_df_to_postgres(df, table_name, engine):
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f"Data successfully ingested into {table_name}")
    except Exception as e:
        logger.error(f"An error occured while ingesting data into {table_name}: {str(e)}")
        raise

def download_zillow_tables():

    base_url =  "https://data.nasdaq.com/api/v3/datatables/ZILLOW/"
    tables = ['DATA', 'INDICATORS', 'REGIONS']
    zillow_user = os.getenv('ZILLOW_USER')
    zillow_password = os.getenv('ZILLOW_PASSWORD')
    db_name =  os.getenv('POSTGRES_DATABASE')

    # Database connection
    engine = create_engine(f'postgresql://{zillow_user}:{zillow_password}@localhost:5432/{db_name}')

    #Iterate through the tables
    for table in tables:
        url = base_url + table

        #Get the data from the API
        logger.info(f"Downloading data from {url}")
        df = paginated_getter(url)
        logger.info(f"Total rows downloaded from {table}: {df.shape[0]}")

        #Ingest the data into the database
        ingest_df_to_postgres(df, table, engine)





if __name__ == "__main__":
    #Load the environment variables
    load_dotenv()

    #Download the zillow tables
    download_zillow_tables()

    
