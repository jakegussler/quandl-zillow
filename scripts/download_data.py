import requests
from dotenv import load_dotenv
import os
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def paginated_getter(url):

    #Data chunk for keeping track of the number of data chunks
    data_chunk = 0
    
    #Initialise an empty list to store all the dataframes
    all_dfs = []
    
    #Parameters to be passed to the API
    url_parameters = {
        'api_key':os.getenv('QUANDL_API_KEY'),
        #'qopts.cursor_id':'1',
        #'qopts.export':'true'
    }

    while(True):
        
        #Log statement to keep track of the data chunk
        data_chunk+=1
        logger.info(f"Getting data chunk {data_chunk}")
        
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
            #Update the cursor id
            url_parameters['qopts.cursor_id'] = json_response["meta"]["next_cursor_id"]

    logger.info(f"Total data chunks retrieved: {len(all_dfs)}")

def process_response(json_response):
    
    #Extract the data and columns from the json response
    data = json_response["datatable"]["data"]
    columns = (col["name"] for col in json_response["datatable"]["columns"])
    
    #Create a dataframe from the data and columns
    df = pd.DataFrame(data, columns=columns)
    return df

def download_zillow_tables():

    base_url =  "https://data.nasdaq.com/api/v3/datatables/ZILLOW/"
    tables = ['DATA', 'INDICATORS', 'REGIONS']

    #Iterate through the tables
    for table in tables:
        url = base_url + table
        table_data = paginated_getter(url)



if __name__ == "__main__":
    #Load the environment variables
    load_dotenv()

    #Download the zillow tables
    download_zillow_tables()

    
