import requests
from dotenv import load_dotenv
import os
import pandas as pd
import logging
from sqlalchemy import create_engine
import datetime
import time
import gc
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
from IPython.display import clear_output

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def paginated_getter(url, max_retries=10, retry_delay=5):

    #Data chunk for keeping track of the number of data chunks, not passed into API
    page_number = 0
    
    #Performance data
    page_numbers = []
    kb_per_seconds = []



    #Parameters to be passed to the API
    url_parameters = {
        'api_key':os.getenv('QUANDL_API_KEY'),
    }
    
    table_name = url.split('/')[-1].lower()

    #Initial retry delay
    initial_retry_delay = retry_delay

    #Database connection
    engine = create_engine(f'{db_config["type"]}://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}')

    s = requests.session()

    #Create plot for performance metrics
    plt.ion()
    fig, ax = plt.subplots(figsize=(10, 6))
    line = ax.plot(page_numbers, kb_per_seconds, label='kb/s')[0]  
    ax.set_xlabel('Page Number')
    ax.set_ylabel('KB/s')
    ax.set_title('API Performance Monitor')
    plt.grid(True) 


    try:
        while(True):
   
            #Log statement to keep track of the data chunk
            page_number+=1
            logger.info(f"Getting data chunk {page_number}---------------------------------")

            #Start time for the request
            start_time = datetime.datetime.now()

            #Retry loop
            for attempt in range(max_retries):
                try:
                    #Make the request to the API
                    response = s.get(url, params=url_parameters, timeout=10)
                    response.raise_for_status() #Raise an error for bad HTTP responses
                    #Exit attempt loop if successful
                    if response.status_code == 200:
                        break
               
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

            #Convert the response to a json
            json_response = response.json()
            
            #Check if the response is empty
            if not json_response["datatable"]["data"]:
                break
            
            #Log the time taken to retrieve the data chunk
            request_end_time = datetime.datetime.now()
            

            #Process the response
            df = process_response(json_response)
            
            #Ingest the dataframe into the database
            ingest_df_to_postgres(df, table_name, engine)

            logger.info(f"Data chunk {page_number} successfully ingested\n")

            ingest_end_time = datetime.datetime.now()

            #Calculate the performance metrics
            response_size_kb = len(response.content)/1024
            kb_per_second = response_size_kb/(ingest_end_time-start_time).total_seconds()

            #Update the pefformance data lists
            page_numbers.append(page_number)
            kb_per_seconds.append(kb_per_second)

            #Update the plot
            line.set_xdata(page_numbers)
            line.set_ydata(kb_per_seconds)

            #Update the plot limits
            ax.relim()
            ax.autoscale_view()

            #Draw the updated plot
            fig.canvas.draw()
            fig.canvas.flush_events()

            #Log the time taken to retrieve the data chunk
            logger.info(f"Request time: {request_end_time-start_time} seconds")
            logger.info(f"{page_number} total time: {ingest_end_time-start_time} seconds")
            logger.info(f"Response size in KB: {response_size_kb} KB")
            logger.info(f"Response kb/s: {kb_per_second} KB/s\n")


            #Reset the retry delay
            retry_delay = initial_retry_delay
            
            #Clean up the dataframe
            del df
                        
            #Check if there is a next cursor id
            if not json_response["meta"]["next_cursor_id"]:
                gc.collect()
                break
            else:
                #Add/Update the cursor id in the URL parameters
                url_parameters['qopts.cursor_id'] = json_response["meta"]["next_cursor_id"]
                
                gc.collect()

                #Wait for a second before making the next request
                time.sleep(2)

    

    except requests.exceptions.HTTPError as e:
        logger.error(f"An error occured at page {page_number}: {str(e)}")
        raise



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
    tables = ['DATA', 'INDICATORS', 'REGIONS']


    #Iterate through the tables
    for table in tables:
        url = base_url + table

        #Get the data from the API
        logger.info(f"Downloading data from {url}")
        paginated_getter(url)






if __name__ == "__main__":
    #Load the environment variables
    load_dotenv()
    

    db_config = {
        'type': os.getenv('DB_TYPE', 'postgresql'),
        'user':os.getenv('DB_USER', 'zillow_user'),
        'password':os.getenv('DB_PASSWORD', 'zillow_password'),
        'host':os.getenv('DB_HOST', 'localhost'),
        'port':os.getenv('DB_PORT', '5432'),
        'database':os.getenv('DB_NAME', 'zillow_analytics')
    }


    #Download the zillow tables
    download_zillow_tables()

    
