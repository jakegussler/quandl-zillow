import requests
from dotenv import load_dotenv
import os


def paginated_getter(url):
    #Set variables
    cursor = 0


    url_paramters = {
        'api_key':os.getenv('QUANDL_API_KEY'),
        'qopts.cursor_id':1
    }

    while(True):
        response = requests.get(url, params=url_paramters)
        requests.raise_for_status()

##### Currently trying to figure out how to get cursor id working to enable participation. Continue reading Quandl docs

def download_zillow_tables():

    base_url =  "https://data.nasdaq.com/api/v3/datatables/ZILLOW/"
    tables = ['DATA', 'INDICATORS', 'REGIONS']


    for table in tables:
        url = base_url + table
        table_data = paginated_getter(url)



if __name__ == "__main__":

    load_dotenv()
    download_zillow_tables()

    
