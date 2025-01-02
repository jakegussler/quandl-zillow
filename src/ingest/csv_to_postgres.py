import pandas as pd
from src.utils.db_utils import get_engine
from src.utils.logger_utils import setup_logging

def csv_to_postgres(csv_filepath: str, table_name: str) -> None:

    """
    Ingest the data from a CSV file into a PostgreSQL database

    Parameters:
    csv_filepath (str): The path to the CSV file
    table_name (str): The name of the table to ingest the data into

    """

    #Data chunk for keeping track of the number of data chunks
    page_number = 0

    #Database connection
    engine = get_engine()
    chunk_size = 10000

    try:
        
        for chunk_number, chunk in enumerate(pd.read_csv(file_path, chunksize=chunksize)):
            print(f"Processing chunk {chunk_number + 1}")


    except:
