import pandas as pd
import os
from utils.db_utils import get_engine
from utils.logger_utils import setup_logging
from utils.error_simulator import simulate_error

logger = setup_logging()

def ingest_csv_to_postgres(filepath: str, schema: str, table_name: str) -> None:

    """
    Ingest the data from a CSV file into a PostgreSQL database

    Parameters:
    filepath (str): The path to the CSV file
    schema (str): The schema to ingest the data into
    table_name (str): The name of the table_name to ingest the data into

    """

    #Database connection
    engine = get_engine()
    chunksize = 10000
    max_retries = 10
 
    for chunk_number, chunk in enumerate(pd.read_csv(filepath, chunksize=chunksize)):

        for attempt in range(max_retries):  
            try:
                ingest_df_to_postgres(df=chunk,schema=schema, table_name=table_name, engine=engine)
                logger.info(f"Chunk number {chunk_number} succesfully ingested into table {table_name}")
                break
            except Exception as e:
                logger.error(f"Error processing chunk {chunk_number}. Attempt {attempt + 1} of {max_retries}")
                if not (attempt < max_retries):
                    logger.error(f"Maximum number of attempts reached for chunk {chunk_number}")
                





def ingest_df_to_postgres(df, schema: str, table_name: str, engine) -> None:
    """
    Ingest a DataFrame into a PostgreSQL database
    Parameters:
    df (pd.DataFrame): The DataFrame to ingest
    schema (str): The schema to ingest to in the database
    table_name (str): The name of the table to ingest the data into
    engine (sqlalchemy.engine.base.Engine): The database engine to use
    """

    try:
        #Ingest the data into the database
        df.to_sql(table_name, engine, if_exists='append', schema=schema, index=False)
    except Exception as e:
        logger.error(f"An error occured while ingesting data into {table_name}: {str(e)}")
        raise

def ingest_all_csv_files_in_folder(folderpath: str, schema: str) -> None:
    """
    Ingest the Zillow CSV files into the PostgreSQL database
    Parameters:
    folderpath (str): The path to the folder containing the Zillow CSV files
    schema (str): The schema to ingest to in the database
    """

    for file in os.listdir(folderpath):
        if file.endswith(".csv"):
            filepath = f"{folderpath}/{file}"
            table_name = file.split('_')[0].lower() + "_" + file.split('_')[1].lower()
            ingest_csv_to_postgres(filepath=filepath, schema=schema, table_name=table_name)




if __name__ == "__main__":


    ingest_all_csv_files_in_folder(folderpath="/Users/jakegussler/Projects/quandl-zillow/data/unzipped", schema="raw")