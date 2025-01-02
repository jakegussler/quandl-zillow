import zipfile
from logger_config import setup_logging
import os

logger = setup_logging()

def unzip_file(filepath: str, output_dir: str) -> None:
    """
    Unzip the data from the NASDAQ Data Link API
    """

    logger.info(f"Unzipping {filepath} to {output_dir}...")

    #Unzip the file
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        zip_ref.extractall(output_dir)
        
    logger.info(f"Unzipped {filepath} to {output_dir}")
    
    return None

def unzip_all_in_folder(folderpath: str, output_dir: str) -> None:

    """
    Unzip all the files in a folder
    """

    logger.info(f"Unzipping all files in {folderpath} to {output_dir}...")

    #Unzip all the files in the folder
    for file in os.listdir(folderpath):
        if file.endswith('.zip'):
            filepath = os.path.join(folderpath, file)
            unzip_file(filepath, output_dir)


if __name__ == '__main__':

    #Unzip the data
    folderpath = '/Users/jakegussler/Projects/quandl-zillow/data/raw'
    output_dir = '/Users/jakegussler/Projects/quandl-zillow/data/unzipped'

    unzip_all_in_folder(folderpath, output_dir)


