import logging
import os

def setup_logging(log_file_name='etl.log', log_level=logging.INFO, log_format='%(asctime)s - %(levelname)s - %(message)s'):
    """
    Sets up logging with a FileHandler and StreamHandler.
    
    Parameters:
    - log_file_name: Name of the log file.
    - log_level: Logging level.
    - log_format: Format of the log messages.
    
    Returns:
    - logger: Configured logger instance.
    """
    

    #Create directory logs if it does not exist
    log_directory = '/Users/jakegussler/Projects/quandl-zillow/logs'
    os.makedirs(log_directory, exist_ok=True)

    #Full path to the log file
    log_file_path = os.path.join(log_directory, log_file_name)

    #Create the logger
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    if not logger.handlers:

        #Create the formatter
        formatter = logging.Formatter(log_format)

        #Create the file handler
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        #Create the stream handler
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(log_level)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger