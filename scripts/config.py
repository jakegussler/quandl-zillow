import os
from dotenv import load_dotenv


load_dotenv()

DB_CONFIG = {
    'type': os.getenv('DB_TYPE', 'postgresql'),
    'user': os.getenv('DB_USER', 'zillow_user'),
    'password': os.getenv('DB_PASSWORD', 'zillow_password'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'zillow_analytics')
}

API_CONFIG = {
    'api_key': os.getenv('NASDAQ_DATA_LINK_API_KEY')
}