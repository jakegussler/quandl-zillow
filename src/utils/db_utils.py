from sqlalchemy import create_engine
from utils.config import DB_CONFIG

def get_engine():
    connection_string = f'{DB_CONFIG["type"]}://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database"]}'
    return create_engine(connection_string)