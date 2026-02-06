import configparser
from sqlalchemy import create_engine

CONFIG = configparser.ConfigParser()
CONFIG.read('/data/prefect_mls_data/tools/config.ini')

HOST = CONFIG['PSQL']['HOST']
USER = CONFIG['PSQL']['USER']
PASSWORD = CONFIG['PSQL']['PASSWORD']
DB_NAME = CONFIG['PSQL']['DB_NAME']
PORT = CONFIG['PSQL']['PORT']


def engine():
    return create_engine(f'postgresql+psycopg://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}')
