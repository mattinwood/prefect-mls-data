import configparser
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

CONFIG = configparser.ConfigParser()
CONFIG.read('/data/prefect_mls_data/tools/config.ini')

HOST = CONFIG['PSQL']['HOST']
USER = CONFIG['PSQL']['USER']
PASSWORD = CONFIG['PSQL']['PASSWORD']
DB_NAME = CONFIG['PSQL']['DB_NAME']
PORT = CONFIG['PSQL']['PORT']


def engine():
    return create_engine(
        f'postgresql+psycopg://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}',
        pool_size=200,  # Up from 5
        max_overflow = 50,  # Up from 10
        pool_pre_ping = True,  # Verify connections before use
        pool_recycle = 3600  # Recycle connections after 1 hour
                         )
