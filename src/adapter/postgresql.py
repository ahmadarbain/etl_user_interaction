import os
import aiopg
import pandas as pd
import urllib.parse
from dotenv import load_dotenv
from src.interface.database import DatabaseInterface

load_dotenv()

class _PostgreSQL(DatabaseInterface):

    def __init__(self, credentials: dict):

        self.host = credentials['host']
        self.port = credentials['port']
        self.user = credentials['user']
        self.password = credentials['password']
        self.db = credentials['db']
        self.connection = None

    async def connect(self) -> aiopg.Connection:
        """Mysql: open database connection'
        """
        dsn = f"dbname={self.db} user={self.user} password={self.password} host={self.host} port={self.port}"
        self.connection = await aiopg.create_pool(dsn)
                
    async def get_query(self, query: str) -> (list, list):
        async with self.connection.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                column_names = [i[0] for i in cur.description]
                r = await cur.fetchall()
                return (column_names, list(r))
            
    async def insert_data(self, df: pd.DataFrame, table: str) -> None:
        async with self.connection.acquire() as conn:
            async with conn.cursor() as cur:
                cols = " , ".join([c for c in df.columns.tolist()])
                try:
                    for _, row in df.iterrows():
                        query = f"insert into {table} ("+ cols +") VALUES ("+ "%s," * (len(row) - 1) + "%s" + ") ON CONFLICT (creator_id) DO NOTHING"
                        await cur.execute(query, tuple(row))
                except Exception as e:
                    raise e
        
    async def close(self) -> None :
        """Mysql: close database connection
        """
        self.connection.close()


def PostgreSQL(credentials: dict) -> DatabaseInterface:
    """Generate new Mysql class

    Returns:
        DatabaseInterface: Mysql class that implement Database interface
    """
    return _PostgreSQL(credentials)
