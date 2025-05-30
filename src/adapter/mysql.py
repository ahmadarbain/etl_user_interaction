import os
import aiomysql
import urllib.parse
import pandas as pd
from dotenv import load_dotenv
from src.interface.database import DatabaseInterface
from zope.interface import implementer

load_dotenv()

@implementer(DatabaseInterface)
class _MySQL:

    def __init__(self, credentials: dict):
        self.host = credentials['host']
        self.port = credentials['port']
        self.user = credentials['user']
        self.password = credentials['password']
        self.db = credentials['db']
        self.connection = None

    async def connect(self) -> aiomysql.Connection:
        """Mysql: open database connection'
        """
        conf = {
            'host': self.host,
            'port': self.port,
            'db':self.db,
            'user': self.user,
            'password': self.password,
        }
        self.connection = await aiomysql.connect(**conf)
    
    async def get_query(self, query: str) -> (list, list): # type: ignore
        async with self.connection.cursor() as cur:
            await cur.execute(query)
            column_names = [i[0] for i in cur.description]
            r = await cur.fetchall()
            return (column_names, list(r))
    
    async def create_table_if_not_exists(self, table_name: str, create_sql: str) -> None:
        """Create table if it doesn't exist."""
        async with self.connection.cursor() as cur:
            try:
                await cur.execute(create_sql)
            except Exception as e:
                await self.connection.rollback()
                raise e
            else:
                await self.connection.commit()

    async def insert_user_status(self, df: pd.DataFrame, table: str) -> None:
        async with self.connection.cursor() as cur:
            truncate_table_query = f"truncate table {table}"

            await cur.execute(truncate_table_query)

            cols = " , ".join([c for c in df.columns.tolist()])
            try:
                for _, row in df.iterrows():
                    query = f"replace into {table} ("+ cols +") VALUES ("+ "%s," * (len(row) - 1) + "%s" + ")"
                    await cur.execute(query, tuple(row))
            except Exception as e:
                await self.connection.rollback()
                raise e
            else:
                await self.connection.commit()

    async def insert_data(self, df: pd.DataFrame, table: str) -> None:
        async with self.connection.cursor() as cur:
            cols = " , ".join([c for c in df.columns.tolist()])
            try:
                for _, row in df.iterrows():
                    query = f"replace into {table} ("+ cols +") VALUES ("+ "%s," * (len(row) - 1) + "%s" + ")"
                    await cur.execute(query, tuple(row))
            except Exception as e:
                await self.connection.rollback()
                raise e
            else:
                await self.connection.commit()

    async def update_data_day(self, df: pd.DataFrame) -> None:
        async with self.connection.cursor() as cur:
            try:
                drop_table_query = "drop table if exists temp_daily_monetization"

                await cur.execute(drop_table_query)

                create_table_query = """

                CREATE TABLE IF NOT EXISTS `temp_daily_monetization` (
                `id` bigint NOT NULL AUTO_INCREMENT,
                `content_id` int NOT NULL,
                `total_duration` decimal(20,2) NOT NULL,
                `creator_id` int NOT NULL,
                `claimer_id` int DEFAULT NULL,
                `label_id` int DEFAULT NULL,
                `earning_duration_creator` decimal(20,2) DEFAULT NULL,
                `earning_duration_claimer` decimal(20,2) DEFAULT NULL,
                `earning_duration_label` decimal(20,2) DEFAULT NULL,
                `earning_duration_rplus` decimal(20,2) DEFAULT NULL,
                `date` date NOT NULL,
                PRIMARY KEY (`id`)
                )"""

                await cur.execute(create_table_query)

                cols = " , ".join([c for c in df.columns.tolist()])
                for _, row in df.iterrows():
                    query = "insert into temp_daily_monetization ("+ cols +") VALUES ("+ "%s," * (len(row) - 1) + "%s" + ")"
                    await cur.execute(query, tuple(row))

                join_query = """
                         update monetization_earning_day dm, temp_daily_monetization tdm
                            set 
                            dm.claimer_id = tdm.claimer_id,
                            dm.label_id = tdm.label_id,
                            dm.earning_duration_creator = tdm.earning_duration_creator,
                            dm.earning_duration_claimer = tdm.earning_duration_claimer,
                            dm.earning_duration_label = tdm.earning_duration_label,
                            dm.earning_duration_rplus = tdm.earning_duration_rplus
                            where dm.content_id = tdm.content_id and dm.creator_id = tdm.creator_id and dm.date = tdm.date and dm.total_duration = tdm.total_duration
                """

                await cur.execute(join_query)
                
                await cur.execute(drop_table_query)

            except Exception as e:
                await self.connection.rollback()
                raise e
            else:
                await self.connection.commit()

    async def update_data_month(self, df: pd.DataFrame) -> None:
        async with self.connection.cursor() as cur:
            try:
                drop_table_query = "drop table if exists temp_monthly_monetization"

                await cur.execute(drop_table_query)

                create_table_query = """

                CREATE TABLE IF NOT EXISTS `temp_monthly_monetization` (
                `id` bigint NOT NULL AUTO_INCREMENT,
                `content_id` int NOT NULL,
                `total_duration` decimal(20,2) NOT NULL,
                `creator_id` int NOT NULL,
                `claimer_id` int DEFAULT NULL,
                `label_id` int DEFAULT NULL,
                `earning_duration_creator` decimal(20,2) DEFAULT NULL,
                `earning_duration_claimer` decimal(20,2) DEFAULT NULL,
                `earning_duration_label` decimal(20,2) DEFAULT NULL,
                `earning_duration_rplus` decimal(20,2) DEFAULT NULL,
                `month_year` date NOT NULL,
                PRIMARY KEY (`id`)
                )"""

                await cur.execute(create_table_query)

                cols = " , ".join([c for c in df.columns.tolist()])
                for _, row in df.iterrows():
                    query = "insert into temp_monthly_monetization ("+ cols +") VALUES ("+ "%s," * (len(row) - 1) + "%s" + ")"
                    await cur.execute(query, tuple(row))

                join_query = """
                         update monetization_earning_month dm, temp_monthly_monetization tdm
                            set 
                            dm.claimer_id = tdm.claimer_id,
                            dm.label_id = tdm.label_id,
                            dm.earning_duration_creator = tdm.earning_duration_creator,
                            dm.earning_duration_claimer = tdm.earning_duration_claimer,
                            dm.earning_duration_label = tdm.earning_duration_label,
                            dm.earning_duration_rplus = tdm.earning_duration_rplus
                            where dm.content_id = tdm.content_id and dm.creator_id = tdm.creator_id and dm.month_year = tdm.month_year and dm.total_duration = tdm.total_duration
                """
                
                await cur.execute(join_query)
                
                await cur.execute(drop_table_query)

            except Exception as e:
                await self.connection.rollback()
                raise e
            else:
                await self.connection.commit()
        
    async def close(self) -> None :
        """Mysql: close database connection
        """
        self.connection.close()


def MySQL(credentials: dict) -> DatabaseInterface:
    """Generate new Mysql class

    Returns:
        DatabaseInterface: Mysql class that implement Database interface
    """
    return _MySQL(credentials) 
