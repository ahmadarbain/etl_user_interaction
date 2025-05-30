import pandas as pd
import asyncio
import time
import pytz
import itertools
from datetime import date, datetime, timedelta
from zope.interface import implementer
from src.interface.usecase import UseCaseInterface
from src.interface.database import DatabaseInterface
from src.library.logger import debug, info
from src.library.conviva import get_mongo_collection_name
from src.adapter.gsheet_api import GSheetAPI
from src.adapter.mysql import MySQL
from src.library.mysql import (
    load_mysql_config
    )
from src.library.gsheet import (
    get_gsheet_css_sheet_key,
    get_gsheet_css_input_data_worksheet,
    get_gsheet_cdt_input_data_date_column_letter,
    get_gsheet_cdt_input_data_day_column_letter,
    get_gsheet_cdfl_sheet_key,
    get_gsheet_cdt_filtered_data_worksheet,
    get_gsheet_cdt_output_column_letter,
    get_gsheet_cdt_summary_report_worksheet,
    get_gsheet_cdfl_conviva_daily_worksheet,
    get_gsheet_cdfl_conviva_daily_fid_column_letter,
    get_gsheet_cdfl_conviva_daily_nig_column_letter,
    col_no_to_letter,
    col_letter_to_no,
)  

@implementer(UseCaseInterface)
class _GsheetAutoInputData():
    """
    Use case for GSheet Auto Input Data.
    """

    def __init__(
        self, 
        mongo: DatabaseInterface,
        use_case_option: str = None, 
        date: str = None,
        opt_start_date: str = None,
        opt_end_date: str = None,  
        opt_type: str = None,
        credentials: dict = None,
    ):
        self.mongo = mongo
        self.use_case_option = use_case_option
        self.date = date
        self.opt_start_date = opt_start_date
        self.opt_end_date = opt_end_date
        self.opt_type = opt_type
        self.credentials = credentials

        self.mongo_conviva_api_collection_name = get_mongo_collection_name()
        self.gsheet_api = GSheetAPI()
        self.gsheet_key = get_gsheet_css_sheet_key()
        self.gsheet_input_data_worksheet = self.gsheet_api.get_worksheet_by_key(
            self.gsheet_key,
            get_gsheet_css_input_data_worksheet(),
        )


    def run(self):
        self.___get_all_data_in_range()
    
    def ___get_all_data_in_range(self) -> None:
        date_range = self.___get_all_filtered_data_extarct()

        start_date = date_range['start_date']
        end_date = date_range['end_date']

        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

        current_date = end_date_dt - timedelta(days=1)

        while current_date >= start_date_dt:
            # Convert to string format
            date_str = current_date.strftime("%Y-%m-%d")
            info('GsheetAutoInputData', f"Current date: {date_str}")

            # Get the data for the current date
            # Call the function to get all unique device IDs
            self.___get_all_unique_device_ids(date_str)

            # Move to the previous day
            current_date -= timedelta(days=1)
    
    def ___get_all_filtered_data_gsheet(self) -> pd.DataFrame:
        """ Retreive all filtered data from the GSheet and save it to MongoDB """
        try:
            all_values = self.gsheet_api.get_all_values_from_worksheet(self.gsheet_input_data_worksheet)
            # info('gsheet auto input data', f'All data retrieved from worksheet: {all_values}')
            return pd.DataFrame(all_values[1:], columns=all_values[0])
        except Exception as e:
            info('GsheetAutoInputData', f'Error retrieving all filtered data: {e}')
            return pd.DataFrame()
        
    def ___get_all_filtered_data_extarct(self):
        df = self.___get_all_filtered_data_gsheet()
        
        data = {
            'start_date' : df.loc[0, 'start_date'],
            'end_date' : df.loc[0, 'end_date'],
            'program_type' : df.loc[0, 'program_type'],
            'content_type' : df.loc[0, 'content_type'],
            'program_name' : df.loc[0, 'program_name'],
            'asset' : df['asset'].dropna().unique().tolist(),
        }

        return data
    
    def ___write_all_data_to_mysql_sync(self, df: pd.DataFrame) -> None:
        asyncio.run(self.___write_all_data_to_mysql_async(df=df))

    async def ___write_all_data_to_mysql_async(self, df: pd.DataFrame) -> None:
        """ Write all data to MySQL """
        try:
            config = load_mysql_config()
            mysql = MySQL(config)
            await mysql.connect()
            info('GsheetAutoInputData', f"Connected to MySQL: {mysql.host}:{mysql.port}")

            table_name = "short_series"
            
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                `ConvivaSessionId` VARCHAR(255),
                `Asset` TEXT,
                `Country` VARCHAR(100),
                `State` VARCHAR(100),
                `City` VARCHAR(100),
                `ContentType` VARCHAR(100),
                `ProgramName` VARCHAR(255),
                `ProgramType` VARCHAR(100),
                `ContentId` INT,
                `IsPremium` VARCHAR(10),
                `DataDate` DATE            
            );
            """ 
            await mysql.create_table_if_not_exists(table_name, create_sql)
            await mysql.insert_data(df, table_name)

            await mysql.close()
            info('GsheetAutoInputData', "MySQL connection closed")

        except Exception as e:
            info('GsheetAutoInputData', f"Error connecting to MySQL: {e}")
        
            
    def ___get_all_unique_device_ids(self, date: str) -> None:
        filter = self.___get_all_filtered_data_extarct()

        db = self.mongo
        db.connect()

        try:
            col = db.db[self.mongo_conviva_api_collection_name]
            filter_assets = filter.get('asset')

            # Step 1: Match DataDate only
            step1 = { "DataDate": date}
            info('GsheetAutoInputData', f"Step 1 count (DataDate only): {col.count_documents(step1)}")

            # Step 2: Add ProgramType
            step2 = { **step1, "ProgramType": filter['program_type'] }
            info('GsheetAutoInputData', f"Step 2 count (+ ProgramType): {col.count_documents(step2)}")

            # Step 3: Add ContentType
            step3 = { **step2, "ContentType": filter['content_type'] }
            info('GsheetAutoInputData', f"Step 3 count (+ ContentType): {col.count_documents(step3)}")

            # Step 4: Add Partner filter
            step4 = {
                **step3,
                "Partner": {
                    "$exists": True,
                    "$ne": None,
                    "$nin": ["VISION+"]
                },
                '$and': [
                        {'$or': [
                            {'StartupTime': {'$gte': 0}},
                            {'StartupTime': {'$eq': -3}}
                        ]}
                    ],
                    'PlayingTime': {'$gt': 0},
                    'ErrorList': {"$in": [""]}
            }
            info('GsheetAutoInputData', f"Step 4 count (+ Partner filter): {col.count_documents(step4)}")

            # Step 5: Add ProgramName and Asset
            step5 = {
                **step4,
                "ProgramName": filter['program_name'],
                "Asset": {"$in": filter_assets},
            }
            info('GsheetAutoInputData', f"Step 5 count (+ ProgramName & Asset): {col.count_documents(step5)}")

            fields_to_return = {
                "Asset": 1,
                "Country": 1,
                "State": 1,
                "City": 1,
                "ContentType": 1,
                "ProgramName": 1,
                "ProgramType": 1,
                "ContentId": 1,
                "IsPremium": 1,
                "ConvivaSessionId": 1,
                "DataDate": 1,
                "_id": 0  # Optional: exclude Mongo's default _id field
            }

            # If any results exist at the end, use the aggregation pipeline
            pipeline = [
                { "$match": step5},
                { "$project": fields_to_return}
            ]
            cursor = col.aggregate(pipeline)
            results = list(cursor)
            info('GsheetAutoInputData', f"✅ Final Aggregation Results: {len(results)}")

            df = pd.DataFrame(results)
            df = self.__get_UD_processing(df)
            info('GsheetAutoInputData', f"✅ Processed Data: {df}")

            self.___write_all_data_to_mysql_sync(df)

        except Exception as e:
            info('GsheetAutoInputData', f"Error connecting to MongoDB: {e}")
            return
    
    def __get_UD_processing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Processes the UD data by splitting the ConvivaSessionId string.
        For each record, only the first 4 parts (separated by ':') are retained.
        """
        if df.empty:
            return df

        df_ud = df.copy()
        df_ud['ConvivaSessionId'] = df_ud['ConvivaSessionId'].apply(
            lambda x: ':'.join(x.split(':')[:4]) if isinstance(x, str) else x
        )
        return df_ud

def NewGsheetAutoInputData(**kwargs) -> UseCaseInterface:
    return _GsheetAutoInputData(
        mongo=kwargs.get('mongo'),
        use_case_option=kwargs.get('use_case_option'),
        date=kwargs.get('date'),
        opt_start_date=kwargs.get('opt_start_date'),
        opt_end_date=kwargs.get('opt_end_date'),
        opt_type=kwargs.get('opt_type'),
    )

