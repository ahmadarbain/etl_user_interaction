import pandas as pd
from zope.interface import implementer
from src.interface.usecase import UseCaseInterface
from src.interface.database import DatabaseInterface
from src.library.logger import info
from src.library.conviva import get_mongo_collection_name
from src.adapter.gsheet_api import GSheetAPI
from src.library.gsheet import (
    get_gsheet_css_sheet_key,
    get_gsheet_css_input_data_worksheet,
)

@implementer(UseCaseInterface)
class _GsheetAutoInputData():
    """
    Use case for GSheet Auto Input Data.
    """

    def __init__(
        self, 
        mongo: DatabaseInterface,
        mongo2: DatabaseInterface = None,
        use_case_option: str = None, 
        date: str = None,
        opt_start_date: str = None,
        opt_end_date: str = None,  
        opt_type: str = None,
        credentials: dict = None,
    ):
        self.mongo = mongo
        self.mongo2 = mongo2
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
        self.___get_all_filtered_data_extarct()
    
    def ___get_all_filtered_data_gsheet(self) -> pd.DataFrame:
        """Retrieve all filtered data from the GSheet."""
        try:
            all_values = self.gsheet_api.get_all_values_from_worksheet(self.gsheet_input_data_worksheet)
            return pd.DataFrame(all_values[1:], columns=all_values[0])
        except Exception as e:
            info('GsheetAutoInputData', f'Error retrieving all filtered data: {e}')
            return pd.DataFrame()

    def ___get_all_filtered_data_extarct(self) -> pd.DataFrame:
        filter = self.___get_all_filtered_data_gsheet()
        chunk_size = 1000

        id_list = filter['id'].tolist()

        # Connect to both databases
        db = self.mongo
        info('GsheetAutoInputData', 'Connecting to primary MongoDB...')
        db.connect()
        db2 = self.mongo2
        info('GsheetAutoInputData', 'Connecting to secondary MongoDB...')
        db2.connect()

        # Use correct collection names
        behaviour_col = db.db['behaviour']
        likes_col = db2.db['likes']

        info('GsheetAutoInputData', f"Using collections: {behaviour_col.name} and {likes_col.name}")
        all_results = []

        for i in range(0, len(id_list), chunk_size):
            chunk_ids = [int(cid) for cid in id_list[i:i + chunk_size]]

            # Pipeline for all actions (views) from 'behaviour'
            pipeline_views = [
                { "$unwind": { "path": "$action" } },
                { "$match": { "action.contentid": { "$in": chunk_ids } } },
                { "$group": { "_id": "$action.contentid", "TotalViews": { "$sum": 1 } } }
            ]
            results_views = list(behaviour_col.aggregate(pipeline_views))

            pipeline_likes = [
                { "$match": { 
                    "contentid": { "$in": chunk_ids },
                    "action": "like"
                }},
                { "$group": { "_id": "$contentid", "TotalLikes": { "$sum": 1 } } }
            ]
            results_likes = list(likes_col.aggregate(pipeline_likes))


            # Convert to DataFrames and merge on contentid
            df_views = pd.DataFrame(results_views).rename(columns={"_id": "contentid"})
            df_likes = pd.DataFrame(results_likes).rename(columns={"_id": "contentid"})

            # Samakan tipe kolom contentid (misal ke str)
            df_views['contentid'] = df_views['contentid'].astype(str)
            df_likes['contentid'] = df_likes['contentid'].astype(str)

            df_merged = pd.merge(df_views, df_likes, on="contentid", how="outer").fillna(0)
            all_results.append(df_merged)
            info('GsheetAutoInputData', f"Fetched {len(df_views)} views and {len(df_likes)} likes for chunk {i//chunk_size+1}")

        if all_results:
            df = pd.concat(all_results, ignore_index=True)
            # Only call __get_UD_processing if needed and if columns exist
            if hasattr(self, '__get_UD_processing'):
                df = self.__get_UD_processing(df)
            info('GsheetAutoInputData', f"✅ Processed Data: {df}")
            # Save to CSV
            df.to_csv('output.csv', index=False)
            return df
        else:
            return pd.DataFrame()

def NewGsheetAutoInputData(**kwargs) -> UseCaseInterface:
    return _GsheetAutoInputData(
        mongo=kwargs.get('mongo'),
        mongo2=kwargs.get('mongo2'),
        use_case_option=kwargs.get('use_case_option'),
        date=kwargs.get('date'),
        opt_start_date=kwargs.get('opt_start_date'),
        opt_end_date=kwargs.get('opt_end_date'),
        opt_type=kwargs.get('opt_type'),
    )

