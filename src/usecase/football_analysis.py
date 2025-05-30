import pandas as pd
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
from src.library.gsheet import (
    get_gsheet_cdt_sheet_key,
    get_gsheet_cdt_input_data_worksheet,
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
class _GSheetAutoInputData:
    def __init__(
        self,
        mongo: DatabaseInterface,
        use_case_option: str = None,
        date: str = None,
        opt_start_date: str = None,
        opt_end_date: str = None,
        opt_type: str = None,
    ):
        self.mongo = mongo
        self.use_case_option = use_case_option
        # Use given date or default to current date in "%Y-%m-%d" format.
        self.date = date if date is not None else datetime.now().strftime("%Y-%m-%d")
        self.opt_start_date = opt_start_date
        self.opt_end_date = opt_end_date
        self.opt_type = opt_type if opt_type is not None else "custom_range"

        self.mongo_conviva_api_collection_name = get_mongo_collection_name()

        # Initialize GSheet related properties
        self.gsheet_api = GSheetAPI()
        self.dt_sheet_key = get_gsheet_cdt_sheet_key()
        self.dt_input_data_worksheet = self.gsheet_api.get_worksheet_by_key(
            self.dt_sheet_key, get_gsheet_cdt_input_data_worksheet()
        )
        self.dt_input_data_date_column_letter = get_gsheet_cdt_input_data_date_column_letter()
        self.dt_input_data_day_column_letter = get_gsheet_cdt_input_data_day_column_letter()
        self.dt_ouput_column_letter = get_gsheet_cdt_output_column_letter()
        self.dfl_sheet_key = get_gsheet_cdfl_sheet_key()
        self.dt_filter_asset = self.gsheet_api.get_worksheet_by_key(
            self.dfl_sheet_key, get_gsheet_cdt_filtered_data_worksheet()
        )
        self.dt_summary_worksheet = self.gsheet_api.get_worksheet_by_key(
            self.dfl_sheet_key, get_gsheet_cdt_summary_report_worksheet()
        )

    def run(self) -> None:
        df = self.__get_all_times_range()

        final_grouped = {}
        summary_data = {}

        for time_range_info in df:
            self.accumulated_data = pd.DataFrame()
            # Convert 'datetime1' and 'datetime2' to date objects (if necessary)
            start_date =pd.to_datetime(time_range_info["start"]).date()
            end_date = pd.to_datetime(time_range_info["end"]).date()
            info(f"Start Date: {start_date}", f"End Date: {end_date}")

            gte = time_range_info["start_time"]
            lte = time_range_info["end_time"]

            data_type = time_range_info["type"].lower()

            current_date = start_date
            while current_date <= end_date:
                # Convert the current date to a string in the format "YYYY-MM-DD"
                date_str = current_date.strftime("%Y-%m-%d")
                info(f"Processing data on current Date:", f"{date_str}")
                daily_df = self.__get_all_data_from_mongo_days(date=current_date, start_date=start_date, 
                                                end_date=end_date ,convertStartTime=gte, 
                                                ConvertEndTime=lte)
                self.accumulated_data = self.__get_accumulated_data(self.accumulated_data, daily_df)
                # Move to the next day
                current_date += timedelta(days=1)
                time.sleep(1)  # Sleep for 1 second to avoid hitting the API too quickly

            info('gsheet auto input data', f'Total documents accumulated: {len(self.accumulated_data)}')

            if not self.accumulated_data.empty:
                self.__write_data_to_gsheet(len(self.accumulated_data))

                if data_type not in final_grouped:
                    final_grouped[data_type] = self.accumulated_data
                else:
                    final_grouped[data_type] = pd.concat(
                        [final_grouped[data_type], self.accumulated_data], 
                        ignore_index=True
                    )
            else:
                info("gsheet auto input data", "No data to write to GSheet.")

        for data_type, df_concat in final_grouped.items():
            df_concat = df_concat.drop_duplicates(subset=['ConvivaSessionId'])
            count = len(df_concat)
            # self.__write_data_to_gsheet(len(df_concat))
            info("gsheet auto input data", f"Total documents for {data_type}: {count}")
            summary_data[data_type] = count
        
        if "match" in final_grouped and "unmatch" in final_grouped:
            match_df = final_grouped["match"]
            unmatch_df = final_grouped["unmatch"]

            # Extract unique DeviceIds
            match_devices = set(match_df["ConvivaSessionId"].unique())
            unmatch_devices = set(unmatch_df["ConvivaSessionId"].unique())

            # Devices in match but not in unmatch
            unique_to_match = match_devices - unmatch_devices
            unique_count = len(unique_to_match)
            summary_data["unique_devices_match_only"] = unique_count
            info("device check", f"Unique devices in MATCH not in UNMATCH: {len(unique_to_match)}")

        if summary_data:
            self.__write_summary_data_to_gsheet(summary_data)
            info("gsheet auto input data", f"Summary data written: {summary_data}")
        else:
            info("gsheet auto input data", "No summary data to write to GSheet.")
     
    def __get_all_times_range(self):
        df = self.__get_time_filter_gsheet()
        types = self.__get_all_data_from_gsheet()
        time_ranges = list(zip(df['datetime1'], df['datetime2'], types['Types'], df['convert_start'], df['convert_end']))

        result = []
        for start, end, type, converst_start_time, convert_end_time in time_ranges:
            print(f"Start: {start}, End: {end}, Type: {type}")
            result.append({
                "start": start,
                "end": end,
                "type": type,
                "start_time": converst_start_time,
                "end_time": convert_end_time
            })
        
        return result
    
    def __get_all_data_from_gsheet(self) -> pd.DataFrame:
        """
        Retrieves all data from the GSheet worksheet.
        """
        try:
            all_values = self.gsheet_api.get_all_values_from_worksheet(self.dt_input_data_worksheet)
            debug('gsheet auto input data', f'All data retrieved from worksheet: {all_values}')
            return pd.DataFrame(all_values[1:], columns=all_values[0])
        except Exception as e:
            debug('gsheet auto input data', f'Error retrieving all data from gsheet: {e}')
            return pd.DataFrame()
    
    def __get_all_filtered_data_from_gsheet(self) -> list:
        """
        Retrieves all filtered data from the GSheet worksheet.
        """
        try:
            all_values = self.gsheet_api.get_all_values_from_worksheet(self.dt_filter_asset)
            debug('gsheet auto input data', f'All filtered data retrieved from worksheet: {all_values}')
            return pd.DataFrame(all_values[1:], columns=all_values[0])
        except Exception as e:
            debug('gsheet auto input data', f'Error retrieving all filtered data from gsheet: {e}')
            return pd.DataFrame()
        
    def __get_time_filter_gsheet(self) -> pd.DataFrame:
        """
        Convert the date string to a datetime object and filter the DataFrame based on the date range.
        Skips rows with missing Date1, Date2, StartTime, or EndTime.
        """
        df = self.__get_all_data_from_gsheet()
        # Clean and filter out rows with missing or blank values
        required_cols = ["Date1", "StartHour", "Date2", "EndHour"]
        for col in required_cols:
            df[col] = df[col].astype(str).str.strip()

        # Keep only rows where all required fields are non-empty
        df = df[
            (df['Date1'] != '') &
            (df['Date2'] != '') &
            (df['StartHour'] != '') &
            (df['EndHour'] != '') 
        ].reset_index(drop=True)

        debug("change epoch time from gsheet", "+")

        dft = pd.DataFrame()
        dft['datetime1'] = df['Date1']
        dft['datetime2'] = df['Date2']
        dft['convert_start'] = self.__change_to_epoch(df, 'Date1', 'StartHour')
        dft['convert_end'] = self.__change_to_epoch(df, 'Date2', 'EndHour')
        return dft

    def __change_to_epoch(self, df: pd.DataFrame, date_col: str, hour_col: str) -> pd.Series:
        """
        Converts the date, hour, and second columns from GSheet into epoch time.
        """
        # Combine the date, hour, and second columns to form a complete datetime string
        datetime_str = df[date_col].astype(str).str.strip() + ' ' + df[hour_col].astype(str).str.strip()

        # Convert the combined string into a datetime object
        dfs = pd.to_datetime(
            datetime_str,
            format='%Y-%m-%d %H:%M:%S',  # Format to include hour, minute, and second
            errors='coerce'  # Convert invalid datetime to NaT (Not a Time)
        ).dt.tz_localize(pytz.timezone("Asia/Jakarta")).dt.tz_convert(pytz.utc)

        # Drop invalid datetimes (NaT) and convert to epoch time (seconds)
        return (dfs.astype('int64') // int(1e9)).astype('Int64')
    
    def __get_accumulated_data(self, accumulated_df: pd.DataFrame, df_day: pd.DataFrame) -> pd.DataFrame:
            """
            Combines new daily data with the accumulated DataFrame and removes duplicate entries based on ConvivaSessionId.
            """
            if not df_day.empty:
                combined = pd.concat([accumulated_df, df_day], ignore_index=True)
                return combined.drop_duplicates(subset=['ConvivaSessionId'])
            return accumulated_df
    
    # Data gathering from mongodb to be processed in GSheet
    def __get_all_data_from_mongo_days(self, date: date, convertStartTime: int, ConvertEndTime: int, 
                                  start_date: date,  end_date: date ,chunk_size: int = 100000) -> pd.DataFrame:
        
        # Convert date object to string
        date = date.strftime('%Y-%m-%d')
        
        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')

        asset_list = self.__get_all_filtered_data_from_gsheet().values.tolist()
        assets = [item[0] for item in asset_list]

        db = self.mongo
        db.connect()
        all_chunks = []

        try:
            col = db.db[self.mongo_conviva_api_collection_name]
            if date == start_date == end_date:
                query = {
                    'DataDate': date,
                    'Asset': {'$in': assets},
                    '$or': [
                            {'StartupTime': {'$gte': 0}},
                            {'StartupTime': {'$eq': -3}}                        
                    ],
                    'PlayingTime': {'$gt': 0},
                    'ErrorList': {"$in": [""]},
                    'StartTime': {
                        '$gte': int(convertStartTime),
                        '$lte': int(ConvertEndTime)
                    }
                }
            elif date == start_date != end_date:
                query = {
                    'DataDate': date,
                    'Asset': {'$in': assets},
                    '$or': [
                            {'StartupTime': {'$gte': 0}},
                            {'StartupTime': {'$eq': -3}}                        
                    ],
                    'PlayingTime': {'$gt': 0},
                    'ErrorList': {"$in": [""]},
                    'StartTime': {
                        '$gte': int(convertStartTime)
                    }
                }

            elif date == end_date != start_date:
                query = {
                    'DataDate': date,
                    'Asset': {'$in': assets},
                    '$or': [
                            {'StartupTime': {'$gte': 0}},
                            {'StartupTime': {'$eq': -3}}
                        ],
                    'PlayingTime': {'$gt': 0},
                    'ErrorList': {"$in": [""]},
                    'StartTime': {
                        '$lte': int(ConvertEndTime)
                    }
                }
            
            elif date < start_date or date > end_date:
                query = {
                    'DataDate': date,
                    '_id': {'$exists': False}
                }
                info(f'Data is out of expected range', f'{date}')

            else:
                query = {
                    'DataDate': date,
                    'Asset': {'$in': assets},
                    '$and': [
                        {'$or': [
                            {'StartupTime': {'$gte': 0}},
                            {'StartupTime': {'$eq': -3}}
                        ]}
                    ],
                    'PlayingTime': {'$gt': 0},
                    'ErrorList': {"$in": [""]}
                }
            
            projection = {
                '_id': 0,
                'ConvivaSessionId': 1
            }

            cursor = col.find(query, projection).batch_size(chunk_size)

            while True:
                chunk = list(itertools.islice(cursor, chunk_size))
                if not chunk:
                    break
                all_chunks.extend(chunk)
                debug('gsheet auto input data', f'Chunk size: {len(chunk)}')

        except Exception as e:
            debug('gsheet auto input data', f'Error connecting to MongoDB: {e}')
            return pd.DataFrame()
        
        finally:
            db.close()
        
        debug(f'gsheet auto input data | {self.date}', f'Total documents: {len(all_chunks)}')
        df_day = pd.DataFrame(all_chunks)
        df_day = self.__get_UD_processing(df_day)
        return df_day
    

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
    
    def __write_data_to_gsheet(self, input: int) -> None:
        """
        Appends the input integer to the next empty row in the target GSheet column.
        """
        try:
            worksheet = self.dt_input_data_worksheet
            col_letter = get_gsheet_cdt_output_column_letter()
            col_number = col_letter_to_no(col_letter)

            # Get all current values in the column to determine next empty row
            current_values = worksheet.col_values(col_number)
            next_row = len(current_values) + 1  # +1 to write below last filled cell

            worksheet.update_cell(next_row, col_number, input)

            info("gsheet auto input data", f"Appended value {input} to column {col_letter}, row {next_row}.")

        except Exception as e:
            debug("gsheet auto input data", f"Error writing data to GSheet: {e}")
    
    def __write_summary_data_to_gsheet(self, summary_data: dict ) -> None:
        """
        Appends the input integer to the next empty row in the target GSheet column.
        """
        try:
            worksheet = self.dt_summary_worksheet
            headers = ["Type", "Total Documents"]

            worksheet.clear()

            values = [headers]
            for key, val in summary_data.items():
                values.append([key, val])

            # Write data starting at cell A1
            worksheet.update("A1", values)

            info("gsheet auto input data", f"Summary data written: {summary_data}")

        except Exception as e:
            debug("gsheet auto input data", f"Error writing data to GSheet: {e}")
        

def NewGSheetAutoInputData(**kwargs) -> UseCaseInterface:
    return _GSheetAutoInputData(
        mongo=kwargs.get('mongo'),
        use_case_option=kwargs.get('use_case_option'),
        date=kwargs.get('date'),
        opt_start_date=kwargs.get('opt_start_date'),
        opt_end_date=kwargs.get('opt_end_date'),
        opt_type=kwargs.get('opt_type')
    )
