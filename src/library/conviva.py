import os
import re
from urllib.parse import parse_qs
from datetime import datetime
from src.library.config import config
from src.library.dictionary import get_value_from_dict
from src.library.string import str_to_snake, snake_to_camel, remove_parentheses

CONFIG_APP = get_value_from_dict(config, 'app', {})
CONFIG_CONVIVA = get_value_from_dict(config, 'conviva', {})
CONFIG_CONVIVA_API = get_value_from_dict(config, 'conviva_api', {})


def generate_single_data(columns: list, data: list,
                         file_name: str = '', row: int = 0) -> dict:
    """
    Generate & extract single data (dictionary) from single data list (csv).

    :param columns: list: csv columns.
    :param data: list: single data list from csv.
    :param file_name: str: csv file name.
    :param row: int: row number.
    :return: dict: generated dictionary data.
    """
    generated_data = {}
    for i in range(len(columns)):
        if columns[i].lower() in ['sessiontags', 'session_tags', 'session tags']:
            generated_data[columns[i]] = data[i] or ""
            if generated_data[columns[i]]:
                parsed_session_tags = parse_session_tags(data[i])
                for pst in parsed_session_tags.keys():
                    generated_data[pst] = parsed_session_tags[pst] or ""
        else:
            generated_data[columns[i]] = data[i] or ""
    generated_data['DataDate'] = get_date_from_filename(file_name)
    generated_data['FromFilename'] = file_name
    generated_data['DataRowNumber'] = row
    generated_data['AddedAt'] = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S")
    return generated_data


def parse_session_tags(session_tags: str) -> dict:
    """
    Parse session tags.

    :param session_tags: str: session tags string.
    :return: dict: parsed session tags.
    """
    parsed_tags = parse_qs(session_tags)
    norm_parsed_tags = {}
    for key in parsed_tags.keys():
        norm_key = csv_columns_normalization([key])[0]
        if len(parsed_tags[key]) == 1:
            norm_parsed_tags[norm_key] = parsed_tags[key][0]
        else:
            norm_parsed_tags[norm_key] = parsed_tags[key]
    return norm_parsed_tags


def csv_columns_normalization(columns: list) -> list:
    """
    Normalize original csv columns into well formatted name.

    :param columns: list: original columns.
    :return: list: normalized columns.
    """
    normalized_columns = []
    for column in columns:
        normalized_columns.append(snake_to_camel(
            str_to_snake(remove_parentheses(column))))
    return normalized_columns


def get_zip_extension() -> str:
    return get_value_from_dict(
        CONFIG_CONVIVA,
        'ZIP_EXTENSION',
        '.gz'
    )


def get_unzip_done_extension() -> str:
    return get_value_from_dict(
        CONFIG_CONVIVA,
        'UNZIP_DONE_EXTENSION',
        '.unzip.done.csv'
    )


def get_csv_extension() -> str:
    return get_value_from_dict(
        CONFIG_CONVIVA,
        'CSV_EXTENSION',
        '.csv'
    )


def get_box_folder_id() -> int:
    return int(get_value_from_dict(
        CONFIG_CONVIVA,
        'BOX_FOLDER_ID',
        122877003340
    ))


def get_read_csv_chunksize() -> int:
    return int(get_value_from_dict(
        CONFIG_CONVIVA,
        'READ_CSV_CHUNKSIZE',
        1000
    ))


def get_mongo_collection_name() -> str:
    try:
        return CONFIG_CONVIVA['MONGO_COLLECTION_NAME']
    except:
        return 'prod-conviva-data'


def get_mongo_conviva_api_summary_collection_name() -> str:
    try:
        return CONFIG_APP['APP_ENV'] + '-' + CONFIG_CONVIVA['MONGO_CONVIVA_API_SUMMARY_COLLECTION_NAME']
    except:
        return 'conviva_summary'


def get_mongo_conviva_api_trx_collection_name() -> str:
    try:
        return CONFIG_APP['APP_ENV'] + '-' + CONFIG_CONVIVA['MONGO_CONVIVA_API_TRX_COLLECTION_NAME']
    except:
        return 'conviva_trx'

def get_mongo_conviva_api_summary_weekly_collection_name() -> str:
    try:
        return CONFIG_APP['APP_ENV'] + '-' + CONFIG_CONVIVA['MONGO_CONVIVA_API_SUMMARY_WEEKLY_COLLECTION_NAME']
    except:
        return 'conviva_summary_weekly'

def get_mongo_conviva_api_trx_weekly_collection_name() -> str:
    try:
        return CONFIG_APP['APP_ENV'] + '-' + CONFIG_CONVIVA['MONGO_CONVIVA_API_TRX_WEEKLY_COLLECTION_NAME']
    except:
        return 'conviva_trx_weekly'


def get_mongo_conviva_api_summary_monthly_collection_name() -> str:
    try:
        return CONFIG_APP['APP_ENV'] + '-' + CONFIG_CONVIVA['MONGO_CONVIVA_API_SUMMARY_MONTHLY_COLLECTION_NAME']
    except:
        return 'conviva_summary_monthly'


def get_mongo_conviva_api_trx_monthly_collection_name() -> str:
    try:
        return CONFIG_APP['APP_ENV'] + '-' + CONFIG_CONVIVA['MONGO_CONVIVA_API_TRX_MONTHLY_COLLECTION_NAME']
    except:
        return 'conviva_trx_monthly'


def get_download_limit() -> int:
    try:
        return int(get_value_from_dict(
            CONFIG_CONVIVA,
            'DOWNLOAD_LIMIT',
            None
        ))
    except:
        return None


def get_date_from_filename(filename: str) -> str:
    try:
        p = re.compile("DailySessionLog_RCTI_(.*).csv.gz")
        r = p.search(filename)
        return r.group(1)
    except:
        return ''


def get_temp_downloaded_dir() -> str:
    return os.path.abspath(os.path.expanduser(get_value_from_dict(
        CONFIG_CONVIVA,
        'TEMP_DOWNLOADED_DIR',
        'temp/conviva/downloaded'
    )))


def get_csv_filepath(filename: str, specified_date: str = '') -> str:
    temp_downloaded_dir = get_temp_downloaded_dir()
    if specified_date == '':
        return os.path.abspath(os.path.expanduser(
            '{temp_downloaded_dir}/{filename}'.format(
                temp_downloaded_dir=temp_downloaded_dir, filename=filename)))
    else:
        return os.path.abspath(os.path.expanduser(
            '{temp_downloaded_dir}/{specified_date}/{filename}'.format(
                temp_downloaded_dir=temp_downloaded_dir, specified_date=specified_date,
                filename=filename)))


def get_last_offset_file() -> str:
    return os.path.abspath(os.path.expanduser(get_value_from_dict(
        CONFIG_CONVIVA,
        'LAST_OFFSET_FILE',
        'temp/conviva/history/last_offset_file.txt'
    )))


def get_last_stored_file() -> str:
    return os.path.abspath(os.path.expanduser(get_value_from_dict(
        CONFIG_CONVIVA,
        'LAST_STORED_FILE',
        'temp/conviva/history/last_stored_file.txt'
    )))


def get_temp_history_dir() -> str:
    return os.path.abspath(os.path.expanduser(get_value_from_dict(
        CONFIG_CONVIVA,
        'TEMP_HISTORY_DIR',
        'temp/conviva/history'
    )))


def get_conviva_api_host() -> str:
    return get_value_from_dict(
        CONFIG_CONVIVA_API,
        'HOST',
        'https://api.conviva.com'
    )


def get_conviva_api_metrics_endpoint() -> str:
    return get_value_from_dict(
        CONFIG_CONVIVA_API,
        'METRICS_ENDPOINT',
        '/insights/2.5/metrics.json'
    )


def get_conviva_api_filters_endpoint() -> str:
    return get_value_from_dict(
        CONFIG_CONVIVA_API,
        'FILTERS_ENDPOINT',
        '/insights/2.5/filters.json'
    )


def get_conviva_api_metrics_param() -> str:
    return get_value_from_dict(
        CONFIG_CONVIVA_API,
        'METRICS_PARAM',
        'plays,unique_devices,total_minutes,minutes/uniques'
    )


def get_conviva_api_client_id() -> str:
    return get_value_from_dict(
        CONFIG_CONVIVA_API,
        'CLIENT_ID',
        ''
    )


def get_conviva_api_client_secret() -> str:
    return get_value_from_dict(
        CONFIG_CONVIVA_API,
        'CLIENT_SECRET',
        ''
    )


def get_conviva_api_batch() -> int:
    return int(get_value_from_dict(
        CONFIG_CONVIVA_API,
        'BATCH',
        5
    ))
