from src.library.config import config
from src.library.dictionary import get_value_from_dict

CONFIG_GSHEET = get_value_from_dict(config, 'gsheet', {})


def col_no_to_letter(column_no: int):
    n, rem = divmod(column_no - 1, 26)
    char = chr(65 + rem)
    if n:
        return col_no_to_letter(n) + char
    else:
        return char


def col_letter_to_no(column_letter: str):
    n = ord(column_letter[-1]) - 64
    if column_letter[:-1]:
        return 26 * (col_letter_to_no(column_letter[:-1])) + n
    else:
        return n


def get_gsheet_service_account_file() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'SERVICE_ACCOUNT_FILE',
        'service_account.json'
    )


def get_gsheet_cdt_sheet_key() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_DAILY_TRAFFIC_SHEET_KEY',
        ''
    )


def get_gsheet_cdt_input_data_worksheet() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_DAILY_TRAFFIC_INPUT_DATA_WORKSHEET',
        ''
    )

def get_gsheet_cdt_filtered_data_worksheet() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_DAILY_TRAFFIC_FILTERED_DATA_WORKSHEET',
        ''
    )

def get_gsheet_cdt_input_data_date_column_letter() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_DAILY_TRAFFIC_INPUT_DATA_DATE_COLUMN_LETTER',
        ''
    )

def get_gsheet_cdt_input_data_day_column_letter() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_DAILY_TRAFFIC_INPUT_DATA_DAY_COLUMN_LETTER',
        ''
    )

def get_gsheet_cdt_output_column_letter() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_DAILY_TRAFFIC_OUTPUT_DATA',
        ''
    )

def get_gsheet_cdt_summary_report_worksheet() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_DAILY_TRAFFIC_SUMMARY_REPORT_WORKSHEET',
        ''
    )

def get_gsheet_cdt_input_data_date_column_letter() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_DAILY_TRAFFIC_INPUT_DATA_SUMMARY_DATE_COLUMN_LETTER',
        ''
    )

def get_gsheet_cdt_original_series_report_worksheet() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_DAILY_TRAFFIC_ORIGINAL_SERIES_REPORT_WORKSHEET',
        ''
    )


def get_gsheet_cdt_original_series_report_date_column_letter() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_DAILY_TRAFFIC_ORIGINAL_SERIES_REPORT_DATE_COLUMN_LETTER',
        ''
    )


def get_gsheet_cdt_original_series_report_day_column_letter() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_DAILY_TRAFFIC_ORIGINAL_SERIES_REPORT_DAY_COLUMN_LETTER',
        ''
    )


def get_gsheet_cdt_input_data_weekly_worksheet() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_DAILY_TRAFFIC_INPUT_DATA_WEEKLY_WORKSHEET',
        ''
    )

def get_gsheet_cdt_input_data_weekly_column_letter() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_DAILY_TRAFFIC_INPUT_DATA_WEEKLY_COLUMN_LETTER',
        ''
    )

def get_gsheet_cdt_input_data_monthly_worksheet() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_DAILY_TRAFFIC_INPUT_DATA_MONTHLY_WORKSHEET',
        ''
    )
def get_gsheet_cdt_input_data_monthly_column_letter() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_DAILY_TRAFFIC_INPUT_DATA_MONTHLY_COLUMN_LETTER',
        ''
    )

def get_gsheet_cdfl_sheet_key() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_RND_DAILY_FILTER_LIST_SHEET_KEY',
        ''
    )


def get_gsheet_cdfl_conviva_daily_worksheet() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_RND_DAILY_FILTER_LIST_CONVIVA_DAILY_WORKSHEET',
        ''
    )


def get_gsheet_cdfl_conviva_daily_fid_column_letter() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_RND_DAILY_FILTER_LIST_CONVIVA_DAILY_FILTER_ID_COLUMN_LETTER',
        ''
    )


def get_gsheet_cdfl_conviva_daily_nig_column_letter() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_RND_DAILY_FILTER_LIST_CONVIVA_DAILY_NAMES_IN_GSHEET_COLUMN_LETTER',
        ''
    )


def get_gsheet_cdfl_original_series_report_worksheet() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_RND_DAILY_FILTER_LIST_ORIGINAL_SERIES_REPORT_WORKSHEET',
        ''
    )


def get_gsheet_cdfl_original_series_report_fid_column_letter() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_RND_DAILY_FILTER_LIST_ORIGINAL_SERIES_REPORT_FILTER_ID_COLUMN_LETTER',
        ''
    )


def get_gsheet_cdfl_original_series_report_nig_column_letter() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_RND_DAILY_FILTER_LIST_ORIGINAL_SERIES_REPORT_NAMES_IN_GSHEET_COLUMN_LETTER',
        ''
    )

def get_gsheet_css_sheet_key() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_SHORT_SERIES_SHEET_KEY',
        ''
    )

def get_gsheet_css_input_data_worksheet() -> str:
    return get_value_from_dict(
        CONFIG_GSHEET,
        'CONVIVA_SHORT_SERIES_INPUT_DATA_WORKSHEET',
        ''
    )