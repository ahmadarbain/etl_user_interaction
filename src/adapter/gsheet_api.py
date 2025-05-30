import gspread
from src.library.logger import info
from src.library.gsheet import get_gsheet_service_account_file, col_no_to_letter

class GSheetAPI:
    def __init__(self):
        self.service_account_file = get_gsheet_service_account_file()
        self.client = gspread.service_account(filename=self.service_account_file)

    def get_worksheet_by_key(self, sheet_key: str, worksheet_title: str) -> gspread.Worksheet:
        sheet = self.client.open_by_key(sheet_key)
        info('gsheet api', f'connected to worksheet "{worksheet_title}"')
        return sheet.worksheet(worksheet_title)

    def get_values_from_worksheet(self, worksheet: gspread.Worksheet,
                                  row_no: int = None, column_no: int = None) -> (list, list): # type: ignore
        row_values = []
        column_values = []
        if row_no:
            row_values = worksheet.row_values(row_no)
        if column_no:
            column_values = worksheet.col_values(column_no)
        return row_values, column_values

    def get_columns_range_from_worksheet(self, worksheet: gspread.Worksheet) -> (int, str): # type: ignore
        values = worksheet.get_all_values()
        column_range = len(values[0])
        return column_range, col_no_to_letter(column_range)

    def batch_update_to_worksheet(self, worksheet: gspread.Worksheet, data: list,
                                  value_input_option: str = None) -> None:
        worksheet.batch_update(data, value_input_option=value_input_option)

    def get_next_available_row_from_worksheet(self, worksheet: gspread.Worksheet, column_no: int = 1) -> int:
        cols = worksheet.range(1, 1, worksheet.row_count, column_no)
        return max([cell.row for cell in cols if cell.value]) + 1

    def find_column_by_title_from_worksheet(self, worksheet: gspread.Worksheet,
                                            column_title: str) -> (int, int): # type: ignore
        try:
            cell = worksheet.find(column_title)
            return cell.row, cell.col
        except:
            return 0, 0
        
    def get_all_values_from_worksheet(self, worksheet: gspread.Worksheet) -> list:
        """
        Retrieves all values from the given worksheet.
        
        :param worksheet: A gspread.Worksheet object.
        :return: A list of lists containing all cell values.
        """
        try:
            all_values = worksheet.get_all_values()
            info('gsheet api', f'Retrieved all data from worksheet')
            return all_values
        except Exception as e:
            info('gsheet api', f'Error retrieving all data: {e}')
            return []


def NewGSheetAPI() -> GSheetAPI:
    return GSheetAPI()
        