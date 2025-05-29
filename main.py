import sys
import time
import src.usecase as usecase
from src.adapter.mongo import NewMongo
from src.library.logger import info 

REGISTERED_USECASES = [
    ('--football_analysis', usecase.football_analysis.NewGSheetAutoInputData),
    ('--short_series_analytics', usecase.short_series_analytics.NewGsheetAutoInputData),
    ('--like_views_content', usecase.like_views_content.NewGsheetAutoInputData),
]

def main() -> None:
    """App main runner"""
    args = sys.argv
    if len(args) < 2:
        print("Usage: main.py --football_analysis [mode] [date]")
        sys.exit(1)
        
    use_case = args[1]  # e.g., "--football_analysis"
    use_case_option = args[2] if len(args) > 2 else None  # e.g., "weekly" or "custom_range"
    date = args[3] if len(args) > 3 else None  # e.g., "2025-03-23"
    
    for ru in REGISTERED_USECASES:
        __init_use_case(use_case, ru[0], module=ru[1], use_case_option=use_case_option, date=date)

def __init_use_case(use_case: str, registered_use_case: str, **kwargs) -> None:
    """
    Initialize use case.

    Args:
      use_case (str): Use case flag from argument.
      registered_use_case (str): Use case flag registered in the code.
      kwargs: Additional parameters (e.g., use_case_option, date).
    """
    if use_case == registered_use_case:
        mongo1 = NewMongo()
        mongo2 = NewMongo(index="2")

        use_case_option = kwargs.get('use_case_option', None)
        date = kwargs.get('date', None)
        use_case_module = kwargs.get('module')
        # Pass the mode (opt_type) and date to the use case instance.
        use_case_instance = use_case_module(
            mongo=mongo1,
            mongo2=mongo2,
            use_case_option=use_case_option,
            date=date,
            opt_type=use_case_option
        )
        use_case_instance.run()

if __name__ == '__main__':
    start_time = time.time()
    main()
    info('Process time', f'{time.time() - start_time} second(s)')
