import csv
import os
from pkg.library.config import config
from pkg.library.dictionary import get_value_from_dict

CONFIG_TOP_PROGRAM = get_value_from_dict(config, 'top_program', {})


def set_top_program_csv(data: list, filepath: str) -> None:
    """Set top program csv
    """
    with open(filepath, 'w') as f:
        csv_writer = csv.writer(f)
        count = 0
        for d in data:
            if count == 0:
                header = d.keys()
                csv_writer.writerow(header)
                count += 1
            csv_writer.writerow(d.values())
        f.close()


def get_temp_top_program_dir() -> str:
    """Get conviva temp top program dir.

    Returns:
        str: Conviva temp top program dir.
    """
    try:
        return os.path.abspath(os.path.expanduser(
            CONFIG_TOP_PROGRAM['TEMP_TOP_PROGRAM_DIR']))
    except:
        return os.path.abspath(os.path.expanduser(
            'temp/conviva/top_program'))


def get_per_day_by_freq_file() -> str:
    """Get top program per day by freq file

    Returns:
        str: Top program per day by freq file
    """
    try:
        return os.path.abspath(os.path.expanduser(
            CONFIG_TOP_PROGRAM['PER_DAY_BY_FREQ_FILE']))
    except:
        return os.path.abspath(os.path.expanduser(
            'temp/conviva/top_program/top_program_per_day_by_freq.csv'))


def get_per_day_by_playing_time_file() -> str:
    """Get top program per day by playing time file

    Returns:
        str: Top program per day by playing time file
    """
    try:
        return os.path.abspath(os.path.expanduser(
            CONFIG_TOP_PROGRAM['PER_DAY_BY_PLAYING_TIME_FILE']))
    except:
        return os.path.abspath(os.path.expanduser(
            'temp/conviva/top_program/top_program_per_day_by_playing_time.csv'))


def get_per_day_by_freq_and_playing_time_file() -> str:
    """Get top program per day by freq and playing time file

    Returns:
        str: Top program per day by freq and playing time file
    """
    try:
        return os.path.abspath(os.path.expanduser(
            CONFIG_TOP_PROGRAM['PER_DAY_BY_FREQ_AND_PLAYING_TIME_FILE']))
    except:
        return os.path.abspath(os.path.expanduser(
            'temp/conviva/top_program/top_program_per_day_by_freq_and_playing_time.csv'))
