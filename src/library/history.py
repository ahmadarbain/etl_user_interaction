import configparser as cp
from os import path


def set_new_last_offset(last_offset: str, filepath: str) -> None:
    """Set new last offset and save it into a file

    Args:
        last_offset (str): Last offset.
        filepath (str): Last offset history filepath.
    """
    with open(filepath, 'w') as f:
        f.write(last_offset)
        f.close()


def get_last_offset(filepath: str) -> str:
    """Get last offset from file

    Args:
        filepath (str): Last offset history filepath.

    Returns:
        str: Last offset.
    """
    if path.exists(filepath) == False:
        with open(filepath, 'w') as f:
            last_offset = '0'
            f.write(last_offset)
            f.close()
    else:
        with open(filepath, 'r') as f:
            last_index = f.read()
            if last_index == '':
                last_offset = '0'
            else:
                last_offset = last_index
            f.close()
    return last_offset


def append_new_last_stored(last_stored: str, filepath: str) -> None:
    """Append new last stored file name and save it into a file

    Args:
        last_stored (str): Last stored file name.
        filepath (str): Last stored history filepath.
    """
    with open(filepath, 'a') as f:
        f.write(last_stored+'\n')
        f.close()


def get_last_stored(filepath: str) -> str:
    """Get last stored file names from file

    Args:
        filepath (str): Last stored history filepath.

    Returns:
        str:  last stored files.
    """
    if path.exists(filepath) == False:
        with open(filepath, 'w') as f:
            last_stored = ''
            f.write(last_stored)
            f.close()
    else:
        with open(filepath, 'r') as f:
            last_stored = f.read()
            f.close()
    return last_stored