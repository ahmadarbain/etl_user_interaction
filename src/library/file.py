import os
from pyunpack import Archive


def extract_compressed_file(filepath: str, output_dir: str) -> bool:
    """Extract compressed file

    Args:
        filepath (str): Compressed filepath
        output_dir (str): Extracted output dir

    Returns:
        bool: True if success, False if failed
    """
    try:
        Archive(filepath).extractall(output_dir)
        return True
    except Exception as e:
        print(e)
        return False


def rename_file(oldpath: str, newpath: str) -> None:
    """Rename file to new file name/path

    Args:
        oldpath (str): File oldpath
        newpath (str): File newpath
    """
    os.rename(oldpath, newpath)


def remove_file(filepath: str) -> None:
    """Remove file

    Args:
        filepath (str): Filepath to be removed
    """
    os.remove(filepath)