import os


def create_dir_if_not_exists(dir_path: str) -> None:
    """Create directory if not exists (worked on nested directories)

    Args:
        dir_path (str): Directory path
    """
    os.makedirs(dir_path, exist_ok=True)


def get_folder_items(dir_path: str) -> list:
    """Get all items inside a folder

    Args:
        dir_path (str): Folder path

    Returns:
        list: List of items
    """
    try:
        return os.listdir(dir_path)
    except:
        return []