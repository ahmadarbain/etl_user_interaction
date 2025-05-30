from loguru import logger
from src.library.config import config
from src.library.dictionary import get_value_from_dict

CONFIG_APP = get_value_from_dict(config, 'app', {})
LOG_LEVEL = get_value_from_dict(CONFIG_APP, 'LOG_LEVEL', 'debug')


def debug(name: str, debug: str) -> None:
    """Log debug

    Args:
        name (str): Debug action name
        debug (str): Debug description
    """
    if LOG_LEVEL == 'debug':
        logger.debug("{name} | {debug}", name=name, debug=debug)


def info(name: str, info: str) -> None:
    """Log info

    Args:
        name (str): Info action name
        info (str): Info description
    """
    logger.info("{name} | {info}", name=name, info=info)
