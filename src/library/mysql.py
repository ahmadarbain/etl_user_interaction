from src.library.config import config
from src.library.dictionary import get_value_from_dict

def load_mysql_config() -> dict:
    CONFIG_MYSQL = get_value_from_dict(config, 'mysql', {})

    mysql_config = {
        'host': get_value_from_dict(CONFIG_MYSQL, 'host', 'localhost'),
        'user': get_value_from_dict(CONFIG_MYSQL, 'username', ''),
        'password': get_value_from_dict(CONFIG_MYSQL, 'password', ''),
        'db': get_value_from_dict(CONFIG_MYSQL, 'database', ''),
        'port': int(get_value_from_dict(CONFIG_MYSQL, 'port', 3306)),
    }
    return mysql_config
