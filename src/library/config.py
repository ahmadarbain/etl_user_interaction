import configparser as cp

CONFIG_INI_FILE = 'config.ini'
config = cp.ConfigParser()  # Create an instance
config.read(CONFIG_INI_FILE)
