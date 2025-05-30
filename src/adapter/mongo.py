import urllib.parse
import pymongo
from pymongo import MongoClient
from zope.interface import implementer
from src.library.logger import info, debug
from src.interface.database import DatabaseInterface
from src.library.config import config

@implementer(DatabaseInterface)
class _Mongo():

    def __init__(self, index: str = None, db_name: str = None):
        self.client = None
        self.db = None
        self.index = index
        self.db_name = db_name

    def connect(self) -> None:
        """Mongo: open database connection"""
        prefix_conf = 'mongo'
        if self.index:
            prefix_conf = f'mongo_{self.index}'
        if prefix_conf not in config:
            raise KeyError(f"Config section '{prefix_conf}' not found in config.ini")
        section = config[prefix_conf]

        host1 = section.get('HOST', '127.0.0.1')
        port1 = section.get('PORT', '27017')
        host2 = section.get('HOST_SECONDARY', '')
        port2 = section.get('PORT_SECONDARY', '')

        if host2 and port2:
            hosts = f"{host1}:{port1},{host2}:{port2}"
        else:
            hosts = f"{host1}:{port1}"

        conf = {
            'hosts': hosts,
            'database': self.db_name or section.get('DATABASE', ''),
            'username': urllib.parse.quote(section.get('USERNAME', '')),
            'password': urllib.parse.quote(section.get('PASSWORD', '')),
            'auth': section.get('AUTH', ''),
            'replica_set': section.get('REPLICA_SET', ''),
            'direct_connection': str(section.get('DIRECT_CONNECTION', "false")).lower(),
        }
        debug('mongo', f"Connecting with URI: {self.__generate_query_string(conf)}")
        try:
            query_string = self.__generate_query_string(conf)
            client = MongoClient(query_string)
            client.server_info()
            db = client[conf['database']]
            self.client = client
            self.db = db
        except pymongo.errors.ServerSelectionTimeoutError as err:
            info('mongo', f"Timeout: {err}")
        except Exception as e:
            info('mongo', f"Unexpected error: {e}")

    def close(self) -> None:
        """Mongo: close database connection"""
        if self.client:
            self.client.close()

    def __generate_query_string(self, conf: dict) -> str:
        """Generate mongodb query string connection"""
        # Single node (no replica set)
        if not conf['replica_set']:
            if conf['username']:
                auth_part = f"?authSource={conf['auth']}" if conf['auth'] else ""
                return (
                    "mongodb://{username}:{password}@{hosts}/{database}{auth_part}"
                ).format(auth_part=auth_part, **conf)
            else:
                return "mongodb://{hosts}/{database}".format(**conf)
        # Replica set
        else:
            auth_part = f"&authSource={conf['auth']}" if conf['auth'] else ""
            return (
                "mongodb://{username}:{password}@{hosts}/?directConnection={direct_connection}"
                "{auth_part}&replicaSet={replica_set}"
            ).format(auth_part=auth_part, **conf)


def NewMongo(index: str = None, db_name: str = None) -> DatabaseInterface:
    """Generate new Mongo class"""
    return _Mongo(index=index, db_name=db_name)
