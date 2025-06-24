from db_store.postgresql import PostgreSQLHandler
from db_store.mysql import MySQLHandler
from db_store.mongodb import MongoDBHandler
from db_store.sqlite import SQLiteHandler
from db_store.storage_handler import StorageHandler, LocalStorageHandler, S3StorageHandler
from db_store.dbms import DBMSHandler


def get_dbms_handler(db_type: str) -> DBMSHandler:
    handlers = {"postgresql": PostgreSQLHandler, "mysql": MySQLHandler, "mongodb": MongoDBHandler, "sqlite": SQLiteHandler}
    handler = handlers.get(db_type.lower())
    if not handler:
        raise ValueError(f"Unsupported DBMS: {db_type}")
    return handler()

def get_storage_handler(storage_type: str) -> StorageHandler:
    handlers = {"none": LocalStorageHandler, "local": LocalStorageHandler, "s3": S3StorageHandler}
    handler = handlers.get(storage_type.lower())
    if not handler:
        raise ValueError(f"Unsupported storage type: {storage_type}")
    return handler()
