import json
from datetime import datetime
from pathlib import Path
from typing import Dict
from dep_manage.init import load_requirements
from configs.init import logger
from dep_manage.init import DEPENDENCY_GROUPS
from db_store.dbms import DBMSHandler


class MongoDBHandler(DBMSHandler):
    required_deps = DEPENDENCY_GROUPS["database"]["mongodb"]

    def _validate_config(self, db_config: Dict) -> None:
        """Validate database configuration."""
        required_keys = ["host", "port", "name"]
        for key in required_keys:
            if key not in db_config:
                raise ValueError(f"Missing required database config key: {key}")
        if db_config.get("user") and not db_config.get("password"):
            raise ValueError("Password required when username is provided")
        if not isinstance(db_config["port"], int):
            raise ValueError("Port must be an integer")

    def backup(self, target: Dict) -> Path:
        """Create a 1:1 MongoDB backup with metadata."""
        self.ensure_deps(load_requirements())
        import pymongo
        from bson.json_util import dumps
        from pymongo.errors import PyMongoError
        db_config = target.get("database", {})
        self._validate_config(db_config)

        backup_file = self.get_backup_filename(target, "archive")

        try:
            conn_params = {
                'host': db_config["host"],
                'port': db_config["port"],
            }
            if db_config.get("user") and db_config.get("password"):
                conn_params.update({
                    'username': db_config["user"],
                    'password': db_config["password"],
                    'authSource': db_config.get("authSource", "admin")
                })

            client = pymongo.MongoClient(**conn_params)
            try:
                db = client[db_config["name"]]
                backup_data = {
                    'metadata': {
                        'database': db_config["name"],
                        'created_at': datetime.now().isoformat(),
                        'mongo_version': client.server_info().get('version'),
                        'pymongo_version': pymongo.__version__
                    },
                    'collections': {}
                }

                for col_name in db.list_collection_names():
                    collection = db[col_name]
                    backup_data['collections'][col_name] = [
                        json.loads(dumps(doc)) for doc in collection.find({})
                    ]

                with backup_file.open('wb') as f:
                    f.write(json.dumps(backup_data, ensure_ascii=False).encode('utf-8'))

                logger.info(f"MongoDB backup created: {backup_file}")
                return backup_file

            finally:
                client.close()

        except PyMongoError as e:
            logger.error(f"MongoDB backup failed: {e}")
            raise
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Backup file operation failed: {e}")
            raise

    def restore(self, target: Dict, backup_file: Path) -> None:
        """Restore MongoDB database, skipping collections that match backup data."""
        self.ensure_deps(load_requirements())
        import pymongo
        from pymongo.errors import PyMongoError
        from bson.json_util import loads, dumps
        db_config = target.get("database", {})
        self._validate_config(db_config)

        if not backup_file.exists():
            raise FileNotFoundError(f"Backup file not found: {backup_file}")

        try:
            conn_params = {
                'host': db_config["host"],
                'port': db_config["port"],
            }
            if db_config.get("user") and db_config.get("password"):
                conn_params.update({
                    'username': db_config["user"],
                    'password': db_config["password"],
                    'authSource': db_config.get("authSource", "admin")
                })

            client = pymongo.MongoClient(**conn_params)
            try:
                db = client[db_config["name"]]

                with backup_file.open('rb') as f:
                    backup_data = json.loads(f.read().decode('utf-8'))

                if not isinstance(backup_data, dict) or 'collections' not in backup_data:
                    raise ValueError("Invalid backup file format")

                # Validate metadata
                if backup_data.get('metadata', {}).get('database') != db_config["name"]:
                    logger.warning("Backup database name does not match target database")

                existing_collections = set(db.list_collection_names())

                for col_name, backup_docs in backup_data['collections'].items():
                    collection = db[col_name]

                    if col_name in existing_collections:
                        # Convert existing documents to JSON strings using bson.json_util
                        existing_docs = {dumps(doc, sort_keys=True) for doc in collection.find({})}
                        # Convert backup documents back to JSON strings
                        backup_docs_json = {dumps(doc, sort_keys=True) for doc in backup_docs}
                        if existing_docs == backup_docs_json:
                            logger.info(f"Skipping collection {col_name}: identical data")
                            continue
                        else:
                            logger.info(f"Clearing and restoring collection {col_name}")
                            collection.delete_many({})

                    if backup_docs:
                        try:
                            # Restore documents using bson.json_util.loads to handle BSON types
                            collection.insert_many(loads(json.dumps(backup_docs)))
                        except PyMongoError as e:
                            logger.error(f"Failed to restore collection {col_name}: {e}")
                            raise

                logger.info(f"MongoDB database restored: {db_config['name']}")

            finally:
                client.close()

        except PyMongoError as e:
            logger.error(f"MongoDB restore failed: {e}")
            raise
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Restore file operation failed: {e}")
            raise
