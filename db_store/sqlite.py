import shutil
from pathlib import Path
from typing import Dict
from db_store.dbms import DBMSHandler
from dep_manage.init import load_requirements
from configs.init import logger
from dep_manage.init import DEPENDENCY_GROUPS

class SQLiteHandler(DBMSHandler):
    required_deps = DEPENDENCY_GROUPS["database"]["sqlite"]

    def backup(self, target: Dict) -> Path:
        self.ensure_deps(load_requirements())
        db_config = target["database"]
        backup_file = self.get_backup_filename(target, "db")
        import sqlite3
        with sqlite3.connect(db_config["path"]) as src, sqlite3.connect(backup_file) as dst:
            src.backup(dst)
        logger.info(f"SQLite backup created: {backup_file}")
        return backup_file

    def restore(self, target: Dict, backup_file: Path) -> None:
        self.ensure_deps(load_requirements())
        db_config = target["database"]
        target_path = Path(db_config["path"])
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(backup_file, target_path)
        logger.info(f"SQLite database restored to {target_path}")
