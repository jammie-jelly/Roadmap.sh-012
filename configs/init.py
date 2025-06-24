import logging
from pathlib import Path
import json
from typing import Dict

# Setup
CONFIG_DIR = Path.home() / ".db_backup"
CONFIG_FILE = CONFIG_DIR / "config.json"

CONFIG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(CONFIG_DIR / "backup.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def load_config() -> Dict:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError("Configuration file not found. Run 'init' first.")
    with CONFIG_FILE.open("r") as f:
        config = json.load(f)
    if "targets" not in config:
        logger.info("Migrating old config to multi-target format")
        config = {"targets": [{"id": config["database"]["name"] or "default", "database": config["database"], "backup": config["backup"]}]}
        save_config(config)
    return config

def save_config(config: Dict) -> None:
    with CONFIG_FILE.open("w") as f:
        json.dump(config, f, indent=4)
    logger.info(f"Configuration saved to {CONFIG_FILE}")

def validate_config(target: Dict) -> None:
    db_type = target["database"]["type"]
    if db_type == "sqlite":
        if not target["database"].get("path"):
            raise ValueError("SQLite requires 'path'")
    else:
        for field in ["name", "host", "port"]:
            if not target["database"].get(field):
                raise ValueError(f"{db_type} requires '{field}'")
        if db_type in ["postgresql", "mysql"]:
            for field in ["user", "password"]:
                if target["database"].get(field) is None:
                    raise ValueError(f"{db_type} requires '{field}'")
    if not target["backup"].get("local_path"):
        raise ValueError("Backup requires 'local_path'")
    if target["backup"]["cloud"]["type"] == "s3":
        for field in ["bucket", "access_key", "secret_key"]:
            if not target["backup"]["cloud"]["s3"].get(field):
                raise ValueError(f"S3 requires '{field}'")