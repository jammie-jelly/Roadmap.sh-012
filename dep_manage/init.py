from pathlib import Path
import importlib
import subprocess
from typing import Dict
import sys
from configs.init import logger

REQUIREMENTS_FILE = Path(__file__).parent.parent / "requirements.txt"

# Dependencies
DEPENDENCY_GROUPS = {
    "database": {"postgresql": ["psycopg[binary]"], "mysql": ["mysql-connector-python"], "mongodb": ["pymongo"], "sqlite": []},
    "storage": {"local": [], "s3": ["boto3"]},
}

def load_requirements() -> Dict[str, str]:
    if not REQUIREMENTS_FILE.exists():
        raise FileNotFoundError(f"requirements.txt not found at {REQUIREMENTS_FILE}")
    requirements = {}
    with REQUIREMENTS_FILE.open("r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                package = line.split("==")[0] if "==" in line else line
                requirements[package] = line
    return requirements

def install_dependencies(dep_list: list, requirements: Dict[str, str]) -> None:
    for dep in dep_list:
        full_dep = requirements.get(dep, dep)
        try:
            importlib.import_module(dep.replace("-", "_"))
        except ImportError:
            logger.info(f"Installing {full_dep}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", full_dep])