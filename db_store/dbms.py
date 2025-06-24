from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict
from dep_manage.init import install_dependencies

class Handler(ABC):
    required_deps: list = []

    @classmethod
    def ensure_deps(cls, requirements: Dict[str, str]):
        install_dependencies(cls.required_deps, requirements)

class DBMSHandler(Handler):
    @abstractmethod
    def backup(self, target: Dict) -> Path:
        pass

    @abstractmethod
    def restore(self, target: Dict, backup_file: Path) -> None:
        pass

    def get_backup_filename(self, target: Dict, ext: str) -> Path:
        backup_dir = Path(target["backup"]["local_path"])
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return backup_dir / f"{target['database']['type']}_{target['id']}_{target['database']['name']}_{timestamp}.{ext}"