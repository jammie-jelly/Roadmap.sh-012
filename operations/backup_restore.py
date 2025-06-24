import os
import tempfile
import zipfile
from glob import glob
from pathlib import Path
from typing import Dict, Optional
from db_store.dbms_handler import get_dbms_handler, get_storage_handler
from configs.init import logger
from configs.init import validate_config


def find_latest_backup(target: Dict, local_path: Path) -> Optional[Path]:
    pattern = f"{target['database']['type']}_{target['id']}_{target['database']['name']}*.zip"
    files = sorted(glob(str(local_path / pattern)), key=os.path.getmtime, reverse=True)
    return Path(files[0]) if files else None

def perform_backup(target: Dict) -> None:
    validate_config(target)
    dbms_handler = get_dbms_handler(target["database"]["type"])
    backup_file = dbms_handler.backup(target)
    backup_file = compress_backup(backup_file)
    storage_handler = get_storage_handler(target["backup"]["cloud"]["type"])
    storage_handler.store(backup_file, target)

def perform_restore(target: Dict, backup_file: str, force: bool = False) -> None:
    validate_config(target)
    db_type = target["database"]["type"]
    expected_ext = ".db" if db_type == "sqlite" else ".sql" if db_type in ["postgresql", "mysql"] else ".archive"
    if not force:
        confirm = input(f"Restore {db_type} database '{target['database']['name']}' from {backup_file}? (y/n): ").strip().lower()
        if confirm != "y":
            logger.info("Restore cancelled.")
            return
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        storage_handler = get_storage_handler(target["backup"]["cloud"]["type"])
        local_backup = storage_handler.retrieve(backup_file, target, tmp_path)
        decompressed_file = decompress_backup(local_backup, tmp_path)
        if decompressed_file.suffix != expected_ext:
            raise ValueError(f"Invalid backup file for {db_type}: expected {expected_ext}, got {decompressed_file.suffix}")
        dbms_handler = get_dbms_handler(db_type)
        dbms_handler.restore(target, decompressed_file)

def compress_backup(file_path: Path) -> Path:
    compressed_file = file_path.with_suffix(file_path.suffix + ".zip")
    with zipfile.ZipFile(compressed_file, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(file_path, file_path.name)
    file_path.unlink()
    logger.info(f"Backup compressed: {compressed_file}")
    return compressed_file

def decompress_backup(compressed_file: Path, extract_path: Path) -> Path:
    with zipfile.ZipFile(compressed_file, "r") as zf:
        zf.extractall(extract_path)
    extracted_file = extract_path / compressed_file.name.replace(".zip", "")
    if not extracted_file.exists():
        raise FileNotFoundError(f"Decompressed file not found: {extracted_file}")
    logger.info(f"Backup decompressed: {extracted_file}")
    return extracted_file
