from abc import abstractmethod
from pathlib import Path
from typing import Dict
from dep_manage.init import load_requirements
from configs.init import logger
from dep_manage.init import DEPENDENCY_GROUPS
import shutil
from db_store.dbms import Handler


class StorageHandler(Handler):
    @abstractmethod
    def store(self, file_path: Path, target: Dict) -> None:
        pass

    @abstractmethod
    def retrieve(self, file_path: str, target: Dict, local_path: Path) -> Path:
        pass

class LocalStorageHandler(StorageHandler):
    required_deps = DEPENDENCY_GROUPS["storage"]["local"]

    def store(self, file_path: Path, target: Dict) -> None:
        self.ensure_deps(load_requirements())
        logger.info(f"Backup stored locally: {file_path}")

    def retrieve(self, file_path: str, target: Dict, local_path: Path) -> Path:
        self.ensure_deps(load_requirements())
        src_path = Path(file_path)
        if not src_path.exists():
            raise FileNotFoundError(f"Backup file not found: {file_path}")
        dest_path = local_path / src_path.name
        shutil.copy2(src_path, dest_path)
        logger.info(f"Backup retrieved locally: {dest_path}")
        return dest_path

class S3StorageHandler(StorageHandler):
    required_deps = DEPENDENCY_GROUPS["storage"]["s3"]

    def store(self, file_path: Path, target: Dict) -> None:
        self.ensure_deps(load_requirements())
        import boto3
        from botocore.exceptions import ClientError
        s3_config = target["backup"]["cloud"]["s3"]
        try:
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=s3_config["access_key"],
                aws_secret_access_key=s3_config["secret_key"],
            )
            s3_client.upload_file(str(file_path), s3_config["bucket"], file_path.name)
            logger.info(f"Backup uploaded to S3: s3://{s3_config['bucket']}/{file_path.name}")
        except ClientError as e:
            logger.error(f"S3 upload failed: {e}")
            raise
        except Exception as e:
            logger.error(f"S3 storage error: {e}")
            raise

    def retrieve(self, file_path: str, target: Dict, local_path: Path) -> Path:
        self.ensure_deps(load_requirements())
        import boto3
        from botocore.exceptions import ClientError
        s3_config = target["backup"]["cloud"]["s3"]
        file_name = Path(file_path).name
        dest_path = local_path / file_name
        try:
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=s3_config["access_key"],
                aws_secret_access_key=s3_config["secret_key"],
            )
            s3_client.download_file(s3_config["bucket"], file_name, str(dest_path))
            logger.info(f"Backup retrieved from S3: {dest_path}")
            return dest_path
        except ClientError as e:
            logger.error(f"S3 download failed: {e}")
            raise
