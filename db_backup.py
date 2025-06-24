#!/usr/bin/env python3
import os
import sys
import argparse
import re
from glob import glob
from pathlib import Path
from configs.init import logger
from configs.init import load_config, save_config, validate_config
from operations.backup_restore import perform_backup, perform_restore, find_latest_backup
from scheduler.init import schedule_backups


# load .env if found
def load_env_file(filepath):
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' not in line:
                continue
            key, val = line.split('=', 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            os.environ.setdefault(key, val)

env_file = Path(__file__).parent / '.env'
if env_file.exists():
    load_env_file(env_file)

def prompt_for_input(
    prompt: str,
    default: str = "",
    required: bool = False,
    is_password: bool = False,
    allow_empty: bool = False
) -> str:
    while True:
        if is_password:
            value = input(f"{prompt}: ")
        else:
            value = input(f"{prompt} [{default}]: ").strip()
        if value != "":
            return value
        if allow_empty:
            return ""
        if default != "":
            return default
        if not required:
            return value
        print("This field is required.")

def sanitize_id(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "_", name).strip("_").lower() or "default"

def main():
    parser = argparse.ArgumentParser(description="Database Backup CLI")
    subparsers = parser.add_subparsers(dest="command")

    init = subparsers.add_parser("init", help="Initialize backup config")
    init.add_argument("--id", help="Target ID (auto-generated if omitted)")
    init.add_argument("--db-type", help="Database type (postgresql/mysql/mongodb/sqlite)")
    init.add_argument("--db-name", help="Database name or SQLite file path")
    init.add_argument("--db-host", help="Database host")
    init.add_argument("--db-port", type=int, help="Database port")
    init.add_argument("--db-user", help="Database user")
    init.add_argument("--db-password", help="Database password")
    init.add_argument("--backup-path", help="Local backup path")
    init.add_argument("--schedule", choices=["hourly", "daily", "weekly"], help="Backup schedule")
    init.add_argument("--cloud", choices=["none", "s3"], help="Cloud storage")
    init.add_argument("--s3-bucket", help="S3 bucket name")
    init.add_argument("--s3-access-key", help="S3 access key")
    init.add_argument("--s3-secret-key", help="S3 secret key")
    init.add_argument("--interactive", action="store_true")

    backup = subparsers.add_parser("backup", help="Perform backup")
    backup.add_argument("--id", help="Target ID (all if omitted)")

    restore = subparsers.add_parser("restore", help="Restore database")
    restore.add_argument("--id", required=True, help="Target ID")
    restore.add_argument("--file", help="Backup file (latest if omitted)")
    restore.add_argument("--force", action="store_true")
    restore.add_argument("--interactive", action="store_true")

    schedule = subparsers.add_parser("schedule", help="Start scheduler")
    schedule.add_argument("--id", help="Target ID (all if omitted)")

    list_cmd = subparsers.add_parser("list", help="List targets")
    list_cmd.add_argument("--show-backups", action="store_true")

    args = parser.parse_args()

    if args.command == "init":
        config = load_config()
        target = {
            "id": "",
            "database": {"type": "", "name": "", "host": "localhost", "port": 0, "user": "", "password": "", "path": ""},
            "backup": {"local_path": "", "schedule": "daily", "cloud": {"type": "none", "s3": {"bucket": "", "access_key": "", "secret_key": ""}}},
        }

        if args.interactive or not args.db_type:
            target["database"]["type"] = prompt_for_input("Database type (postgresql/mysql/mongodb/sqlite)", required=True).lower()
        else:
            target["database"]["type"] = args.db_type.lower()

        if target["database"]["type"] == "sqlite":
            target["database"]["path"] = args.db_name or prompt_for_input("SQLite file path", required=True)
            target["database"]["name"] = Path(target["database"]["path"]).stem
        else:
            target["database"]["name"] = args.db_name or prompt_for_input("Database name", required=True)
            target["database"]["host"] = args.db_host or prompt_for_input("Database host", "localhost")
            target["database"]["port"] = args.db_port or int(prompt_for_input("Database port", "5432" if target["database"]["type"] == "postgresql" else "3306" if target["database"]["type"] == "mysql" else "27017", required=True))
            if target["database"]["type"] in ["postgresql", "mysql"]:
                target["database"]["user"] = args.db_user or prompt_for_input("Database user", required=True)
                target["database"]["password"] = args.db_password or prompt_for_input("Database password", required=True, is_password=True, allow_empty=True)
            elif target["database"]["type"] == "mongodb":
                target["database"]["user"] = args.db_user or prompt_for_input("Database user", default="", required=False, allow_empty=True)
                target["database"]["password"] = args.db_password or prompt_for_input("Database password", default="", is_password=True, allow_empty=True)

        target["backup"]["local_path"] = args.backup_path or prompt_for_input("Local backup path", required=True)
        target["backup"]["schedule"] = args.schedule or prompt_for_input("Schedule (hourly/daily/weekly)", "daily")
        target["backup"]["cloud"]["type"] = args.cloud or prompt_for_input("Cloud storage (none/s3)", "none")

        if target["backup"]["cloud"]["type"] == "s3":
            target["backup"]["cloud"]["s3"]["bucket"] = args.s3_bucket or prompt_for_input("S3 bucket name", required=True)
            target["backup"]["cloud"]["s3"]["access_key"] = args.s3_access_key or prompt_for_input("S3 access key", required=True)
            target["backup"]["cloud"]["s3"]["secret_key"] = args.s3_secret_key or prompt_for_input("S3 secret key", required=True, is_password=True)

        default_id = sanitize_id(target["database"]["name"])
        target["id"] = sanitize_id(args.id or prompt_for_input("Target ID", default_id, required=True))

        if target["id"] in [t["id"] for t in config["targets"]]:
            raise ValueError(f"ID '{target['id']}' already exists.")
        validate_config(target)
        config["targets"].append(target)
        save_config(config)
        logger.info(f"Target '{target['id']}' initialized.")

    elif args.command == "backup":
        config = load_config()
        targets = [t for t in config["targets"] if args.id is None or t["id"] == args.id]
        if not targets:
            raise ValueError(f"No targets found with id: {args.id}" if args.id else "No targets configured.")
        for target in targets:
            logger.info(f"Backing up target: {target['id']}")
            perform_backup(target)

    elif args.command == "restore":
        config = load_config()
        target = next((t for t in config["targets"] if t["id"] == args.id), None)
        if not target:
            raise ValueError(f"No target with id: {args.id}")
        backup_file = args.file
        if not backup_file and args.interactive:
            backups = sorted(glob(os.path.join(target["backup"]["local_path"], f"{target['database']['type']}_{args.id}_*.zip")), reverse=True)
            if not backups:
                logger.error(f"No backups found for target: {args.id}")
                sys.exit(1)
            for i, b in enumerate(backups, 1):
                print(f"{i}. {b}")
            choice = int(prompt_for_input("Select backup number", required=True)) - 1
            backup_file = backups[choice]
        elif not backup_file:
            backup_file = find_latest_backup(target, Path(target["backup"]["local_path"]))
            if not backup_file:
                logger.error(f"No backups found for target: {args.id}")
                sys.exit(1)
            backup_file = str(backup_file)
        logger.info(f"Restoring target: {args.id} from {backup_file}")
        perform_restore(target, backup_file, args.force)

    elif args.command == "schedule":
        config = load_config()
        schedule_backups(config["targets"], args.id)

    elif args.command == "list":
        config = load_config()
        if not config["targets"]:
            print("No backup targets configured.")
            return
        print("Backup targets:")
        for target in config["targets"]:
            print(f"- ID: {target['id']}")
            print(f"  Database: {target['database']['type']} ({target['database']['name']})")
            print(f"  Backup Path: {target['backup']['local_path']}")
            print(f"  Schedule: {target['backup']['schedule']}")
            print(f"  Cloud: {target['backup']['cloud']['type']}")
            if args.show_backups:
                backups = sorted(glob(os.path.join(target["backup"]["local_path"], f"{target['database']['type']}_{target['id']}_*.zip")), reverse=True)
                print("  Backups:" if backups else "  No backups found.")
                for b in backups:
                    print(f"    - {b}")
            print()

    else:
        parser.print_help()

if __name__ == "__main__":
    main()