#import logging
from pathlib import Path
from typing import Dict
from dep_manage.init import load_requirements
from configs.init import logger
from dep_manage.init import DEPENDENCY_GROUPS
from db_store.dbms import DBMSHandler


class MySQLHandler(DBMSHandler):
    required_deps = DEPENDENCY_GROUPS["database"]["mysql"]

    def backup(self, target: Dict) -> Path:
        self.ensure_deps(load_requirements())
        import mysql.connector
        from mysql.connector import Error
        db_config = target["database"]
        backup_file = self.get_backup_filename(target, "sql")

        try:
            connection = mysql.connector.connect(
                host=db_config["host"],
                port=db_config["port"],
                user=db_config["user"],
                password=db_config["password"],
                database=db_config["name"]
            )

            with open(backup_file, "w") as f:
                cursor = connection.cursor()
                # Get all tables in the database
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()

                # Generate SQL dump
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
                    create_table = cursor.fetchone()[1]
                    f.write(f"{create_table};\n\n")

                    cursor.execute(f"SELECT * FROM `{table_name}`")
                    rows = cursor.fetchall()
                    if rows:
                        columns = [desc[0] for desc in cursor.description]
                        for row in rows:
                            values = ', '.join([f"'{str(v).replace('\'', '\\\'')}'" if v is not None else 'NULL' for v in row])
                            f.write(f"INSERT INTO `{table_name}` (`{'`, `'.join(columns)}`) VALUES ({values});\n")
                        f.write("\n")

                cursor.close()

            logger.info(f"MySQL backup created: {backup_file}")
            return backup_file

        except Error as e:
            logger.error(f"Backup failed: {e}")
            raise
        finally:
            if connection.is_connected():
                connection.close()

    def restore(self, target: Dict, backup_file: Path, force: bool = False) -> None:
        self.ensure_deps(load_requirements())
        import mysql.connector
        from mysql.connector import Error
        db_config = target["database"]

        try:
            connection = mysql.connector.connect(
                host=db_config["host"],
                port=db_config["port"],
                user=db_config["user"],
                password=db_config["password"]
            )

            cursor = connection.cursor()
            if force:
                logger.info(f"Dropping database `{db_config['name']}` due to --force flag")
                cursor.execute(f"DROP DATABASE IF EXISTS `{db_config['name']}`")
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_config['name']}`")
            cursor.close()
            connection.close()

            # Connect to the target database
            connection = mysql.connector.connect(
                host=db_config["host"],
                port=db_config["port"],
                user=db_config["user"],
                password=db_config["password"],
                database=db_config["name"]
            )

            connection.autocommit = False
            cursor = connection.cursor()

            try:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

                if force:
                    # Clear existing tables if --force is enabled
                    cursor.execute("SHOW TABLES")
                    tables = cursor.fetchall()
                    for table in tables:
                        table_name = table[0]
                        cursor.execute(f"DROP TABLE `{table_name}`")

                with open(backup_file, "r") as f:
                    sql_statements = f.read().split(";\n")

                for statement in sql_statements:
                    statement = statement.strip()
                    if not statement or statement.startswith("SET FOREIGN_KEY_CHECKS"):
                        continue

                    # Convert INSERT to INSERT IGNORE to handle duplicates
                    if statement.startswith("INSERT INTO"):
                        statement = statement.replace("INSERT INTO", "INSERT IGNORE INTO", 1)

                    try:
                        cursor.execute(statement)
                    except mysql.connector.errors.ProgrammingError as e:
                        if e.errno == 1050:  # Table already exists
                            logger.warning(f"Skipping table creation: {e}")
                            continue
                        elif e.errno == 1062:  # Duplicate entry
                            logger.warning(f"Skipping duplicate entry: {e}")
                            continue
                        else:
                            logger.error(f"Statement failed: {statement[:100]}... {e}")
                            raise

                cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
                connection.commit()
                logger.info(f"MySQL database restored: {db_config['name']}")

            except Exception as e:
                connection.rollback()
                logger.error(f"Restore failed, transaction rolled back: {e}")
                raise

        except Error as e:
            logger.error(f"Restore failed: {e}")
            raise
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
