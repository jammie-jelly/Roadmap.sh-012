from pathlib import Path
from typing import Dict
from db_store.dbms import DBMSHandler
from dep_manage.init import load_requirements
from configs.init import logger


class PostgreSQLHandler(DBMSHandler):
    required_deps = ["psycopg[binary]"]

    def backup(self, target: Dict) -> Path:
        self.ensure_deps(load_requirements())
        import psycopg
        from psycopg import sql

        db_config = target["database"]
        backup_file = self.get_backup_filename(target, "sql")

        conn = None
        cursor = None
        try:
            conn = psycopg.connect(
                dbname=db_config["name"],
                user=db_config.get("user", ""),
                password=db_config.get("password", ""),
                host=db_config["host"],
                port=db_config["port"],
                autocommit=True
            )
            cursor = conn.cursor()

            with open(backup_file, "w", encoding="utf-8") as f:
                f.write("-- PostgreSQL Backup\n")
                f.write(f"-- Database: {db_config['name']}\n\n")

                # === Sequences ===
                f.write("-- Sequences\n")
                cursor.execute("""
                    SELECT schemaname, sequencename
                    FROM pg_sequences
                    WHERE schemaname = 'public'
                    ORDER BY sequencename
                """)
                sequences = cursor.fetchall()

                for schema, seq_name in sequences:
                    seq_id = sql.Identifier(seq_name)
                    cursor.execute(sql.SQL("""
                        SELECT start_value, increment_by, max_value, min_value, cache_size, cycle
                        FROM pg_sequences
                        WHERE schemaname = 'public' AND sequencename = {}
                    """).format(sql.Literal(seq_name)))
                    start, inc, maxv, minv, cache, cycle = cursor.fetchone()

                    cursor.execute(sql.SQL("SELECT last_value, is_called FROM {}").format(seq_id))
                    last_value, is_called = cursor.fetchone()

                    f.write(f"""CREATE SEQUENCE IF NOT EXISTS {seq_id.as_string(cursor)}
    START WITH {start}
    INCREMENT BY {inc}
    MINVALUE {minv}
    MAXVALUE {maxv}
    CACHE {cache}
    {"CYCLE" if cycle else "NO CYCLE"};\n""")
                    f.write(f"SELECT setval({sql.Literal(seq_name).as_string(cursor)}, {last_value}, {str(is_called).lower()});\n\n")

                # === Tables ===
                f.write("-- Tables\n")
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
                tables = [row[0] for row in cursor.fetchall()]

                for table in tables:
                    cursor.execute(sql.SQL("""
                        SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod),
                               NOT a.attnotnull AS is_nullable,
                               pg_get_expr(ad.adbin, ad.adrelid) AS default_value
                        FROM pg_attribute a
                        LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
                        WHERE a.attrelid = 'public.{}'::regclass AND a.attnum > 0 AND NOT a.attisdropped
                        ORDER BY a.attnum
                    """).format(sql.Identifier(table)))
                    columns = cursor.fetchall()

                    col_defs = []
                    for name, type_str, is_nullable, default_val in columns:
                        col_id = sql.Identifier(name).as_string(cursor)
                        col_def = f"{col_id} {type_str}"
                        if default_val is not None:
                            col_def += f" DEFAULT {default_val}"
                        if not is_nullable:
                            col_def += " NOT NULL"
                        col_defs.append(col_def)

                    f.write(sql.SQL("CREATE TABLE IF NOT EXISTS {} (\n  {}\n);\n\n").format(
                        sql.Identifier(table),
                        sql.SQL(",\n  ").join(map(sql.SQL, col_defs))
                    ).as_string(cursor))

                # === Constraints ===
                f.write("-- Constraints\n")
                for table in tables:
                    cursor.execute(sql.SQL("""
                        SELECT conname, pg_get_constraintdef(c.oid, true)
                        FROM pg_constraint c
                        JOIN pg_class t ON c.conrelid = t.oid
                        WHERE t.relname = {} AND t.relnamespace = 'public'::regnamespace
                        ORDER BY conname
                    """).format(sql.Literal(table)))
                    for name, defn in cursor.fetchall():
                        f.write(sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} {};\n").format(
                            sql.Identifier(table),
                            sql.Identifier(name),
                            sql.SQL(defn)
                        ).as_string(cursor))
                    f.write("\n")

                # === Table Data ===
                f.write("-- Table Data\n")
                for table in tables:
                    cursor.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table)))
                    rows = cursor.fetchall()
                    if not rows:
                        continue
                    col_names = [sql.Identifier(desc.name).as_string(cursor) for desc in cursor.description]
                    for row in rows:
                        values = [sql.Literal(v).as_string(cursor) if v is not None else 'NULL' for v in row]
                        f.write(sql.SQL("INSERT INTO {} ({}) VALUES ({});\n").format(
                            sql.Identifier(table),
                            sql.SQL(", ").join(map(sql.SQL, col_names)),
                            sql.SQL(", ").join(map(sql.SQL, values))
                        ).as_string(cursor))
                    f.write("\n")

                # === Indexes ===
                f.write("-- Indexes\n")
                cursor.execute("""
                    SELECT indexdef
                    FROM pg_indexes
                    WHERE schemaname = 'public' AND indexname NOT LIKE '%_pkey'
                    ORDER BY indexname
                """)
                for (index_def,) in cursor.fetchall():
                    f.write(f"{index_def};\n")
                f.write("\n")

                # === Views ===
                f.write("-- Views\n")
                cursor.execute("""
                    SELECT 'CREATE OR REPLACE VIEW ' || quote_ident(viewname) || ' AS ' || definition
                    FROM pg_views
                    WHERE schemaname = 'public'
                    ORDER BY viewname
                """)
                for (view_def,) in cursor.fetchall():
                    f.write(f"{view_def};\n")
                f.write("\n")

                # === Triggers ===
                f.write("-- Triggers\n")
                cursor.execute("""
                    SELECT pg_get_triggerdef(t.oid)
                    FROM pg_trigger t
                    JOIN pg_class c ON t.tgrelid = c.oid
                    WHERE c.relnamespace = 'public'::regnamespace AND NOT t.tgisinternal
                    ORDER BY t.tgname
                """)
                for (trigger_def,) in cursor.fetchall():
                    f.write(f"{trigger_def};\n")
                f.write("\n")

                # === Functions ===
                f.write("-- Functions\n")
                cursor.execute("""
                    SELECT pg_get_functiondef(p.oid)
                    FROM pg_proc p
                    WHERE p.pronamespace = 'public'::regnamespace
                    ORDER BY p.proname
                """)
                for (func_def,) in cursor.fetchall():
                    f.write(f"{func_def};\n")
                f.write("\n")

            logger.info(f"PostgreSQL backup created: {backup_file}")
            return backup_file

        except Exception as e:
            logger.error(f"Backup failed: {e}")
            raise

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def restore(self, target: Dict, backup_file: Path) -> None:
        self.ensure_deps(load_requirements())
        from psycopg import sql
        import psycopg

        db_config = target["database"]
        target_db = db_config["name"]
        admin_db = "template1" if target_db == "postgres" else "postgres"

        conn = None
        cursor = None
        try:
            # Connect to admin database to drop and recreate target database
            conn = psycopg.connect(
                dbname=admin_db,
                user=db_config.get("user", ""),
                password=db_config.get("password", ""),
                host=db_config["host"],
                port=db_config["port"],
                autocommit=True
            )
            cursor = conn.cursor()

            # Terminate active connections to the target database
            cursor.execute(sql.SQL("""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = {} AND pid <> pg_backend_pid()
            """).format(sql.Literal(target_db)))

            # Drop and recreate the target database
            cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(target_db)))
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_db)))
            cursor.close()
            conn.close()

            # Connect to the new database
            conn = psycopg.connect(
                dbname=target_db,
                user=db_config.get("user", ""),
                password=db_config.get("password", ""),
                host=db_config["host"],
                port=db_config["port"],
                autocommit=True
            )
            cursor = conn.cursor()

            # Parse and execute SQL statements
            with open(backup_file, "r", encoding="utf-8") as f:
                statements = []
                buffer = ""
                for line in f:
                    stripped = line.strip()
                    if not stripped or stripped.startswith("--"):
                        continue
                    buffer += line
                    if stripped.endswith(";"):
                        statements.append(buffer.strip())
                        buffer = ""

            # Execute statements, skipping redundant primary key constraints
            for stmt in statements:
                try:
                    if "ADD CONSTRAINT" in stmt.upper() and "_pkey" in stmt:
                        continue  # Skip primary key constraints to avoid duplicates
                    cursor.execute(stmt)
                except Exception as e:
                    logger.error(f"SQL execution failed:\n{stmt}\nError: {e}")
                    raise

            logger.info(f"PostgreSQL database restored: {target_db}")

        except Exception as e:
            logger.error(f"Restore failed: {e}")
            raise

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()