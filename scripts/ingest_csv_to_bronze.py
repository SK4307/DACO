import argparse
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def build_engine():
    load_dotenv()

    host = os.environ["SQLSERVER_HOST"]
    port = os.getenv("SQLSERVER_PORT", "1433")
    database = os.environ["SQLSERVER_DATABASE"]
    username = os.environ["SQLSERVER_USERNAME"]
    password = os.environ["SQLSERVER_PASSWORD"]
    driver = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
    trust_certificate = os.getenv("SQLSERVER_TRUST_CERTIFICATE", "yes")

    odbc = (
        f"DRIVER={{{driver}}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate={trust_certificate};"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}", fast_executemany=True)


def normalize_column_name(value):
    cleaned = "".join(char.lower() if char.isalnum() else "_" for char in str(value).strip())
    cleaned = "_".join(part for part in cleaned.split("_") if part)
    return cleaned or "unnamed_column"


def dedupe_columns(columns):
    seen = {}
    output = []
    for column in columns:
        base = normalize_column_name(column)
        count = seen.get(base, 0)
        seen[base] = count + 1
        output.append(base if count == 0 else f"{base}_{count + 1}")
    return output


def ensure_schema_and_log_table(engine, schema):
    with engine.begin() as conn:
        conn.execute(text(f"IF SCHEMA_ID(:schema_name) IS NULL EXEC('CREATE SCHEMA [{schema}]')"), {"schema_name": schema})
        conn.execute(
            text(
                f"""
                IF OBJECT_ID('[{schema}].[ingestion_file_log]', 'U') IS NULL
                CREATE TABLE [{schema}].[ingestion_file_log] (
                    ingestion_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
                    source_name NVARCHAR(256) NOT NULL,
                    source_file NVARCHAR(1024) NOT NULL,
                    target_table NVARCHAR(512) NOT NULL,
                    batch_id UNIQUEIDENTIFIER NOT NULL,
                    status NVARCHAR(32) NOT NULL,
                    row_count BIGINT NULL,
                    error_message NVARCHAR(MAX) NULL,
                    started_at_utc DATETIME2 NOT NULL,
                    completed_at_utc DATETIME2 NULL
                )
                """
            )
        )


def ensure_bronze_table(engine, schema, table, columns):
    column_definitions = ",\n                    ".join(f"[{column}] NVARCHAR(MAX) NULL" for column in columns)
    metadata_definitions = """
                    [_batch_id] UNIQUEIDENTIFIER NOT NULL,
                    [_source_file] NVARCHAR(1024) NOT NULL,
                    [_source_row_number] BIGINT NOT NULL,
                    [_loaded_at_utc] DATETIME2 NOT NULL
    """

    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                IF OBJECT_ID('[{schema}].[{table}]', 'U') IS NULL
                CREATE TABLE [{schema}].[{table}] (
                    {column_definitions},
                    {metadata_definitions}
                )
                """
            )
        )
        for column in columns:
            conn.execute(
                text(
                    f"""
                    IF COL_LENGTH('[{schema}].[{table}]', :column_name) IS NULL
                    ALTER TABLE [{schema}].[{table}] ADD [{column}] NVARCHAR(MAX) NULL
                    """
                ),
                {"column_name": column},
            )


def insert_log(engine, schema, record):
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO [{schema}].[ingestion_file_log] (
                    ingestion_id, source_name, source_file, target_table, batch_id,
                    status, row_count, error_message, started_at_utc, completed_at_utc
                )
                VALUES (
                    :ingestion_id, :source_name, :source_file, :target_table, :batch_id,
                    :status, :row_count, :error_message, :started_at_utc, :completed_at_utc
                )
                """
            ),
            record,
        )


def update_log(engine, schema, ingestion_id, status, row_count=None, error_message=None):
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                UPDATE [{schema}].[ingestion_file_log]
                SET status = :status,
                    row_count = :row_count,
                    error_message = :error_message,
                    completed_at_utc = :completed_at_utc
                WHERE ingestion_id = :ingestion_id
                """
            ),
            {
                "ingestion_id": ingestion_id,
                "status": status,
                "row_count": row_count,
                "error_message": error_message,
                "completed_at_utc": datetime.now(timezone.utc),
            },
        )


def move_file(source_file, root, source_name):
    target_dir = Path(root) / source_name
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / source_file.name

    if target_path.exists():
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        target_path = target_dir / f"{source_file.stem}_{timestamp}{source_file.suffix}"

    shutil.move(str(source_file), str(target_path))


def load_file(engine, source_config, source_file, archive_root, rejected_root):
    source_name = source_config["name"]
    schema = source_config.get("bronze_schema", "bronze")
    table = source_config["bronze_table"]
    batch_id = uuid.uuid4()
    ingestion_id = uuid.uuid4()
    started_at = datetime.now(timezone.utc)
    row_count = 0

    ensure_schema_and_log_table(engine, schema)
    insert_log(
        engine,
        schema,
        {
            "ingestion_id": ingestion_id,
            "source_name": source_name,
            "source_file": str(source_file),
            "target_table": f"{schema}.{table}",
            "batch_id": batch_id,
            "status": "RUNNING",
            "row_count": None,
            "error_message": None,
            "started_at_utc": started_at,
            "completed_at_utc": None,
        },
    )

    try:
        header = 0 if source_config.get("has_header", True) else None
        reader = pd.read_csv(
            source_file,
            sep=source_config.get("delimiter", ","),
            encoding=source_config.get("encoding", "utf-8"),
            header=header,
            dtype=str,
            chunksize=int(source_config.get("chunksize", 50000)),
            keep_default_na=False,
        )

        for chunk_index, chunk in enumerate(reader):
            if header is None:
                chunk.columns = [f"column_{index + 1}" for index in range(len(chunk.columns))]
            chunk.columns = dedupe_columns(chunk.columns)

            ensure_bronze_table(engine, schema, table, list(chunk.columns))

            chunk["_batch_id"] = str(batch_id)
            chunk["_source_file"] = str(source_file)
            chunk["_source_row_number"] = range(row_count + 1, row_count + len(chunk) + 1)
            chunk["_loaded_at_utc"] = datetime.now(timezone.utc)
            chunk.to_sql(table, engine, schema=schema, if_exists="append", index=False, method=None)

            row_count += len(chunk)
            print(f"Loaded {len(chunk)} rows from {source_file.name}, chunk {chunk_index + 1}")

        update_log(engine, schema, ingestion_id, "SUCCEEDED", row_count=row_count)
        move_file(source_file, archive_root, source_name)
    except Exception as exc:
        update_log(engine, schema, ingestion_id, "FAILED", row_count=row_count, error_message=str(exc))
        move_file(source_file, rejected_root, source_name)
        raise


def load_config(path):
    with open(path, "r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/sources.yml")
    args = parser.parse_args()

    config = load_config(args.config)
    engine = build_engine()

    landing_root = Path(config.get("landing_root", "landing/incoming"))
    archive_root = Path(config.get("archive_root", "landing/archive"))
    rejected_root = Path(config.get("rejected_root", "landing/rejected"))

    for source in config["sources"]:
        source_dir = landing_root / source["name"]
        source_dir.mkdir(parents=True, exist_ok=True)
        files = sorted(source_dir.glob(source.get("file_pattern", "*.csv")))

        if not files:
            print(f"No files found for source '{source['name']}' in {source_dir}")
            continue

        for source_file in files:
            load_file(engine, source, source_file, archive_root, rejected_root)


if __name__ == "__main__":
    main()
