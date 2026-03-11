import sqlite3
import logging
import os

import pandas as pd
import yaml

logger = logging.getLogger(__name__)


def load_config(config_path="strategic-recon/config/config.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_db_connection(config):
    db_path = config["database"]["path"]
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return sqlite3.connect(db_path)


def seed_database(config):
    """Load sample CSVs into SQLite database."""
    conn = get_db_connection(config)

    for system_name, system_config in config["source_systems"].items():
        csv_path = system_config["csv"]
        table_name = system_config["table"]

        if not os.path.exists(csv_path):
            logger.warning(f"CSV not found: {csv_path}, skipping {system_name}")
            continue

        df = pd.read_csv(csv_path)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        logger.info(f"Loaded {len(df)} rows into '{table_name}' from {csv_path}")

    # Load world check data
    wc_config = config["world_check"]
    wc_csv = wc_config["csv"]
    if os.path.exists(wc_csv):
        df_wc = pd.read_csv(wc_csv)
        df_wc.to_sql(wc_config["table"], conn, if_exists="replace", index=False)
        logger.info(f"Loaded {len(df_wc)} World Check entities from {wc_csv}")

    conn.close()
    logger.info("Database seeding complete")


def query_source_system(config, system_name):
    """Query all rows from a source system table."""
    conn = get_db_connection(config)
    table_name = config["source_systems"][system_name]["table"]
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    logger.info(f"Queried {len(df)} rows from '{table_name}'")
    return df


def query_world_check(config):
    """Query all World Check entities."""
    conn = get_db_connection(config)
    table_name = config["world_check"]["table"]
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    logger.info(f"Queried {len(df)} World Check entities")
    return df
