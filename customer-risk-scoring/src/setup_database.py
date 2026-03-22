"""
Standalone database setup script.
Note: run_pipeline.py handles setup automatically on first run.
Use this only if you want to set up the database independently.
"""
from db_connection import get_connection
from config_loader import get_sql_path


def run_sql_file(cursor, file_path):
    with open(file_path, "r") as f:
        sql_script = f.read()
    cursor.execute(sql_script)


def setup_database():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        print("Creating tables...")
        run_sql_file(cursor, get_sql_path("create_tables.sql"))

        print("Inserting initial data...")
        run_sql_file(cursor, get_sql_path("insert_table.sql"))

        conn.commit()
        print("Database setup completed.")
    except Exception as e:
        print(f"Setup failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    setup_database()