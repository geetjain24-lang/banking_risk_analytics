import csv
from db_connection import get_connection
from config_loader import get_sql_path, get_data_path


def run_sql_file(cursor, file_path):
    with open(file_path, "r") as f:
        sql_script = f.read()
    cursor.execute(sql_script)


def tables_exist(cursor):
    """Check if the Customer table already exists in the database."""
    cursor.execute(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_NAME = 'Customer'"
    )
    return cursor.fetchone()[0] > 0


def setup_database(cursor):
    """Run CREATE and INSERT scripts once."""
    print("Creating tables...")
    run_sql_file(cursor, get_sql_path("create_tables.sql"))
    print("Inserting sample data...")
    run_sql_file(cursor, get_sql_path("insert_table.sql"))
    print("Database setup completed.")


def run_risk_scoring(cursor):
    """Execute risk scoring query and return results."""
    sql_path = get_sql_path("risk_logic.sql")
    with open(sql_path, "r") as f:
        sql_script = f.read()
    cursor.execute(sql_script)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return columns, rows


def export_to_csv(columns, rows):
    """Write risk scoring results to CSV."""
    output_path = get_data_path("risk_output.csv")
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    print(f"Results exported to {output_path} ({len(rows)} rows)")


def main():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Step 1: One-time setup — only runs if tables don't exist yet
        if not tables_exist(cursor):
            setup_database(cursor)
            conn.commit()
        else:
            print("Tables already exist — skipping setup.")

        # Step 2: Run risk scoring and export
        print("Running risk scoring...")
        columns, rows = run_risk_scoring(cursor)
        export_to_csv(columns, rows)
        print("Pipeline completed.")

    except Exception as e:
        print(f"Pipeline failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()