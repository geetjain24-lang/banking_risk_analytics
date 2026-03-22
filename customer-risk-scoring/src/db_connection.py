import pyodbc
from config_loader import load_config

def get_connection():
    config = load_config()
    db = config["database"]
    conn = pyodbc.connect(
        f"DRIVER={{{db['driver']}}};"
        f"SERVER={db['server']};"
        f"DATABASE={db['database']};"
        "Trusted_Connection=yes;"
    )
    return conn