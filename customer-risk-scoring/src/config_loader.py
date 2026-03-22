import os
import yaml

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def load_config():
    config_path = os.path.join(BASE_DIR, "config", "config.yaml")
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config

def get_sql_path(filename):
    return os.path.join(BASE_DIR, "sql", filename)

def get_data_path(filename):
    return os.path.join(BASE_DIR, "data", filename)