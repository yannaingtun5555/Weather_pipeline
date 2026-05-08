import os


def get_db_config():
    return {
        "dbname": os.getenv("PGDATABASE", "weather"),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", "postgres"),
        "host": os.getenv("PGHOST", "postgres"),
        "port": int(os.getenv("PGPORT", "5432")),
    }

def location():
    return {"Yangon","Pathein","Naypyidaw","Mandalay"}


OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
