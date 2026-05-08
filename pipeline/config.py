import os


def get_db_config():
    return {
        "dbname": os.getenv("PGDATABASE", "weather"),
        "user": os.getenv("PGUSER", "ynt"),
        "password": os.getenv("PGPASSWORD", "ynt"),
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432")),
    }

def location():
    return {"Yangon","Pathein","Naypyidaw","mandalay"}


OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
