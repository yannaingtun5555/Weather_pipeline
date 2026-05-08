import psycopg2
from psycopg2 import sql

# Database connection settings
DB_CONFIG = {
    "dbname": "weather",
    "user": "ynt",
    "password": "ynt",   
    "host": "localhost",
    "port": 5432
}

# SQL statement to create the table (if not exists)
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather (
    id SERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    country CHAR(2),
    timestamp TIMESTAMPTZ NOT NULL,
    temp_celsius NUMERIC(5,2),
    feels_like_celsius NUMERIC(5,2),
    temp_min_celsius NUMERIC(5,2),
    temp_max_celsius NUMERIC(5,2),
    humidity INTEGER,
    pressure_hpa INTEGER,
    wind_speed_ms NUMERIC(5,2),
    wind_deg INTEGER,
    weather_main TEXT,
    weather_description TEXT,
    clouds_percent INTEGER,
    visibility_meters INTEGER,
    sunrise TIMESTAMPTZ,
    sunset TIMESTAMPTZ,
    UNIQUE(city, timestamp)
);
"""

def create_table():
    """Connect to PostgreSQL and create the weather table if it doesn't exist."""
    conn = None
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Execute the CREATE TABLE statement
        cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        
        print("✅ Table 'weather' created successfully (or already exists).")
        
        # Optional: Verify by listing tables
        cur.execute("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' AND tablename = 'weather';
        """)
        if cur.fetchone():
            print("   ✓ Confirmed: table is present in the database.")
        
        cur.close()
        return True
    except psycopg2.Error as e:
        print("❌ Database error:", e)
    except Exception as e:
        print("❌ General error:", e)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    create_table()