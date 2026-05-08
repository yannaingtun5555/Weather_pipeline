import psycopg2

from pipeline.config import get_db_config

def load_data(data_list):
    conn = None
    try:
        conn = psycopg2.connect(**get_db_config())
        cur = conn.cursor()
        
        for data in data_list:  
            columns = list(data.keys())
            placeholders = ", ".join([f"%({col})s" for col in columns])
            sql = f"""
                INSERT INTO weather
                ({', '.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT (city, timestamp) DO NOTHING
            """
            cur.execute(sql, data)
        
        conn.commit()
        cur.close()
        return True
    except psycopg2.Error as exc:
        print(f"Database error loading weather row: {exc}")
        raise  
    finally:
        if conn:
            conn.close()