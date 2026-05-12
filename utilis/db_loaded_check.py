from psycopg2 import sql
from pipeline.config import get_db_config
import psycopg2  

def verify_database_insertion(**context):
    """
    Verify that data was successfully inserted into the database
    """
    conn = None
    try:
        conn = psycopg2.connect(**get_db_config())
        cur = conn.cursor()
        
        # Get execution date from context (when the DAG run started)
        execution_date = context.get('execution_date')
        if execution_date:
            # Look for data inserted in the last hour
            time_threshold = execution_date - timedelta(hours=1)
            
            cur.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT city) as unique_cities,
                    MIN(timestamp) as earliest_record,
                    MAX(timestamp) as latest_record
                FROM weather
                WHERE timestamp >= %s
            """, (time_threshold,))
            
            result = cur.fetchone()
            
            print("\n=== DATABASE VERIFICATION ===")
            print(f"Total records inserted: {result[0]}")
            print(f"Unique cities: {result[1]}")
            print(f"Earliest record: {result[2]}")
            print(f"Latest record: {result[3]}")
            
            if result[0] > 0:
                print("✅ SUCCESS: Data found in database!")
                
                # Show sample of inserted data
                cur.execute("""
                    SELECT city, temperature, humidity, timestamp
                    FROM weather
                    WHERE timestamp >= %s
                    ORDER BY timestamp DESC
                    LIMIT 5
                """, (time_threshold,))
                
                print("\n📊 Latest 5 records:")
                for row in cur.fetchall():
                    print(f"  {row[0]}: {row[1]}°C, {row[2]}% humidity at {row[3]}")
                
                return True
            else:
                print("❌ ERROR: No data found in database for this run!")
                raise ValueError("Database verification failed - no data inserted")
                
        else:
            # Fallback: just check total count
            cur.execute("SELECT COUNT(*) FROM weather")
            total = cur.fetchone()[0]
            print(f"Total records in weather table: {total}")
            
            if total > 0:
                print("✅ Database has data")
                return True
            else:
                raise ValueError("Database is empty!")
                
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()