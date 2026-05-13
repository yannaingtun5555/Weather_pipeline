from psycopg2 import sql
from pipeline.config import get_db_config
import psycopg2  
from datetime import timedelta

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
            
            # NEW: Show row count per city
            cur.execute("""
                SELECT 
                    city, 
                    COUNT(*) as row_count
                FROM weather
                WHERE timestamp >= %s
                GROUP BY city
                ORDER BY row_count DESC, city
            """, (time_threshold,))
            
            city_counts = cur.fetchall()
            
            print("\n📊 ROW COUNT PER CITY:")
            print("-" * 40)
            for city, count in city_counts:
                # Create a simple bar visualization
                bar = "█" * min(count, 50)  # Cap at 50 for readability
                print(f"  {city:<20} : {count:>4} rows {bar}")
            
            # Show summary
            total_city_rows = sum(count for _, count in city_counts)
            print("-" * 40)
            print(f"  {'TOTAL':<20} : {total_city_rows:>4} rows")
            
            if result[0] > 0:
                print("\n✅ SUCCESS: Data found in database!")
                
                # Show sample of inserted data (most recent per city)
                cur.execute("""
                    SELECT DISTINCT ON (city) 
                        city, temperature, humidity, timestamp
                    FROM weather
                    WHERE timestamp >= %s
                    ORDER BY city, timestamp DESC
                    LIMIT 5
                """, (time_threshold,))
                
                print("\n📊 Latest record per city (sample up to 5 cities):")
                for row in cur.fetchall():
                    print(f"  {row[0]}: {row[1]}°C, {row[2]}% humidity at {row[3]}")
                
                return True
            else:
                print("❌ ERROR: No data found in database for this run!")
                raise ValueError("Database verification failed - no data inserted")
                
        else:
            # Fallback: show city counts for entire table
            cur.execute("""
                SELECT 
                    city, 
                    COUNT(*) as row_count
                FROM weather
                GROUP BY city
                ORDER BY row_count DESC, city
            """)
            
            city_counts = cur.fetchall()
            
            total = sum(count for _, count in city_counts)
            print(f"\n📊 ROW COUNT PER CITY (all time):")
            print("-" * 40)
            for city, count in city_counts:
                bar = "█" * min(count, 50)
                print(f"  {city:<20} : {count:>4} rows {bar}")
            print("-" * 40)
            print(f"  {'TOTAL':<20} : {total:>4} rows")
            
            if total > 0:
                print("\n✅ Database has data")
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