import psycopg2
from kafka import KafkaConsumer
from pipeline.config import get_db_config
import json

def load_data():
    consumer = KafkaConsumer(
        'transformed_topic',
        bootstrap_servers=['kafka1:9092','kafka2:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='weather_load',
        consumer_timeout_ms= 10 * 1000

    )
    print("Kafka consumer initialized, waiting for messages...")
    data_list = []
    BATCH_SIZE = 4

    for message in consumer:
        data = message.value
        data_list.append(data)

        print(f"Received for loading: {data['city']} at {data['timestamp']}")

        if len(data_list) >= BATCH_SIZE:
            if load_database(data_list):
                print(f"Loaded batch of {len(data_list)} records to database")
                data_list.clear()
            else:
                print("Failed to load batch to database")

def load_database(data_list):   
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

            # ✅ log what you're inserting
            print(f"Inserted/Processed city: {data['city']} at {data['timestamp']}")

        conn.commit()
        cur.close()
        return True           
# 
            
    except psycopg2.Error as exc:
        print(f"Database error loading weather row: {exc}")
        raise  

    finally:
        if conn:
            conn.close()