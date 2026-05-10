import requests
from pipeline.config import  location
from airflow.models import Variable
from kafka import KafkaProducer
import json

def extract():
    topic = 'raw_topic'
    producer = KafkaProducer(
        bootstrap_servers=['kafka1:9092','kafka2:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        request_timeout_ms=30000,
        metadata_max_age_ms=10000,
        retries=10,
        max_block_ms=30000
    )
    OPENWEATHER_API_KEY =  Variable.get("openweather_api_key")
    if not OPENWEATHER_API_KEY:
        raise ValueError("Missing OPENWEATHER_API_KEY")

    cities = location()
    results = []
    failed = 0

    for city in cities:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            "q": city,
            "appid": OPENWEATHER_API_KEY,
            "units": "metric",
        }

        try:
            response = requests.get(url, params=params, timeout=20)

            if response.status_code != 200:
                print(f"❌ Failed for {city}: {response.text}")
                failed += 1
                continue

            weather_data = response.json()
            print(f"✅ Success: {city}")
                       
            producer.send(topic, value=weather_data)
            producer.flush()   
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Request error for {city}: {e}")
            failed += 1

    producer.close()

    if failed == len(cities):
        raise Exception("All API requests failed")
    
    print(f"Finished: {len(cities) - failed} success, {failed} failed")
