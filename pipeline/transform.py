from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
import json

def transform_and_stream():
    # Consumer from weather1
    consumer = KafkaConsumer(
        'raw_topic',
        bootstrap_servers=['kafka1:9092','kafka2:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='weather_transform',
        consumer_timeout_ms= 10 * 1000
    )

    # Producer to weather2
    producer = KafkaProducer(
        bootstrap_servers=['kafka1:9092','kafka2:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for i, message in enumerate(consumer):
        if i > 5:   
            break
        data = message.value
        
        if isinstance(data, list):
            records_to_process = data
        else:
            records_to_process = [data]

        for record in records_to_process:
            try:
                transformed = {
                    "city": record["name"],
                    "country": record["sys"]["country"],
                    "timestamp": datetime.fromtimestamp(record["dt"], tz=timezone.utc).isoformat(),
                    "feels_like_celsius": round(record["main"]["feels_like"], 2),
                    "temp_min_celsius": round(record["main"]["temp_min"], 2),
                    "temp_max_celsius": round(record["main"]["temp_max"], 2),
                    "humidity": record["main"]["humidity"],
                    "pressure_hpa": record["main"]["pressure"],
                    "wind_speed_ms": record["wind"]["speed"],
                    "wind_deg": record["wind"].get("deg"),
                    "weather_main": record["weather"][0]["main"],
                    "weather_description": record["weather"][0]["description"],
                    "clouds_percent": record["clouds"]["all"],
                    "visibility_meters": record.get("visibility"),
                    "sunrise": datetime.fromtimestamp(record["sys"]["sunrise"], tz=timezone.utc).isoformat(),
                    "sunset": datetime.fromtimestamp(record["sys"]["sunset"], tz=timezone.utc).isoformat()
                }
                
                # Send transformed data to weather2
                producer.send('transformed_topic', value=transformed)
                print(f"Sent to transformed_topic: {transformed['city']} at {transformed['timestamp']}")
                
            except KeyError as e:
                print(f"Skipping record - missing key {e}: {record}")
            except Exception as e:
                print(f"Error processing record: {e}")

    # Close resources (though loop runs forever; add flush on interrupt)
    producer.flush()
    producer.close()
    consumer.close()

if __name__ == "__main__":
    transform_and_stream()