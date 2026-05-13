# check_weather2.py
from kafka import KafkaConsumer, TopicPartition
import json
from datetime import datetime, timezone, timedelta

def last_n_transformed_records(n=5, topic='transformed_topic'):

    consumer = KafkaConsumer(
        bootstrap_servers=['kafka1:9092','kafka2:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        consumer.close()
        return []

    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(topic_partitions)
    consumer.seek_to_beginning()

    all_records = [] 
    while True:
        records = consumer.poll(timeout_ms=1000)
        if not records:
            break
        for tp, messages in records.items():
            for msg in messages:
                data = msg.value
              
                ts = data.get("timestamp")
                if ts:
                    all_records.append((ts, data))

    consumer.close()

    if not all_records:
        return []

    all_records.sort(key=lambda x: x[0], reverse=True)
    return [rec for ts, rec in all_records[:n]]

def completeness_test_transformed(records):
    required_fields = [
        "city", "country", "timestamp",
        "feels_like_celsius", "temp_min_celsius", "temp_max_celsius",
        "humidity", "pressure_hpa", "wind_speed_ms",
        "weather_main", "weather_description", "clouds_percent",
        "sunrise", "sunset"
    ]
    optional_fields = ["wind_deg", "visibility_meters"]

    for i, rec in enumerate(records, 1):
        missing = [f for f in required_fields if f not in rec]
        if missing:
            raise ValueError(f"Record {i} (city={rec.get('city')}) missing fields: {missing}")
        missing_opt = [f for f in optional_fields if f not in rec]
        if missing_opt:
            print(f"Warning: record {i} missing optional fields: {missing_opt}")
    print("Completeness test PASSED.")


def range_test_transformed(records):
    for i, rec in enumerate(records, 1):
        city = rec.get("city")

        # Temperatures (Celsius)
        feels = rec.get("feels_like_celsius")
        if feels is not None and not (-50 < feels < 60):
            raise ValueError(f"{city}: feels_like={feels} out of range (-50..60)")

        tmin = rec.get("temp_min_celsius")
        tmax = rec.get("temp_max_celsius")
        if tmin is not None and not (-50 < tmin < 60):
            raise ValueError(f"{city}: temp_min={tmin} out of range")
        if tmax is not None and not (-50 < tmax < 60):
            raise ValueError(f"{city}: temp_max={tmax} out of range")

        # Humidity (%)
        hum = rec.get("humidity")
        if hum is not None and not (0 <= hum <= 100):
            raise ValueError(f"{city}: humidity={hum} not in 0..100")

        # Pressure (hPa)
        press = rec.get("pressure_hpa")
        if press is not None and not (800 <= press <= 1100):
            raise ValueError(f"{city}: pressure={press} out of range (800..1100)")

        # Wind speed (m/s)
        ws = rec.get("wind_speed_ms")
        if ws is not None and not (0 <= ws <= 150):
            raise ValueError(f"{city}: wind_speed={ws} unrealistic (>150 m/s)")

    print("Range test PASSED.")


def logical_consistency_test(records):
    for i, rec in enumerate(records, 1):
        city = rec.get("city")
        tmin = rec.get("temp_min_celsius")
        tmax = rec.get("temp_max_celsius")
        if tmin is not None and tmax is not None and tmin > tmax:
            raise ValueError(f"{city}: temp_min ({tmin}) > temp_max ({tmax})")

        ts_str = rec.get("timestamp")
        if ts_str:
            try:
                datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            except Exception as e:
                raise ValueError(f"{city}: invalid timestamp format '{ts_str}' - {e}")

        feels = rec.get("feels_like_celsius")
        if tmin is not None and tmax is not None and feels is not None:
            avg = (tmin + tmax) / 2
            if abs(feels - avg) > 15:
                print(f"Warning: {city} feels_like ({feels}) far from avg temp ({avg})")

    print("Logical consistency test PASSED.")


def freshness_test_transformed(records, max_age_hours=2):
    """Check that transformed timestamps are not older than `max_age_hours`."""
    now = datetime.now(timezone.utc)
    for rec in records:
        ts_str = rec.get("timestamp")
        if not ts_str:
            raise ValueError("Missing timestamp")
        dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        age = now - dt
        if age > timedelta(hours=max_age_hours):
            raise ValueError(f"Stale data: {rec['city']} timestamp {ts_str} is {age}")
    print(f"Freshness test PASSED (all records younger than {max_age_hours} hours).")

def check_transformed_stream():
    """Entry point: retrieve last 5 transformed records and run all checks."""
    records = last_n_transformed_records(5)
    if len(records) < 5:
        raise RuntimeError(f"Only {len(records)} records found in weather2, expected at least 5.")

    print(f"Testing {len(records)} records from weather2...")
    completeness_test_transformed(records)
    range_test_transformed(records)
    logical_consistency_test(records)
    freshness_test_transformed(records, max_age_hours=2)

    print("\nAll checks passed successfully for weather2 stream.")


if __name__ == "__main__":
    check_transformed_stream()