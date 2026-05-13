from kafka import KafkaConsumer, TopicPartition
from datetime import datetime,timezone
import json

CITIES = ["Yangon","Pathein","Naypyidaw","Mandalay"]

def last_n_records(n=10):
    # Consumer reading from earliest
    consumer = KafkaConsumer(
        bootstrap_servers=['kafka1:9092','kafka2:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    topic = 'raw_topic'
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        print(f"No partitions found for topic '{topic}'")
        consumer.close()
        return []
    
    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(topic_partitions)
    consumer.seek_to_beginning()
    
    all_records = []  # store (dt, record)
    
    while True:
        records = consumer.poll(timeout_ms=1000)
        if not records:
            break
        for tp, messages in records.items():
            for msg in messages:
                data = msg.value
                records_list = data if isinstance(data, list) else [data]
                for rec in records_list:
                    dt = rec.get("dt")
                    if dt is not None:
                        all_records.append((dt, rec))
    
    consumer.close()
    
    if not all_records:
        print("No records with 'dt' field found.")
        return []
    
    # Sort by dt descending and take first n
    all_records.sort(key=lambda x: x[0], reverse=True)
    latest_n = [rec for dt, rec in all_records[:n]]
  
    return latest_n

def city_test(cities):
    try:
        last_5 = last_n_records(5)
        cities_to_remove = set()

        for record in last_5:
            name = record.get("name")
            if name:
                cities_to_remove.add(name)
        if cities_to_remove == set(cities):
            print("All cities are successfuly fetched")
        else:
            print(f"Remaining cities: {cities}")
            raise Exception("Not all cities were fetched successfully")
        return cities
    except Exception as e:
        print(f"city_test failed: {e}")
        raise

def completeness_test():
    try:
        last_5 = last_n_records(5)
        if not last_5:
            print("No records retrieved.")
            return

        required_fields = [
            "name",
            "sys.country",
            "dt",
            "main.feels_like",
            "main.temp_min",
            "main.temp_max",
            "main.humidity",
            "main.pressure",
            "wind.speed",
            "weather[0].main",
            "weather[0].description",
            "clouds.all",
            "sys.sunrise",
            "sys.sunset"
        ]

        optional_fields = ["wind.deg", "visibility"]

        for i, rec in enumerate(last_5, 1):
            city = rec.get("name", "Unknown")
            missing = []

            def check_path(obj, path):
                parts = path.split('.')
                for part in parts:
                    if '[' in part and ']' in part:
                        idx_start = part.find('[')
                        idx_end = part.find(']')
                        key = part[:idx_start]
                        idx = int(part[idx_start+1:idx_end])
                        if key in obj and isinstance(obj[key], list) and len(obj[key]) > idx:
                            obj = obj[key][idx]
                        else:
                            return False
                    else:
                        if part in obj:
                            obj = obj[part]
                        else:
                            return False
                return True

            for path in required_fields:
                if not check_path(rec, path):
                    missing.append(path)

            if missing:
                print(f"Record {i} ({city}) is INCOMPLETE. Missing: {missing}")
            else:
                print(f"Record {i} ({city}) is COMPLETE (all required fields present).")

            missing_opt = []
            for path in optional_fields:
                if not check_path(rec, path):
                    missing_opt.append(path)
            if missing_opt:
                print(f"  Optional fields missing: {missing_opt}")
    except Exception as e:
        print(f"completeness_test failed: {e}")
        raise

def duplicate_test():
    try:
        last_5 = last_n_records(5)
        seen = set()
        duplicates = []

        for rec in last_5:
            key = (rec.get("name"), rec.get("dt"))
            if key in seen:
                duplicates.append(key)
            else:
                seen.add(key)

        if duplicates:
            raise ValueError(f"Duplicate records found: {duplicates}")
        else:
            print("All good – no duplicate (name, dt) city pairs in the last 5 records.")
    except Exception as e:
        print(f"duplicate_test failed: {e}")
        raise

def run_all_tests():
    tests = [
        ("city_test", lambda: city_test(CITIES)),
        ("completeness_test", completeness_test),
        ("duplicate_test", duplicate_test),
    ]

    for test_name, test_fn in tests:
        try:
            test_fn()
            print(f"{test_name}: PASSED")
        except Exception as e:
            print(f"{test_name}: FAILED - {e}")

if __name__ == "__main__":
    tests = [
        ("city_test", lambda: city_test(CITIES)),
        ("completeness_test", completeness_test),
        ("duplicate_test", duplicate_test),
    ]

    for test_name, test_fn in tests:
        try:
            test_fn()
            print(f"{test_name}: PASSED")
        except Exception as e:
            print(f"{test_name}: FAILED - {e}")