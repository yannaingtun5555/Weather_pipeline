import sys
import argparse
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError

def check_kafka_health(bootstrap_servers='kafka1:9092,kafka2:9092', topic_name=None):
    """
    Check Kafka broker health.
    
    Args:
        bootstrap_servers (str): Comma-separated broker addresses.
        topic_name (str, optional): Topic to verify existence.
    
    Returns:
        bool: True if healthy, False otherwise.
    """
    print(f"\n[INFO] Connecting to Kafka broker(s): {bootstrap_servers}")

    # Create a consumer (without subscribing) just to fetch metadata
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            client_id='health_check',
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000
        )
        # Fetch all topic names – this forces a metadata request
        topics = consumer.topics()
        print(f"[PASS] Successfully connected to Kafka cluster.")
        print(f"[INFO] Found {len(topics)} topic(s) in the cluster.")
        consumer.close()
    except NoBrokersAvailable:
        print("[FAIL] No Kafka brokers available. Is Kafka running?")
        return False
    except KafkaError as e:
        print(f"[FAIL] Kafka error: {e}")
        return False
    except Exception as e:
        print(f"[FAIL] Unexpected error: {e}")
        return False

    # Optional topic existence check
    if topic_name:
        print(f"\n[INFO] Checking existence of topic: '{topic_name}'")
        try:
            consumer2 = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                client_id='topic_check',
                request_timeout_ms=5000
            )
            partitions = consumer2.partitions_for_topic(topic_name)
            if partitions:
                print(f"[PASS] Topic '{topic_name}' exists with {len(partitions)} partition(s).")
            else:
                print(f"[FAIL] Topic '{topic_name}' does NOT exist.")
                consumer2.close()
                return False
            consumer2.close()
        except KafkaError as e:
            print(f"[FAIL] Error checking topic: {e}")
            return False
        except Exception as e:
            print(f"[FAIL] Unexpected error during topic check: {e}")
            return False

    print("\n[RESULT] Kafka health check completed successfully.\n")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check Kafka broker health")
    parser.add_argument(
        "--servers",
        type=str,
        default="kafka1:9092,kafka2:9092",
        help="Kafka broker(s) (e.g., 'localhost:9092' or 'broker1:9092,broker2:9092')"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=None,
        help="Optional: topic name to verify existence"
    )
    args = parser.parse_args()

    healthy = check_kafka_health(bootstrap_servers=args.servers, topic_name=args.topic)
    sys.exit(0 if healthy else 1)