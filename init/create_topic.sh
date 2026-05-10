#!/bin/bash
echo "Waiting for Kafka..."

BOOTSTRAP_SERVER="kafka1:9092"
TOPICS=("raw_topic" "transformed_topic")

until kafka-topics --bootstrap-server $BOOTSTRAP_SERVER --list >/dev/null 2>&1; do
  echo "Kafka not ready yet... sleeping"
  sleep 3
done

echo "Kafka is ready ✔"

for TOPIC in "${TOPICS[@]}"; do
  echo "Checking topic: $TOPIC"

  kafka-topics --bootstrap-server $BOOTSTRAP_SERVER --list | grep -w "$TOPIC" >/dev/null

  if [ $? -eq 0 ]; then
    echo "Topic $TOPIC already exists ✔"
  else
    echo "Creating topic $TOPIC..."
    kafka-topics --bootstrap-server $BOOTSTRAP_SERVER \
      --create --topic "$TOPIC" \
      --partitions 1 \
      --replication-factor 1

    echo "Created $TOPIC ✔"
  fi
done

echo "All topics checked/created ✔"