# Weather Pipeline

An end-to-end weather ETL pipeline orchestrated with Airflow, streamed through Kafka, and stored in PostgreSQL.

## What This Project Does
- Extracts current weather data from OpenWeather for a fixed city set: `Yangon`, `Pathein`, `Naypyidaw`, `Mandalay`.
- Publishes raw API responses to Kafka topic `raw_topic`.
- Validates raw stream quality (city coverage, required fields, duplicates).
- Transforms raw payloads into a clean schema and publishes to `transformed_topic`.
- Validates transformed stream (completeness, ranges, consistency, freshness).
- Loads transformed records into PostgreSQL table `weather`.
- Runs a final database verification step.

## Tech Stack
- Apache Airflow `2.8.1` (LocalExecutor)
- Apache Kafka (2 brokers + Zookeeper, via Confluent images)
- PostgreSQL `15`
- Python libs: `kafka-python`, `psycopg2-binary`, `requests`
- Docker + Docker Compose

## Project Structure
```text
.
├── dags/
│   └── dag.py                      # Airflow DAG definition
├── pipeline/
│   ├── config.py                   # DB config, city list
│   ├── extract.py                  # OpenWeather -> raw_topic
│   ├── transform.py                # raw_topic -> transformed_topic
│   ├── load.py                     # transformed_topic -> PostgreSQL
│   └── table_create.py             # CREATE TABLE weather
├── utilis/
│   ├── broker_check.py             # Kafka health check
│   ├── raw_topic_test.py           # Raw data quality checks
│   ├── transformed_topic_test.py   # Transformed data quality checks
│   └── db_loaded_check.py          # DB verification task
├── init/
│   └── create_topic.sh             # Kafka topic bootstrap
├── Dockerfile
└── docker-compose.yml
```

## Data Flow (DAG)
Airflow DAG ID: `kafka_health`  
Schedule: `@hourly`

Task order:
1. `run_kafka_check`
2. `table_check`
3. `extract_task`
4. `raw_topic_test`
5. `transform_task`
6. `transformed_topic_test`
7. `load_task`
8. `db_check_task`

## Prerequisites
- Docker
- Docker Compose
- OpenWeather API key(put API key into the variable of the Apache airflow)

## Quick Start
1. Start all services:
```bash
docker compose up -d --build
```

2. Open Airflow UI:
- URL: `http://localhost:8080`
- Username: `admin`
- Password: `admin`

3. Set Airflow variable for API key:
- Go to **Admin -> Variables**
- Add:
  - Key: `openweather_api_key`
  - Value: `<your_openweather_api_key>`

4. Enable and trigger DAG:
- DAG name: `kafka_health`
- Toggle it on and click **Trigger DAG** (or wait for hourly schedule).

## PostgreSQL Access
- Host: `localhost`
- Port: `5432`
- Database: `weather`
- User: `postgres`
- Password: `postgres`

Example:
```bash
psql -h localhost -p 5432 -U postgres -d weather
```

## Kafka Topics
- `raw_topic`: unmodified OpenWeather API payloads
- `transformed_topic`: normalized weather records ready for loading

Topics are created automatically by `init/create_topic.sh` at startup.

## Weather Table Schema
Table: `weather`

Key columns:
- `city`, `country`, `timestamp`
- `feels_like_celsius`, `temp_min_celsius`, `temp_max_celsius`
- `humidity`, `pressure_hpa`
- `wind_speed_ms`, `wind_deg`
- `weather_main`, `weather_description`
- `clouds_percent`, `visibility_meters`
- `sunrise`, `sunset`

Uniqueness:
- `UNIQUE(city, timestamp)` to avoid duplicate inserts.

## Useful Commands
View running containers:
```bash
docker compose ps
```

Follow logs:
```bash
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver
docker compose logs -f kafka1
```

Stop services:
```bash
docker compose down
```

Stop and remove DB volume (fresh reset):
```bash
docker compose down -v
```

## Troubleshooting
- DAG fails at extract step:
  - Ensure Airflow variable `openweather_api_key` is set correctly.
- Kafka health/topic errors:
  - Check `kafka1`, `kafka2`, `zookeeper`, and `init-topics` container logs.
- No rows loaded:
  - Confirm transformed records exist in `transformed_topic`.
  - Verify PostgreSQL is reachable using credentials above.
- `db_check_task` query error:
  - Current check script references `temperature`, but table uses `temp_*` columns.
  - Update `utilis/db_loaded_check.py` if that task fails on sample query.

## Notes
- The pipeline uses an internal Docker network host `postgres` for app-to-DB access inside containers.
- If running scripts outside Docker, DB host may need to be changed to `localhost`.
