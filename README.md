# Kafka Streaming Demo – Prescriber Events

This project demonstrates a simple, end-to-end Kafka streaming pipeline using Docker, Python, and SQLite.
It models a CDC-style event stream where prescriber records are upserted or deleted and materialized into a “silver” table.

## Architecture

Producer (manual / Python)
→ Kafka Topic (`prescriber_events`)
→ Python Consumer (idempotent upsert logic)
→ SQLite “Silver” Table

## Components

- Kafka + Zookeeper (Docker Compose)
- Python producer (`producer.py`)
- Python consumer with idempotent upsert logic (`consumer_to_sqlite.py`)
- SQLite silver table (`silver_prescribers.db`)
- Query utility (`query_silver.py`)

## Event Schema

Events are JSON messages published to Kafka:

{
  "event_type": "upsert | delete",
  "npi": "string",
  "source": "cms_partd | python_demo",
  "ts": "epoch seconds OR ISO-8601 UTC string"
}

## Run Instructions

1. Start Kafka

docker compose up -d

Verify containers are running:

docker compose ps

2. Produce Events

py producer.py

3. Start the Consumer

py consumer_to_sqlite.py

The consumer:
- Reads events from Kafka
- Normalizes mixed timestamp formats
- Applies idempotent upsert / delete logic
- Persists the latest state per NPI into SQLite

4. Query the Silver Table

py query_silver.py

Example output:

NPI        | event  | source      | last_ts_utc           | updated_at_utc
--------------------------------------------------------------------------
1234567890 | upsert | cms_partd   | 2026-01-21 18:55:00Z | 2026-01-21T19:21:28Z

## Data Model (Silver Table)

prescribers_silver (
  npi TEXT PRIMARY KEY,
  last_event_type TEXT,
  source TEXT,
  last_ts REAL,
  updated_at_utc TEXT
)

- One row per NPI
- Always reflects the most recent event
- Delete events are modeled as soft deletes

## Design Notes

- Uses Kafka consumer groups for replayability
- Idempotent upsert pattern supports safe reprocessing
- CDC-style event modeling without Spark
- Designed to be easily extended to Postgres, Parquet, or Databricks

## Possible Extensions

- Replace SQLite with Postgres
- Add a dead-letter queue topic
- Persist silver output as Parquet
- Integrate with Databricks Bronze → Silver
- Add schema validation (Avro or JSON Schema)

