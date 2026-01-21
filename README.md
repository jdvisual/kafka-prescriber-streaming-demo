# Kafka Streaming Demo – Prescriber Events

This project demonstrates a simple, end-to-end Kafka streaming pipeline using Docker, Python, and SQLite.  
It models a CDC-style event stream where prescriber records are upserted or deleted and materialized into a “silver” table.

## Architecture

Producer (manual / Python)  
→ Kafka Topic (`prescriber_events`)  
→ Python Consumer (upsert logic)  
→ SQLite “Silver” Table

## Components

- Kafka + Zookeeper (Docker Compose)
- Python producer (`producer.py`)
- Python consumer with idempotent upsert logic (`consumer_to_sqlite.py`)
- SQLite silver table (`silver_prescribers.db`)
- Query utility (`query_silver.py`)

## Event Schema

Events are JSON messages published to Kafka:

```json
{
  "event_type": "upsert | delete",
  "npi": "string",
  "source": "cms_partd | python_demo",
  "ts": "epoch seconds OR ISO-8601 UTC string"
}
## Run Instructions

### 1. Start Kafka

```bash
docker compose up - 

py producer.py

py consumer_to_sqlite.py


in a seperate terminal L

py query_silver.py
