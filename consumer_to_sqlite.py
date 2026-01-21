# consumer_to_sqlite.py
import json
import sqlite3
import time
from kafka import KafkaConsumer

DB_PATH = "silver_prescribers.db"
TOPIC = "prescriber_events"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS prescribers_silver (
            npi TEXT PRIMARY KEY,
            last_event_type TEXT,
            source TEXT,
            last_ts REAL,
            updated_at_utc TEXT
        )
        """
    )
    conn.commit()
    return conn


def normalize_ts(event: dict) -> float:
    """
    Accepts:
      - epoch seconds (int/float)
      - ISO-8601 Zulu string: "YYYY-MM-DDTHH:MM:SSZ"
    Returns epoch seconds (float).
    """
    raw_ts = event.get("ts")

    if raw_ts is None:
        return time.time()

    if isinstance(raw_ts, (int, float)):
        return float(raw_ts)

    if isinstance(raw_ts, str):
        # Try ISO-8601 like "2026-01-21T12:55:00Z"
        try:
            # Parse as UTC
            tm = time.strptime(raw_ts, "%Y-%m-%dT%H:%M:%SZ")
            # Convert struct_time (UTC) -> epoch seconds
            return float(time.mktime(tm))
        except ValueError:
            return time.time()

    return time.time()


def upsert(conn, event: dict):
    npi = event.get("npi")
    if not npi:
        return

    last_event_type = event.get("event_type", "unknown")
    source = event.get("source", "unknown")
    last_ts = normalize_ts(event)
    updated_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO prescribers_silver (npi, last_event_type, source, last_ts, updated_at_utc)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(npi) DO UPDATE SET
          last_event_type=excluded.last_event_type,
          source=excluded.source,
          last_ts=excluded.last_ts,
          updated_at_utc=excluded.updated_at_utc
        """,
        (npi, last_event_type, source, last_ts, updated_at_utc),
    )
    conn.commit()


def main():
    conn = init_db()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:19092",  # host-mapped port
        auto_offset_reset="earliest",         # start from beginning for demo
        enable_auto_commit=True,
        group_id="silver_writer_v1",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    print("Listening... Ctrl+C to stop.")
    for msg in consumer:
        event = msg.value
        upsert(conn, event)
        print("processed:", event)


if __name__ == "__main__":
    main()
