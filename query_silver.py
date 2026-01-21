 # query_silver.py
import sqlite3
from datetime import datetime, timezone

def to_utc_str(epoch_seconds: float) -> str:
    try:
        return datetime.fromtimestamp(float(epoch_seconds), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    except Exception:
        return "n/a"

conn = sqlite3.connect("silver_prescribers.db")
cur = conn.cursor()

cur.execute("""
select
  npi,
  last_event_type,
  source,
  last_ts,
  updated_at_utc
from prescribers_silver
order by npi
""")

rows = cur.fetchall()

print("NPI        | event  | source      | last_ts_utc           | updated_at_utc")
print("-"*78)
for npi, event_type, source, last_ts, updated_at_utc in rows:
    print(f"{npi:<10} | {event_type:<6} | {source:<10} | {to_utc_str(last_ts):<20} | {updated_at_utc}")

conn.close()
