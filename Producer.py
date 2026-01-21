import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:19092",   # host-mapped port
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

events = [
    {"event_type":"upsert","npi":"1111111111","source":"python_demo","ts":time.time()},
    {"event_type":"upsert","npi":"2222222222","source":"python_demo","ts":time.time()},
    {"event_type":"delete","npi":"3333333333","source":"python_demo","ts":time.time()},
]

for e in events:
    producer.send("prescriber_events", e)
    producer.flush()
    print("sent:", e)
    time.sleep(0.5)

producer.close()
print("done")
