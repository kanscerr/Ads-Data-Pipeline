from kafka import KafkaConsumer
import json

# data cleanup
def clean_event(event):
    try:
        return {
            "ad_id": int(event["ad_id"]),
            "clicks": int(event["clicks"]),
            "impressions": int(event["impressions"]),
            "timestamp": event["timestamp"]
        }
    except Exception as e:
        print("Bad event: ", event, e)
        return None
    
# data enrichment
def enrich_event(event):
    if event["impressions"] == 0:
        ctr = 0
    else:
        ctr = event["clicks"] / event["impressions"]
    event["ctr"] = round(ctr, 4)
    return event

# consumer setup
consumer = KafkaConsumer(
    'ads_performance',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
print("Consumer started...")

for message in consumer:
    raw_event = message.value
    cleaned = clean_event(raw_event)
    if not cleaned:
        continue
    enriched = enrich_event(cleaned)
    print("Final Event:", enriched)