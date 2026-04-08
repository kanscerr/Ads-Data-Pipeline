import json
import random
import uuid 
import time 
from datetime import datetime
import pytz #for timezone awareness

from kafka import KafkaProducer #to send data off to kafka topic
from faker import Faker #for generating the fake data
from config import * #the config.py file we created

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8") #since kafka expects bytes
)

def generate_metrics():
    impressions = random.randint(100, 10000)
    clicks = random.randint(0, impressions)
    unique_clickes = random.randint(0, clicks)
    conversions = random.randint(0, clicks)
    spend = round(random.uniform(10, 500), 2)
    revenue = round(conversions * random.uniform(5, 50), 2)
    ctr = clicks / impressions if impressions else 0
    cpc = spend / clicks if clicks else 0
    cpm = (spend / impressions * 100) if impressions else 0
    cpa = spend / conversions if conversions else 0
    roas = revenue / spend if spend else 0

    return{
        "impressions" : impressions,
        "clicks" : clicks,
        "unique_clicks" : unique_clickes,
        "conversions": conversions,
        "spend": spend,
        "revenue": revenue,
        "ctr" : ctr,
        "cpc": cpc,
        "cpm": cpm,
        "cpa": cpa,
        "roas": roas
    }

def generate_event():
    now = datetime.now(pytz.utc)
    metrics = generate_metrics()
    event = {
        "event_name": "ads_performance",
        "timestamp": now.isoformat(),
        "date": now.strftime("%Y-%m-%d"),
        "hour": now.hour,
        "platform": random.choice(PLATFORMS),
        "account_id": f"acc_{random.randint(1,10)}",
        "account_name": fake.company(),
        "campaign_id": f"camp_{random.randint(1,50)}",
        "campaign_name": fake.catch_phrase(),
        "campaign_objectives": random.choice(["awareness", "traffic", "conversions"]),
        "adset_id": f"adset_{random.randint(1,100)}",
        "adset_name": fake.word(),
        "optimization_goal": random.choice(["clicks", "comversions"]),
        "ad_id": f"ad_{random.randint(1,500)}",
        "ad_name": fake.word(),
        "creative_type": random.choice(["image", "video", "carousel"]),
        "placement": random.choice(["feed", "stories", "search"]),
        "network": random.choice(["facebook", "instagram", "google_display"]),
        "country": fake.country(),
        "region": fake.state(),
        "city": fake.city(),
        "device_type": random.choice(DEVICES),
        "os": random.choice(OS_LIST),
        "browser": random.choice(BROWSERS),
        "age_group": random.choice(AGE_GROUPS),
        "gender": random.choice(GENDERS),
        "audience_segment": random.choice(["retargeting", "lookalike", "cold"]),
        "language": random.choice(["en", "hi"]),
        "timezone": "UTC",
        "product_id": f"prod_{random.randint(1,1000)}",
        "product_category": random.choice(["fashion", "electronics", "beauty"]),
        "currency": "USD", 
        # Metric
        **metrics,
        "reach": int(metrics["impressions"] * random.uniform(0.6, 0.9)),
        "frequency": round(random.uniform(1, 3), 2),
        "video_views": random.randint(0, metrics["impressions"]),
        "video_25_percent_views": random.randint(0, metrics["impressions"]),
        "video_50_percent_views": random.randint(0, metrics["impressions"]),
        "video_75_percent_views": random.randint(0, metrics["impressions"]),
        "video_100_percent_views": random.randint(0, metrics["impressions"]),
        "engagements": random.randint(0, metrics["impressions"]),
        "likes": random.randint(0, 1000),
        "shares": random.randint(0, 500),
        "comments": random.randint(0, 300),
        "add_to_cart": random.randint(0, metrics["clicks"]),
        "checkout_started": random.randint(0, metrics["clicks"]),
        "conversion_type": random.choice(["purchase", "signup"]),
        "conversion_value": metrics["revenue"],
        "conversion_rate": metrics["conversions"] / metrics["clicks"] if metrics["clicks"] else 0,
        "installs": random.randint(0, 200),
        "signups": random.randint(0, 200),
        "purchases": metrics["conversions"],
        "attribution_window": "7d_click",
        "quality_score": round(random.uniform(1, 10), 2),
        "relevance_score": round(random.uniform(1, 10), 2),
    }
    return event

def main():
    print("Streaming ads performance events...")
    while True:
        event = generate_event()
        producer.send(TOPIC, value=event)
        print(event)
        time.sleep(1)

if __name__ == "__main__":
    main()