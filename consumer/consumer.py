from kafka import KafkaConsumer
import json
import logging
import psycopg2

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# --------------------------------------------------
# PostgreSQL Connection
# --------------------------------------------------
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="adsdb",
    user="admin",
    password="admin123"
)

cursor = conn.cursor()

# --------------------------------------------------
# Kafka Consumer
# --------------------------------------------------
consumer = KafkaConsumer(
    "ads_performance",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="ads-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

logger.info("Consumer started...")

# --------------------------------------------------
# Dedup Memory Cache
# --------------------------------------------------
seen_events = set()

# --------------------------------------------------
# Clean Event
# --------------------------------------------------
def clean_event(event):
    try:
        required_fields = [
            "timestamp",
            "event_name",
            "platform",
            "account_id",
            "campaign_id",
            "adset_id",
            "ad_id",
            "impressions",
            "clicks",
            "spend",
            "revenue"
        ]

        if not all(field in event for field in required_fields):
            raise ValueError("Missing required fields")

        return event

    except Exception as e:
        logger.error(f"Bad Event: {e}")
        return None

# --------------------------------------------------
# Enrich Event
# --------------------------------------------------
def enrich_event(event):
    impressions = int(event["impressions"])
    clicks = int(event["clicks"])
    conversions = int(event.get("conversions", 0))
    spend = float(event["spend"])
    revenue = float(event["revenue"])

    event["event_id"] = f'{event["ad_id"]}_{event["timestamp"]}'
    event["event_timestamp"] = event["timestamp"]
    event["event_date"] = event["date"]
    event["event_hour"] = int(event["hour"])

    event["ctr"] = round(clicks / impressions, 4) if impressions else 0
    event["cpc"] = round(spend / clicks, 4) if clicks else 0
    event["cpm"] = round((spend / impressions) * 1000, 4) if impressions else 0
    event["cpa"] = round(spend / conversions, 4) if conversions else 0
    event["roas"] = round(revenue / spend, 4) if spend else 0

    return event

# --------------------------------------------------
# Insert Query
# --------------------------------------------------
insert_query = """
INSERT INTO ad_events (
    event_id,
    event_name,
    event_timestamp,
    event_date,
    event_hour,
    platform,
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    campaign_objective,
    adset_id,
    adset_name,
    optimization_goal,
    ad_id,
    ad_name,
    creative_type,
    placement,
    network,
    country,
    region,
    city,
    device_type,
    os,
    browser,
    age_group,
    gender,
    audience_segment,
    language,
    timezone,
    product_id,
    product_category,
    currency,
    impressions,
    clicks,
    unique_clicks,
    conversions,
    spend,
    revenue,
    ctr,
    cpc,
    cpm,
    cpa,
    roas,
    reach,
    frequency,
    video_views,
    video_25_percent_views,
    video_50_percent_views,
    video_75_percent_views,
    video_100_percent_views,
    engagements,
    likes,
    shares,
    comments,
    add_to_cart,
    checkout_started,
    conversion_type,
    conversion_value,
    conversion_rate,
    installs,
    signups,
    purchases,
    attribution_window,
    quality_score,
    relevance_score
)
VALUES (
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s
)
ON CONFLICT (event_id)
DO UPDATE SET
clicks = EXCLUDED.clicks,
impressions = EXCLUDED.impressions,
conversions = EXCLUDED.conversions,
spend = EXCLUDED.spend,
revenue = EXCLUDED.revenue,
ctr = EXCLUDED.ctr,
cpc = EXCLUDED.cpc,
cpm = EXCLUDED.cpm,
cpa = EXCLUDED.cpa,
roas = EXCLUDED.roas,
processed_at = CURRENT_TIMESTAMP;
"""

# --------------------------------------------------
# Main Consumer Loop
# --------------------------------------------------
for message in consumer:
    raw_event = message.value

    cleaned = clean_event(raw_event)
    if not cleaned:
        continue

    dedup_key = (cleaned["ad_id"], cleaned["timestamp"])

    if dedup_key in seen_events:
        logger.warning(f"Duplicate skipped: {dedup_key}")
        continue

    seen_events.add(dedup_key)

    enriched = enrich_event(cleaned)

    try:
        cursor.execute(insert_query, (
            enriched["event_id"],
            enriched["event_name"],
            enriched["event_timestamp"],
            enriched["event_date"],
            enriched["event_hour"],
            enriched["platform"],
            enriched["account_id"],
            enriched["account_name"],
            enriched["campaign_id"],
            enriched["campaign_name"],
            enriched["campaign_objectives"],
            enriched["adset_id"],
            enriched["adset_name"],
            enriched["optimization_goal"],
            enriched["ad_id"],
            enriched["ad_name"],
            enriched["creative_type"],
            enriched["placement"],
            enriched["network"],
            enriched["country"],
            enriched["region"],
            enriched["city"],
            enriched["device_type"],
            enriched["os"],
            enriched["browser"],
            enriched["age_group"],
            enriched["gender"],
            enriched["audience_segment"],
            enriched["language"],
            enriched["timezone"],
            enriched["product_id"],
            enriched["product_category"],
            enriched["currency"],
            enriched["impressions"],
            enriched["clicks"],
            enriched["unique_clicks"],
            enriched["conversions"],
            enriched["spend"],
            enriched["revenue"],
            enriched["ctr"],
            enriched["cpc"],
            enriched["cpm"],
            enriched["cpa"],
            enriched["roas"],
            enriched["reach"],
            enriched["frequency"],
            enriched["video_views"],
            enriched["video_25_percent_views"],
            enriched["video_50_percent_views"],
            enriched["video_75_percent_views"],
            enriched["video_100_percent_views"],
            enriched["engagements"],
            enriched["likes"],
            enriched["shares"],
            enriched["comments"],
            enriched["add_to_cart"],
            enriched["checkout_started"],
            enriched["conversion_type"],
            enriched["conversion_value"],
            enriched["conversion_rate"],
            enriched["installs"],
            enriched["signups"],
            enriched["purchases"],
            enriched["attribution_window"],
            enriched["quality_score"],
            enriched["relevance_score"]
        ))

        conn.commit()
        logger.info("Inserted into PostgreSQL")

    except Exception as e:
        conn.rollback()
        logger.error(f"DB Insert Failed: {e}")

    # Save backup JSON
    with open("output/output.json", "a") as f:
        f.write(json.dumps(enriched) + "\n")

    logger.info(f"Saved Event: {enriched['event_id']}")