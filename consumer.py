import json
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-data")
GROUP_ID = os.getenv("KAFKA_GROUP", "stock-group")

# InfluxDB configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "m4sU-xYCqZDf0xxAvKJdsIqOhmTUV3ISnd8OrvGS3g38reJaF_JSEdRE8cDkWQEZ65LTVxJw_wJ9D3S9qSQ_2A==")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "sjsu")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "bigdata")

# Create InfluxDB Client
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",  # Start from earliest if no offsets exist
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print(f"Listening to Kafka topic '{KAFKA_TOPIC}'...")

# Consume and write to InfluxDB
for message in consumer:
    try:
        # Deserialize message
        stock_data = message.value
        print(f"Received data: {stock_data}")

        # Prepare InfluxDB Point
        point = Point("stock_prices") \
            .tag("symbol", stock_data["stock"]) \
            .field("open", float(stock_data["open"]) if stock_data["open"] is not None else 0.0) \
            .field("high", float(stock_data["high"]) if stock_data["high"] is not None else 0.0) \
            .field("low", float(stock_data["low"]) if stock_data["low"] is not None else 0.0) \
            .field("close", float(stock_data["close"]) if stock_data["close"] is not None else 0.0) \
            .field("volume", int(stock_data["volume"]) if stock_data["volume"] is not None else 0) \
            .time(stock_data["date"])

        # Write to InfluxDB
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        print(f"Data written to InfluxDB for {stock_data['stock']} at {stock_data['date']}")

    except Exception as e:
        print(f"Error processing message: {e}")

