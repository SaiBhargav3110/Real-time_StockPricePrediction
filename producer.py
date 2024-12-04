import yfinance as yf
import pandas as pd
import numpy as np
import json
from kafka import KafkaProducer
from datetime import datetime, timedelta, time, timezone
import time as t

# Kafka Configurations
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "stock-data"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Utility Function to Send Data to Kafka
def send_to_kafka(producer, topic, key, message):
    try:
        producer.send(topic, key=key.encode("utf-8"), value=message)
        producer.flush()
        print(f"Sent to Kafka | Key: {key}, Message: {message}")
    except Exception as e:
        print(f"Error sending to Kafka: {e}")

# Retrieve Historical Data
def retrieve_historical_data(producer, stock_symbol, kafka_topic):
    stock_symbols = stock_symbol.split(",") if stock_symbol else []
    if not stock_symbols:
        print("No stock symbols provided.")
        return

    # Define the date range for historical data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)  # Last 3 months
    print(end_date)

    for symbol in stock_symbols:
        try:
            # Fetch historical data
            historical_data = yf.download(symbol, start=start_date.strftime('%Y-%m-%d'), 
                                           end=end_date.strftime('%Y-%m-%d'), 
                                           interval="1d")
            

            print(historical_data.head())
            # Ensure the data has the correct format
            if isinstance(historical_data.columns, pd.MultiIndex):
                historical_data.columns = historical_data.columns.get_level_values(0)  # Flatten multi-level columns

            print(historical_data.head())
            # Handle empty or invalid data
            if historical_data.empty:
                print(f"No historical data available for {symbol}. Skipping.")
                continue

            # Replace zero-volume rows with NaN
            if "Volume" in historical_data.columns:
                historical_data.loc[historical_data["Volume"] == 0, ["Open", "High", "Low", "Close", "Adj Close", "Volume"]] = np.nan

            # Convert and send historical data to Kafka
            for index, row in historical_data.iterrows():
                data_point = {
                    'stock': symbol,
                    'date': index.isoformat(),
                    'open': row.get('Open', None),
                    'high': row.get('High', None),
                    'low': row.get('Low', None),
                    'close': row.get('Close', None),
                    'volume': row.get('Volume', None)
                }
                send_to_kafka(producer, kafka_topic, symbol, data_point)
                print(f"Sent historical data for {symbol}: {data_point}")

        except Exception as e:
            print(f"Error fetching historical data for {symbol}: {e}")

# Retrieve Real-Time Data
def retrieve_real_time_data(producer, stock_symbol, kafka_topic):
    retrieve_historical_data(producer, stock_symbol, kafka_topic)
    stock_symbols = stock_symbol.split(",") if stock_symbol else []
    if not stock_symbols:
        print("No stock symbols provided.")
        return

    while True:
        current_time = datetime.now()
        is_market_open = is_stock_market_open(current_time)

        if is_market_open:
            end_time = datetime.now()
            start_time = end_time - timedelta(minutes=2)  # Fetch last 2 minutes of data
            for symbol in stock_symbols:
                try:
                    # Fetch real-time data
                    real_time_data = yf.download(symbol, start=start_time, end=end_time, interval="1m", progress=False)
                    print(f"Real-time data for {symbol}: \n{real_time_data}")

                    # Ensure the data has the correct format
                    if isinstance(real_time_data.columns, pd.MultiIndex):
                        real_time_data.columns = real_time_data.columns.get_level_values(0)  # Flatten multi-level columns

                    # Handle empty or invalid data
                    if real_time_data.empty:
                        print(f"No real-time data available for {symbol}. Skipping.")
                        continue

                    

                    # Convert and send real-time data to Kafka
                    for index, row in real_time_data.iterrows():
                        real_time_data_point = {
                            'stock': symbol,
                            'date': index.isoformat(),
                            'open': row.get('Open', None),
                            'high': row.get('High', None),
                            'low': row.get('Low', None),
                            'close': row.get('Close', None),
                            'volume': int(row.get('Volume', None)) if not pd.isna(row.get('Volume', None)) else None
                        }
                        send_to_kafka(producer, kafka_topic, symbol, real_time_data_point)
                        print(f"Real-time data sent for {symbol}: {real_time_data_point}")
                except Exception as e:
                    print(f"Error fetching real-time data for {symbol}: {e}")
        else:
            # Send null data when the market is closed
            for symbol in stock_symbols:
                null_data_point = {
                    'stock': symbol,
                    'date': current_time.isoformat(),
                    'open': None,
                    'high': None,
                    'low': None,
                    'close': None,
                    'volume': None
                }
                send_to_kafka(producer, kafka_topic, symbol, null_data_point)
        t.sleep(60)  # Fetch real-time data every minute

# Check if Stock Market is Open
def is_stock_market_open(current_datetime=None):
    if current_datetime is None:
        current_datetime = datetime.now()
    market_open_time = time(9, 30)
    market_close_time = time(16, 0)
    current_time_et = current_datetime.astimezone(timezone(timedelta(hours=-4)))  # EDT (UTC-4)
    return current_time_et.weekday() < 5 and market_open_time <= current_time_et.time() < market_close_time

# Main Execution
if __name__ == "__main__":
    stock_symbol = "AMZN,AAPL,GOOGL,MSFT,TSLA"
    retrieve_real_time_data(producer, stock_symbol, KAFKA_TOPIC)

