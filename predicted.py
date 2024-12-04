import pandas as pd
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Initialize InfluxDB client
url = "http://localhost:8086"  # Replace with your InfluxDB URL
token = "m4sU-xYCqZDf0xxAvKJdsIqOhmTUV3ISnd8OrvGS3g38reJaF_JSEdRE8cDkWQEZ65LTVxJw_wJ9D3S9qSQ_2A=="  # Replace with your InfluxDB token
org = "sjsu"          # Replace with your organization name
bucket = "bigdata"    # Replace with your bucket name

client = InfluxDBClient(url=url, token=token, org=org)

# Load the CSV into a DataFrame
df = pd.read_csv("predicted_stocks.csv")  # Replace with the actual CSV file path

# Ensure the Date column is in datetime format and convert it to ISO 8601
df["Date"] = pd.to_datetime(df["Date"]).dt.strftime('%Y-%m-%dT00:00:00Z')

# Write data to InfluxDB
write_api = client.write_api(write_options=SYNCHRONOUS)

for index, row in df.iterrows():
    # Iterate over each stock symbol and its value
    for symbol in ["GOOGL", "AMZN", "AAPL", "MSFT", "TSLA"]:
        point = Point("stock_prices_predicted") \
            .tag("symbol", symbol) \
            .field("close", row[symbol]) \
            .time(row["Date"])  # Use ISO 8601 formatted timestamp
        write_api.write(bucket=bucket, org=org, record=point)

# Close the client connection
client.close()

print("Data written to InfluxDB successfully.")

