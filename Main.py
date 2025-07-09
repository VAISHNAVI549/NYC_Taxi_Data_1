import pandas as pd
from sqlalchemy import create_engine
import ssl

# Disable SSL verification (since i had SSL cert issues on my Mac)
ssl._create_default_https_context = ssl._create_unverified_context

# PostgreSQL connection setup
engine = create_engine("postgresql+psycopg2://vaishnaviyvs@localhost:5432/nyc_taxi")

# Download + read data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
print("Downloading Data...")
df = pd.read_parquet(url)
print(f"Raw data downloaded with shape: {df.shape}")

# Explore data
print("\n=== Data info ===")
print(df.info())

print("\n=== Missing values per column ===")
print(df.isnull().sum())

print("\n=== Summary statistics ===")
print(df.describe())

# Clean data
# 1️⃣ Drop rows missing pickup or dropoff datetime
df_clean = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

# 2️⃣ Remove rows where fare or distance are <= 0
df_clean = df_clean[
    (df_clean["fare_amount"] > 0) &
    (df_clean["trip_distance"] > 0)
]

# 3️⃣ Fill missing tip_amount (optional, good practice)
if "tip_amount" in df_clean.columns:
    df_clean["tip_amount"] = df_clean["tip_amount"].fillna(0)

print(f"\nAfter cleaning, data shape: {df_clean.shape}")

# Load clean data
print("Loading clean data into table: taxi_data_clean...")
df_clean.to_sql("Taxi_Data_clean", engine, if_exists="replace", index=False)
print("Clean data loaded successfully!")
