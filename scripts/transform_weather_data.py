import pandas as pd
import duckdb
import glob
import os
import json
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class WeatherTransformer:
    def __init__(self, db_path: str = "data/final/weather_warehouse.db"):
        self.db_path = db_path
        self.raw_path = "data/raw/*.json"
        self.processed_path = "data/processed"
        os.makedirs(self.processed_path, exist_ok=True)

    def process_latest_file(self):
        # Get the newest JSON file
        list_of_files = glob.glob(self.raw_path)
        if not list_of_files:
            print("No raw data found.")
            return

        latest_file = max(list_of_files, key=os.path.getctime)
        logging.info(f"Processing: {latest_file}")

        with open(latest_file, 'r') as f:
            data = json.load(f)

        # Flatten the hourly data into a DataFrame
        hourly_data = data['hourly']
        df = pd.DataFrame(hourly_data)
        
        # Add metadata
        df['extraction_timestamp'] = datetime.now()
        df['city'] = 'Barcelona'
        
        # Convert time column to actual datetime objects
        df['time'] = pd.to_datetime(df['time'])

        # Save to Processed folder (Parquet format)
        parquet_file = os.path.join(self.processed_path, "latest_weather.parquet")
        df.to_parquet(parquet_file, index=False, engine="fastparquet")
        logging.info(f"Saved to Parquet: {parquet_file}")
        
        return df

    def load_to_duckdb(self, df):
        """Loads the DataFrame into a DuckDB table."""
        con = duckdb.connect(self.db_path)
        
        # Create table if it doesn't exist and append data
        con.execute("CREATE TABLE IF NOT EXISTS weather_history AS SELECT * FROM df WHERE 1=0")
        con.execute("INSERT INTO weather_history SELECT * FROM df")
        
        # Verify
        count = con.execute("SELECT count(*) FROM weather_history").fetchone()[0]
        logging.info(f"Total rows in DuckDB: {count}")
        con.close()

if __name__ == "__main__":
    from datetime import datetime
    transformer = WeatherTransformer()
    df = transformer.process_latest_file()
    if df is not None:
        transformer.load_to_duckdb(df)