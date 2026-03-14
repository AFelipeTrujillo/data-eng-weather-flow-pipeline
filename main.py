import sys
import logging
from datetime import datetime

# Import your classes from the scripts folder
from scripts.extract_weather_data import WeatherExtractor
from scripts.transform_weather_data import WeatherTransformer

# Configure a logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_pipeline():
    start_time = datetime.now()
    logging.info("Starting Data-Eng-Weather-Flow-Pipeline")

    try:
        # 1. Extraction Layer (Bronze)
        logging.info("STEP 1: Extracting data from Barcelona API...")
        extractor = WeatherExtractor(latitude=41.3851, longitude=2.1734)
        raw_data = extractor.fetch_data()
        extractor.save_to_json(raw_data)
        logging.info("Extraction successful.")

        # 2. Transformation & Loading Layer (Silver & Gold)
        logging.info("STEP 2: Transforming JSON to Parquet and Loading to DuckDB...")
        transformer = WeatherTransformer(db_path="data/final/weather_warehouse.db")
        df = transformer.process_latest_file()
        
        if df is not None:
            transformer.load_to_duckdb(df)
            logging.info("Transformation and Loading successful.")
        else:
            logging.warning("No data was processed.")

        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Pipeline finished successfully in {duration.total_seconds():.2f} seconds.")

    except Exception as e:
        logging.error(f"Pipeline FAILED: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()