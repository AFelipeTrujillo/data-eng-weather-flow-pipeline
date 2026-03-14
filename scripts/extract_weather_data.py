import requests
import json
import os
from datetime import datetime

class WeatherExtractor:
    def __init__(self, latitude: float, longitude: float):
        self.base_url = "https://api.open-meteo.com/v1/forecast"
        self.params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m",
            "timezone": "UTC"
        }
        # Ensure the directory exists
        self.output_path = "data/raw"
        os.makedirs(self.output_path, exist_ok=True)

    def fetch_data(self) -> dict:
        """Fetches weather data from the API."""
        try:
            response = requests.get(self.base_url, params=self.params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            raise

    def save_to_json(self, data: dict):
        """Saves the dictionary to a JSON file with a timestamp."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"weather_data_{timestamp}.json"
        full_path = os.path.join(self.output_path, filename)
        
        with open(full_path, 'w') as f:
            json.dump(data, f, indent=4)
        
        print(f"Data successfully saved to {full_path}")

if __name__ == "__main__":
    # Location: Barcelona
    extractor = WeatherExtractor(latitude=41.3851, longitude=2.1734)
    weather_data = extractor.fetch_data()
    extractor.save_to_json(weather_data)