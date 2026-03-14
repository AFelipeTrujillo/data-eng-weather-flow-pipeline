import duckdb

def run_analytics():
    # Connect to the local database
    con = duckdb.connect("data/final/weather_warehouse.db")
    
    print("--- Quick Summary ---")
    # Query: Get the average temperature and max wind speed
    query = """
    SELECT 
        city,
        AVG(temperature_2m) as avg_temp,
        MAX(wind_speed_10m) as max_wind,
        COUNT(*) as data_points
    FROM weather_history
    GROUP BY city
    """
    
    result = con.execute(query).df()
    print(result)
    
    print("\n--- Last 5 Hours in Barcelona ---")
    print(con.execute("SELECT time, temperature_2m FROM weather_history ORDER BY time DESC LIMIT 5").df())
    
    con.close()

if __name__ == "__main__":
    run_analytics()