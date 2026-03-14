import duckdb

def draw_bar(label, value, max_value, width=40):
    if max_value == 0:
        bar_length = 0
    else:
        bar_length = int((value / max_value) * width)
    
    bar = "█" * bar_length
    return f"{label:10} | {bar} {value}°C"

def show_weather_chart():
    try:
        con = duckdb.connect("data/final/weather_warehouse.db")
        
        df = con.execute("""
            SELECT time, temperature_2m 
            FROM weather_history 
            ORDER BY time DESC 
            LIMIT 15
        """).df()
        con.close()

        if df.empty:
            return

        # Ordenar cronológicamente
        df = df.iloc[::-1]
        
        temps = df['temperature_2m'].tolist()
        labels = df['time'].dt.strftime('%H:%M').tolist()
        max_temp = max(temps) if temps else 0

        print("\n" + "="*50)
        print(" BARCELONA WEATHER REPORT (Gold Layer)")
        print("="*50 + "\n")

        for label, temp in zip(labels, temps):
            print(draw_bar(label, temp, max_temp))

        print("\n" + "="*50)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    show_weather_chart()