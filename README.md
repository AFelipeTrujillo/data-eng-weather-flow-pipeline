# Barcelona Weather Data Pipeline (Lite ETL)

A high-performance, local ETL (Extract, Transform, Load) pipeline that ingests weather data from Barcelona, processes it into columnar formats, and loads it into an analytical data warehouse.

## Architecture: The Medallion Approach
This project follows the **Medallion Architecture** to ensure data quality and traceability:

1.  **Bronze Layer (Raw):** Ingests raw JSON data from the Open-Meteo API. Data is stored in `data/raw/` with timestamps to maintain a history of ingestions.
2.  **Silver Layer (Processed):** Cleans and flattens the JSON into **Apache Parquet** format using `fastparquet`. This stage ensures schema consistency and optimizes storage.
3.  **Gold Layer (Final):** Loads the processed data into **DuckDB**, an in-process analytical database, allowing for high-speed SQL queries and reporting.

## Tech Stack
- **Language:** Python 3.12+
- **Data Processing:** Pandas & PyArrow/Fastparquet
- **Storage:** Apache Parquet (Columnar storage)
- **Database:** DuckDB (OLAP Optimized)
- **Orchestration:** Custom Python Orchestrator (`main.py`)

## 🚀 Getting Started

### Prerequisites
- Python installed
- Virtual environment (recommended)

### Installation
1. Clone the repository:
   ```bash
   git clone <your-repo-url>
   cd data-eng-weather-flow-pipeline
   ```

2. Set up the environment:

    ```bash
    python -m venv venv
    source venv/bin/activate  # Windows: venv\Scripts\activate
    pip install -r requirements.txt
    ```

### Running the Pipeline

To execute the full ETL flow (Extract -> Transform -> Load):
```bash
python main.py
```

To run a sample analytical query on the warehouse:
```bash
python scripts/query_warehouse.py
```

### Why DuckDB + Parquet?

Instead of a traditional row-based RDBMS (like Postgres), this pipeline uses a columnar-first approach.

* Parquet reduces disk I/O by only reading the columns needed for analysis.

* DuckDB provides the power of SQL without the overhead of a server-client architecture, making it the perfect choice for modern "Lite" Data Engineering stacks.