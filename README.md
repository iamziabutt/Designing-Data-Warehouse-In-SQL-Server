## Designing Data Warehouse Using SQL Server

We are going to design a data warehouse in sql server for historical weather data for multiple cities.

### 🌿Solution Archecture:

- **Extract:** Python script (requests) → Staging tables

- **Transform/Clean:** T-SQL (stored procedures)

- **Load:** Incremental `MERGE` into star schema

- **Orchestration:** Windows Task Scheduler + SQL Agent jobs

- **Monitoring:** Database Mail alerts


### 🌿Project Structure:
```text
text

D:\Designing data warehouse in sql server\
├── etl\
│   ├── run_etl.bat          <-- Batch file (create here)
│   ├── extract_weather.py   <-- Python script
│   └── transform_load.sql   <-- SQL transformation script
└── logs\ 
|
├── data_extraction.ipynb 
└── README.md
```

### 🌿Plan:

1. **Extraction (Python module)**
    - use he Open-Meteo Historical Weather API (https://archive-api.open-meteo.com/v1/archive) to fetch daily weather data. No API key required
    -  `First run`: fetch data from 2000-01-01 to current date for all cities.
    - `Subsequent runs`: fetch data from the last available date in our database to current date (incremental).
    -  We'll store the raw JSON response in a staging table in SQL Server.

2. **Staging (SQL server)**
   - Create a staging table to store the raw JSON data and metadata (e.g., city, date of extraction).
   - The Python script will insert the raw JSON into this staging table.

3. **Transformation and cleaning (SQL server)**
    - We will use SQL Server to parse the JSON, transform, clean, and load into dimension and fact tables.
   - We'll design a star schema with:
        - A date dimension table (pre-built for 2000 to current year+)
        - A location dimension table (cities: London, New York, Sydney, Lahore, Dubai)
        - A fact table for daily weather measurements.

4. **Incremental Loading:**
   - For the `fact table`, we will load new data and update existing data if necessary (though historical weather data is immutable, so updates are unlikely).
   - We'll use a `staging table` to hold the transformed data and then merge into the fact table.

5. **Additional Features:**
     - **Change Data Capture (CDC)**: Enable CDC on the staging table to capture new extracts, but note that CDC is for tracking changes in the source table. We might use it for near-real-time, but our process is daily. We'll set up CDC on the fact table if needed for downstream processes.
     - **Scheduling**: Use Windows Task Scheduler to run the Python script daily.
6. **Cost Free**: We are using SQL Server Developer (free for development) and Open-Meteo API (free for non-commercial use).

7. **Production**: Schedule with Windows Task Scheduler, set up Database Mail for alerts

### 🌿Step By Step Implementation:



 ✨**Step 1: Creating SQL setup**
   - Create a new database named `"WeatherDataWarehouse"`
    - We will have:
        - Dimension table called `dbo.dim_city`
        - Fact table called `dbo.fact_weather`
   - We will use Slowly Changing Dimension (SCD) Type 2 for `dbo.dim_city` if attributes of a city change over time (like population, timezone, etc.)
   - For incremental load, we will use the staging table to update the fact and dimension tables.

```sql
sql

-- DIMENSION TABLE
CREATE TABLE dim_city (
    city_id INT PRIMARY KEY IDENTITY,
    city_name NVARCHAR(50) NOT NULL,
    country NVARCHAR(50),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    timezone NVARCHAR(50),
    valid_from DATETIME2 GENERATED ALWAYS AS ROW START,
    valid_to DATETIME2 GENERATED ALWAYS AS ROW END,
    PERIOD FOR SYSTEM_TIME (valid_from, valid_to)
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.dim_city_history)
);

-- FACT TABLE
CREATE TABLE fact_weather (
    weather_id BIGINT PRIMARY KEY IDENTITY,
    city_id INT FOREIGN KEY REFERENCES dim_city(city_id),
    date DATE NOT NULL,
    temp_max DECIMAL(5,2),
    temp_min DECIMAL(5,2),
    precipitation DECIMAL(5,2),
    load_timestamp DATETIME2 DEFAULT GETDATE()  -- to track and store changes to data over time
);

-- STAGING TABLE (Raw API data)
CREATE TABLE stg_weather_raw (
    city_name NVARCHAR(50),
    date DATE,
    temp_max DECIMAL(5,2),
    temp_min DECIMAL(5,2),
    precipitation DECIMAL(5,2),
    is_processed BIT DEFAULT 0 -- to track if data is processed or not
);

```
 
✨**Step 2: Extract Data with Python**
   - We'll write a Python script that:
        - Checks the last date available in the fact table (for incremental) or if the table is empty does a full load.
        - For the first run, it extracts from 2000-01-01 to today.
        - For subsequent runs, it extracts from the last available date + 1 day to today.
   - We will store the raw data in a staging table in SQL Server.
   - Connects to SQL Server to find the maximum date for each city in the fact table (if any)
   - For cities that don't have any data, it fetches from 2000-01-01 to today.
   - For cities that have data, it fetches from the day after the last record to today.
   - The Open-Meteo API endpoint: 
        https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&daily={metrics}
  
   - We need the coordinates for the cities:
```
London: 51.5074, -0.1278
New York: 40.7128, -74.0060
Sydney: -33.8688, 151.2093
Lahore: 31.5204, 74.3587
Dubai: 25.276987, 55.296249 
```
 - The script will (for each city):
                    - Determine the start date (either 2000-01-01 or last date + 1 day) and end date (today)
            - If the start date is greater than end date, skip.
            - Call the API and parse the JSON.
            - Insert the data into the staging table.

```python
import requests
import pyodbc
from datetime import datetime, timedelta
import time

# 1. Define cities and coordinates. Picked up randomly
CITIES = {
    "London": {"lat": 51.5074, "lon": -0.1278},
    "New York": {"lat": 40.7128, "lon": -74.0060},
    "Sydney": {"lat": -33.8688, "lon": 151.2093},
    "Lahore": {"lat": 31.5204, "lon": 74.3587},
    "Dubai": {"lat": 25.276987, "lon": 55.296249}
}

# 2. Connect to SQL Server
sql_connection = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=WeatherDataWarehouse;'
    'Trusted_Connection=yes;'
)

# 3. Process each city (city loop starts here)
for city, coords in CITIES.items():
    # 4. Get last recorded date
    cursor = sql_connection.cursor()
    cursor.execute("SELECT MAX(date) FROM fact_weather WHERE city_id = (SELECT city_id FROM dim_city WHERE city_name = ?)", city)
    last_date = cursor.fetchone()[0] or datetime(2000, 1, 1).date()
    
    # 5. Calculate date range
    start_date = last_date + timedelta(days=1)
    end_date = datetime.now().date()
    if start_date > end_date:
        continue
    
    # 6. Fetch weather data from API with retry method
    max_retries = 3 # maximum attempt requests
    retry_delay = 60  # wait for 60 seconds for the next try for a failed city
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={coords['lat']}&longitude={coords['lon']}&start_date={start_date}&end_date={end_date}&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
    daily_data = None

    # retry loop within city loop
    for attempt in range(max_retries):
        response = requests.get(url) # fetching response
        if response.status_code == 200: # confirming the response with 200 code
            data = response.json() # storing API response in json format
            if 'daily' in data:
                daily_data = data['daily'] # daily key keeps the data
                print(f"Data fetched for {city} successfully")
                break  # Exit retry loop on success
        print(f"Attempt {attempt+1} failed for {city}. Retrying in 60 seconds...")
        time.sleep(retry_delay)
    else:  # Executes if loop completes without break
        print(f"ERROR for {city}: Failed after {max_retries} attempts")
        continue  # city loop ends here for failed city
    
    # 7. Insert each daily record into the staging table
    
    '''
    - insertion is only for successful API calls
    - insertion loop must be within main city loop
    - insertion will be written to stg_weather_raw table in data warehouse   
    ''' 
    for i in range(len(daily_data['time'])):
        cursor.execute("""
            INSERT INTO stg_weather_raw (city_name, date, temp_max, temp_min, precipitation)
            VALUES (?, ?, ?, ?, ?)
        """, city, 
           daily_data['time'][i],
           daily_data['temperature_2m_max'][i],
           daily_data['temperature_2m_min'][i],
           daily_data['precipitation_sum'][i])
    
    sql_connection.commit() # commiting the insertion
    print(f"Inserted {len(daily_data['time'])} records for {city}")

# 8. Clean up
sql_connection.close()
```
 
  
 ✨**Step 3:  ETL: Staging and Transformation in SQL Server**
   - We are going to create a staging table `stg_weather_raw` to land the raw data from Python.
   - Then use stored procedures to:
        - Clean staging table: TRUNCATE TABLE staging_weather before each extract
        - Remove duplicates: If the same city and date appears multiple times, take the latest or an aggregate? We can use ROW_NUMBER() to remove duplicates.
        - Handle missing data (e.g., fill with average, or leave as NULL if appropriate)
        - Outlier detection (using statistical methods, e.g., z-score, IQR) and handle (e.g., cap, floor, or set to NULL)
        - Data enrichment (e.g., adding season based on date, day of week, etc.)
        - Data integration (combining with other dimensions, e.g., city dimension)
        - Derived columns (e.g., feels-like temperature, humidity index, etc. if applicable)
        - Normalization (we are using a star schema, so we break into dimensions and facts)
        - SCD for dim_city: We will have an initial load of the cities. Since city attributes are static, we might not need SCD Type 2. But we'll design for it.
        - Apply business rules and aggregations (if any)
 

```sql
sql

USE WeatherDataWarehouse;
GO

/*updated on 08.07.25*/


-- STEP 1: Data Cleaning

WITH Deduplicated AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY city_name, date 
        ORDER BY (SELECT NULL)) AS rn
    FROM stg_weather_raw
    WHERE is_processed = 0
)
DELETE FROM Deduplicated WHERE rn > 1;

-- Handle missing data (example: temperature)

UPDATE stg_weather_raw
SET temp_max = (SELECT AVG(temp_max) FROM stg_weather_raw WHERE city_name = s.city_name AND MONTH(date) = MONTH(s.date)),
    temp_min = (SELECT AVG(temp_min) FROM stg_weather_raw WHERE city_name = s.city_name AND MONTH(date) = MONTH(s.date))
FROM stg_weather_raw s
WHERE (temp_max IS NULL OR temp_min IS NULL) AND is_processed = 0;

-- Outlier detection (Z-score method)
WITH Stats AS (
    SELECT city_name, 
        AVG(temp_max) AS mean, 
        STDEV(temp_max) AS stddev
    FROM stg_weather_raw
    GROUP BY city_name
)
UPDATE s
SET temp_max = CASE WHEN ABS(s.temp_max - st.mean) / st.stddev > 3 THEN st.mean ELSE s.temp_max END
FROM stg_weather_raw s
JOIN Stats st ON s.city_name = st.city_name
WHERE is_processed = 0;


-- STEP 2: SCD Type 2 for dim_city

/*
We are performing a Slowly Changing Dimension (SCD) Type 2 operation on
the `dbo.dim_city` dimension table and inserting new cities into dimension
table from the staging table. This only handles new inserts

Specifically, this MERGE statement:
   - Compares the staging table (`dbo.stg_weather_raw`) with the target dimension table (`dbo.dim_city`).
   - For any new city that exists in the staging table but not in the dimension table (i.e., `WHEN NOT MATCHED`), it inserts a new row into `dim_city` with the city name.
*/

MERGE dim_city AS target
USING (SELECT DISTINCT city_name FROM stg_weather_raw) AS source
ON target.city_name = source.city_name
WHEN NOT MATCHED THEN
    INSERT (city_name) VALUES (source.city_name);

-- STEP 3: Load fact table (incremental)
MERGE fact_weather AS target
USING (
    SELECT 
        c.city_id,
        s.date,
        s.temp_max,
        s.temp_min,
        s.precipitation
    FROM stg_weather_raw s
    JOIN dim_city c ON s.city_name = c.city_name
    WHERE s.is_processed = 0
) AS source
ON (target.city_id = source.city_id AND target.date = source.date)
WHEN MATCHED THEN
    UPDATE SET 
        temp_max = source.temp_max,
        temp_min = source.temp_min,
        precipitation = source.precipitation
WHEN NOT MATCHED THEN
    INSERT (city_id, date, temp_max, temp_min, precipitation)
    VALUES (source.city_id, source.date, source.temp_max, source.temp_min, source.precipitation);

-- Mark records as processed
UPDATE stg_weather_raw SET is_processed = 1;
```

 


 ✨**Step 4: Orchestration & Scheduling**
   - Create a batch file (`run_etl_bat`) in `etl` sub-folder of project directory
   - Parameters used:
        - -S localhost: Server name
        - -d WeatherDataWarehouse: Database name
        - -i transform_load.sql: Input script file
        - -o output.log: Redirects output to log file
        - -a packetsize: Network packet size (improves large data transfers)
        - -b: Exit on error (critical for failure detection)

   - Key components explained
        - Checks `%errorlevel%` after each step
        - Daily log files with date in filename
        - Captures both Python and SQL outputs
        - Run daily via Windows Task Scheduler
        - Daly schedule time: 2 AM (randomly selected due to being off-peak hours)
        - Run with highest privileges

`
Please ensure that Task Scheduler account is same as SQL server account. I am using windows credentials to log in to SQL server and same credentials I am using to create etl task in Task Scheduler. 
`


**Task Scheduler Setup:**

**1 - Open Task Scheduler:**

Search for "Task Scheduler" in Windows

**2 - Create Task:**

- Name: `WeatherData_ETL`
- Security options: `"un whether user is logged in or not`
- Trigger: Daily at 2:00 AM
- Action: Start a program
- Program/script:` D:\Zia\data-engineering\Projects\Designing Data Warehouse In SQL Server\etl\run_etl.bat`

```powershell
powershell

# Manual test command
Start-Process "D:\Zia\data-engineering\Projects\Designing Data Warehouse In SQL Server\etl\run_etl.bat" -WindowStyle Hidden
```

✨**Step 5: Change Data Capture (CDC)**
   We enable CDC on the database and on the staging table and fact table? But note: CDC is for capturing changes in the source. Our source is the API, so we are getting the data and then loading. We don't have a source database that changes. However, we can use CDC to capture changes in the staging table? But that might be overkill.

   ```sql
   sql

   EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'fact_weather', @role_name = NULL;
   
   ```
   
✨**Step 6: Future Enhancements**

 Spinning of to the main project, I'll be creating more repositories with these potential enhancements:
   - Using a more sophisticated orchestration tool such as `Apache Airflow` 
   - Real-time data streaming (e.g., using `Kafka`, and then ingest into the data warehouse)
   - ETL workflows using `SQL Server Integration Services (SSIS)`
   - Data quality framework (e.g., using Great Expectations)
   - Further validation checks post -ETL process
