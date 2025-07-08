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