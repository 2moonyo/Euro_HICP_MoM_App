-- 1) ENERGY
CREATE OR REPLACE TABLE silver.energy_prices AS
SELECT
  dt AS ts_date,
  CASE 
    WHEN geo IS NULL OR TRIM(geo) IN ('', 'NA', 'N/A', 'na', 'n/a', 'null', 'NULL') 
    THEN NULL 
    ELSE geo 
  END AS geo,
  series_id,
  'energy_price' AS measure,
  value,
  'USD' AS unit,
  'M' AS freq,
  source,
  ingest_ts AS vintage_ts
FROM bronze.energy_prices;

-- 2) ECB RATES
CREATE OR REPLACE TABLE silver.ecb_rates AS
SELECT
  dt AS ts_date,
  CASE 
    WHEN geo IS NULL OR TRIM(geo) IN ('', 'NA', 'N/A', 'na', 'n/a', 'null', 'NULL') 
    THEN NULL 
    ELSE geo 
  END AS geo,
  series_id,
  'policy_rate' AS measure,
  value,
  'percent' AS unit,
  'D' AS freq,
  source,
  ingest_ts AS vintage_ts
FROM bronze.ecb_rates_eu;

-- 3) ECB FX
CREATE OR REPLACE TABLE silver.ecb_fx AS
SELECT
  dt AS ts_date,
  CASE 
    WHEN geo IS NULL OR TRIM(geo) IN ('', 'NA', 'N/A', 'na', 'n/a', 'null', 'NULL') 
    THEN NULL 
    ELSE geo 
  END AS geo,
  series_id,
  'exchange_rate' AS measure,
  value,
  NULL AS unit,
  'D' AS freq,
  source,
  ingest_ts AS vintage_ts
FROM bronze.ecb_fx_eu;

-- 4) EUROSTAT HICP
CREATE OR REPLACE TABLE silver.eurostat_hicp AS
SELECT
  dt AS ts_date,
  CASE 
    WHEN geo IS NULL OR TRIM(geo) IN ('', 'NA', 'N/A', 'na', 'n/a', 'null', 'NULL') 
    THEN NULL 
    ELSE geo 
  END AS geo,
  series_id,
  'price_index' AS measure,
  value,
  'Index (2015=100)' AS unit,
  'M' AS freq,
  source,
  ingest_ts AS vintage_ts
FROM bronze.eurostat_hicp;