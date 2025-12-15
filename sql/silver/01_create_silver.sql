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

-- 5) EUROSTAT LABOUR UNEMPLOYMENT

CREATE OR REPLACE TABLE silver.eurostat_labour_unemployment AS
SELECT
  dt AS ts_date,
  CASE 
    WHEN geo IS NULL OR TRIM(geo) IN ('', 'NA', 'N/A', 'na', 'n/a', 'null', 'NULL') 
      THEN NULL 
    ELSE geo 
  END AS geo,
  series_id,
  'unemployment_rate' AS measure,
  value,
  'percent of active population' AS unit,
  'M' AS freq,
  source,
  ingest_ts AS vintage_ts
FROM bronze.eurostat_labour_unemployment_stg;

-- 6) EUROSTAT LABOUR COST INDEX

CREATE OR REPLACE TABLE silver.labour_cost_index AS
SELECT
  CASE 
    WHEN dt LIKE '%-Q1' THEN make_date(left(dt,4)::INTEGER, 1, 1)
    WHEN dt LIKE '%-Q2' THEN make_date(left(dt,4)::INTEGER, 4, 1)
    WHEN dt LIKE '%-Q3' THEN make_date(left(dt,4)::INTEGER, 7, 1)
    WHEN dt LIKE '%-Q4' THEN make_date(left(dt,4)::INTEGER, 10, 1)
    ELSE NULL
  END AS ts_date,
  geo,
  series_id,
  'labour_cost_index' AS measure,
  value,
  'Index (2020=100)' AS unit,
  'Q' AS freq,
  source,
  ingest_ts AS vintage_ts
FROM bronze.labour_cost_index_stg;

-- 7) EUROSTAT JOB VACANCY RATE

CREATE OR REPLACE TABLE silver.labour_job_vacancy_rate AS
SELECT
  CASE 
    WHEN dt LIKE '%-Q1' THEN make_date(left(dt,4)::INTEGER, 1, 1)
    WHEN dt LIKE '%-Q2' THEN make_date(left(dt,4)::INTEGER, 4, 1)
    WHEN dt LIKE '%-Q3' THEN make_date(left(dt,4)::INTEGER, 7, 1)
    WHEN dt LIKE '%-Q4' THEN make_date(left(dt,4)::INTEGER, 10, 1)
  END AS ts_date,
  geo,
  series_id,
  'labour_job_vacancy_rate' AS measure,
  value,
  'percent' AS unit,
  'Q' AS freq,
  source,
  ingest_ts
FROM bronze.labour_job_vacancy_rate_stg;

-- 8) EUROSTAT HOURS WORKED

CREATE OR REPLACE TABLE silver.labour_hours_worked AS
SELECT
  CASE 
    WHEN dt LIKE '%-Q1' THEN make_date(left(dt,4)::INTEGER, 1, 1)
    WHEN dt LIKE '%-Q2' THEN make_date(left(dt,4)::INTEGER, 4, 1)
    WHEN dt LIKE '%-Q3' THEN make_date(left(dt,4)::INTEGER, 7, 1)
    WHEN dt LIKE '%-Q4' THEN make_date(left(dt,4)::INTEGER, 10, 1)
  END AS ts_date,
  geo,
  series_id,
  'hours_worked' AS measure,
  value,
  'Index (2021=100)' AS unit,
  'Q' AS freq,
  source,
  ingest_ts
FROM bronze.labour_hours_worked_stg;



