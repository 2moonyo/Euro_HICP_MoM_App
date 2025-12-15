-- ENERGY (monthly) → bronze.energy_prices_stg
CREATE OR REPLACE TABLE bronze.energy_prices_stg AS
SELECT
  try_cast(dt AS DATE)                         AS dt,
  dataset,
  series_id,
  COALESCE(NULLIF(TRIM(geo), ''), 'EA20')      AS geo,           
  try_cast(value AS DOUBLE)                    AS value,
  source,
  try_cast(ingest_ts AS TIMESTAMPTZ)           AS ingest_ts
FROM read_csv_auto(
  'data/bronze/energy_prices/dt=*/part-*.csv',
  hive_partitioning = 1
);

-- ECB RATES (daily) → bronze.ecb_rates_eu_stg
CREATE OR REPLACE TABLE bronze.ecb_rates_eu_stg AS
SELECT
  try_cast(dt AS DATE)                         AS dt,
  dataset,
  series_id,
  COALESCE(NULLIF(TRIM(geo), ''), 'EA20')      AS geo,
  try_cast(value AS DOUBLE)                    AS value,
  source,
  try_cast(ingest_ts AS TIMESTAMPTZ)           AS ingest_ts
FROM read_csv_auto(
  'data/bronze/ecb_rates_eu/dt=*/part-*.csv',
  hive_partitioning = 1
);

-- ECB FX (daily) → bronze.ecb_fx_eu_stg
CREATE OR REPLACE TABLE bronze.ecb_fx_eu_stg AS
SELECT
  try_cast(dt AS DATE)                         AS dt,
  dataset,
  series_id,
  COALESCE(NULLIF(TRIM(geo), ''), 'EA20')      AS geo,
  try_cast(value AS DOUBLE)                    AS value,
  source,
  try_cast(ingest_ts AS TIMESTAMPTZ)           AS ingest_ts
FROM read_csv_auto(
  'data/bronze/ecb_fx_eu/dt=*/part-*.csv',
  hive_partitioning = 1
);

-- EUROSTAT HICP (monthly) → bronze.eurostat_hicp_stg
CREATE OR REPLACE TABLE bronze.eurostat_hicp_stg AS
SELECT
  try_cast(dt AS DATE)                         AS dt,
  dataset,
  series_id,
  COALESCE(NULLIF(TRIM(geo), ''), 'EA20')      AS geo,
  try_cast(value AS DOUBLE)                    AS value,
  source,
  try_cast(ingest_ts AS TIMESTAMPTZ)           AS ingest_ts
FROM read_csv_auto(
  'data/bronze/eurostat_hicp/dt=*/part-*.csv',
  hive_partitioning = 1
);

-- EUROSTAT LABOUR UNEMPLOYMENT (monthly) → bronze.eurostat_labour_unemployment_stg
CREATE OR REPLACE TABLE bronze.eurostat_labour_unemployment_stg AS
SELECT
  try_cast(dt AS DATE)                         AS dt,
  dataset,
  series_id,
  COALESCE(NULLIF(TRIM(geo), ''), 'EA20')      AS geo,           
  try_cast(value AS DOUBLE)                    AS value,
  source,
  try_cast(ingest_ts AS TIMESTAMPTZ)           AS ingest_ts
FROM read_csv_auto(
  'data/bronze/labour_unemployment/dt=*/part-*.csv',
  hive_partitioning = 1
);

-- EUROSTAT LABOUR COST INDEX (quartely) → bronze.eurostat_labour_labour_cost_index_stg
CREATE OR REPLACE TABLE bronze.labour_cost_index_stg AS
SELECT
  dt,  
  dataset,
  series_id,
  COALESCE(NULLIF(TRIM(geo), ''), 'EA20') AS geo,
  try_cast(value AS DOUBLE) AS value,
  source,
  try_cast(ingest_ts AS TIMESTAMPTZ) AS ingest_ts
FROM read_csv_auto(
  'data/bronze/labour_cost_index/dt=*/part-*.csv',
  hive_partitioning = 1
);

-- EUROSTAT JOB VACANCY_RATE (quartely) → bronze.eurostat_labour_labour_cost_index_stg
CREATE OR REPLACE TABLE bronze.labour_job_vacancy_rate_stg AS
SELECT
  dt,
  dataset,
  series_id,
  COALESCE(NULLIF(TRIM(geo), ''), 'EA20') AS geo,
  try_cast(value AS DOUBLE) AS value,
  source,
  try_cast(ingest_ts AS TIMESTAMPTZ) AS ingest_ts
FROM read_csv_auto(
  'data/bronze/labour_job_vacancy_rate/dt=*/part-*.csv',
  hive_partitioning = 1
);

-- EUROSTAT HOURS WORKED (quartely) → bronze.eurostat_labour_labour_cost_index_stg
CREATE OR REPLACE TABLE bronze.labour_hours_worked_stg AS
SELECT
  dt,
  dataset,
  series_id,
  COALESCE(NULLIF(TRIM(geo), ''), 'EA20') AS geo,
  try_cast(value AS DOUBLE) AS value,
  source,
  try_cast(ingest_ts AS TIMESTAMPTZ) AS ingest_ts
FROM read_csv_auto(
  'data/bronze/labour_hours_worked/dt=*/part-*.csv',
  hive_partitioning = 1
);
