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
