-- LWW = last write wins on ingest_ts per (dt, geo, series_id)

CREATE OR REPLACE VIEW bronze.energy_prices AS
SELECT dt, dataset, series_id, geo, value, source, ingest_ts
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY dt, geo, series_id ORDER BY ingest_ts DESC) AS rn
  FROM bronze.energy_prices_stg
)
WHERE rn = 1;

CREATE OR REPLACE VIEW bronze.ecb_rates_eu AS
SELECT dt, dataset, series_id, geo, value, source, ingest_ts
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY dt, geo, series_id ORDER BY ingest_ts DESC) AS rn
  FROM bronze.ecb_rates_eu_stg
)
WHERE rn = 1;

CREATE OR REPLACE VIEW bronze.ecb_fx_eu AS
SELECT dt, dataset, series_id, geo, value, source, ingest_ts
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY dt, geo, series_id ORDER BY ingest_ts DESC) AS rn
  FROM bronze.ecb_fx_eu_stg
)
WHERE rn = 1;

CREATE OR REPLACE VIEW bronze.eurostat_hicp AS
SELECT dt, dataset, series_id, geo, value, source, ingest_ts
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY dt, geo, series_id ORDER BY ingest_ts DESC) AS rn
  FROM bronze.eurostat_hicp_stg
)
WHERE rn = 1;
