CREATE OR REPLACE VIEW silver.all_series AS
SELECT * FROM silver.energy_prices
UNION ALL
SELECT * FROM silver.ecb_rates
UNION ALL
SELECT * FROM silver.ecb_fx
UNION ALL
SELECT * FROM silver.eurostat_hicp;
