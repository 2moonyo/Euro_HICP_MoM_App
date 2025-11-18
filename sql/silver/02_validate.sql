-- Count by dataset
SELECT 'energy' AS t, COUNT(*) FROM silver.energy_prices
UNION ALL SELECT 'rates', COUNT(*) FROM silver.ecb_rates
UNION ALL SELECT 'fx', COUNT(*) FROM silver.ecb_fx
UNION ALL SELECT 'hicp', COUNT(*) FROM silver.eurostat_hicp;

-- Bronze table count (FIXED - added schema prefix)
SELECT 'energy_bronze' AS b, COUNT(*) FROM bronze.energy_prices
UNION ALL SELECT 'rates_bronze', COUNT(*) FROM bronze.ecb_rates_eu
UNION ALL SELECT 'fx_bronze',    COUNT(*) FROM bronze.ecb_fx_eu
UNION ALL SELECT 'hicp_bronze',  COUNT(*) FROM bronze.eurostat_hicp;