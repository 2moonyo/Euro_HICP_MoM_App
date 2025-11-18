-- Physical staging table counts
SELECT 'energy_stg' AS t, COUNT(*) FROM bronze.energy_prices_stg
UNION ALL SELECT 'rates_stg', COUNT(*) FROM bronze.ecb_rates_eu_stg
UNION ALL SELECT 'fx_stg',    COUNT(*) FROM bronze.ecb_fx_eu_stg
UNION ALL SELECT 'hicp_stg',  COUNT(*) FROM bronze.eurostat_hicp_stg;

-- LWW view (deduped) counts
SELECT 'energy_v' AS v, COUNT(*) FROM bronze.energy_prices
UNION ALL SELECT 'rates_v', COUNT(*) FROM bronze.ecb_rates_eu
UNION ALL SELECT 'fx_v',    COUNT(*) FROM bronze.ecb_fx_eu
UNION ALL SELECT 'hicp_v',  COUNT(*) FROM bronze.eurostat_hicp;

-- Spot check most recent HICP rows
SELECT * FROM bronze.eurostat_hicp ORDER BY dt DESC LIMIT 5;

-- Introspection: only bronze
SELECT table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'bronze'
ORDER BY 1,2,ordinal_position;
