{{ config(materialized='table') }}

with features as ( select * from {{ ref('fact_cpi_features') }} ),

headline as (
  select * from features where series_id = 'CP00'   -- ASSUMPTION: headline
)

select
  month,
  geo,
  hicp_index as target_hicp_index,      -- nowcast target (t)
  lag_1m, lag_3m, lag_6m, lag_12m,
  hicp_mom, hicp_yoy,
  fx_usd_eur_avg,
  policy_rate_monthly_avg,
  policy_rate_eom,
  (brent_crude_usd + nat_gas_eu_usd) / 2.0 as energy_price_usd
from headline
order by month, geo
