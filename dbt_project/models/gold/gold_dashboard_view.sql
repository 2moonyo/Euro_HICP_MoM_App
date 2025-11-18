{{ config(materialized='view') }}

select
  month,
  geo,
  max(case when series_id = 'CP00' then hicp_index end) as hicp_index,
  max(fx_usd_eur_avg) as fx_usd_eur_avg,
  max(policy_rate_eom) as policy_rate_eom,
  max(brent_crude_usd) as brent_crude_usd,
  max(nat_gas_eu_usd) as nat_gas_eu_usd
from {{ ref('fact_cpi_features') }}
group by month, geo
order by month, geo
