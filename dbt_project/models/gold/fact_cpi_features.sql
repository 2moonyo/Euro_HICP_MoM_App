{{ config(materialized='table') }}

with hicp as ( select * from {{ ref('fact_hicp') }} ),
macro as ( select * from {{ ref('fact_macro') }} ),

joined as (
  select
    h.month,
    h.geo,
    h.series_id,
    h.hicp_index,
    m.fx_usd_eur_avg,
    m.policy_rate_monthly_avg,
    m.policy_rate_eom,
    m.brent_crude_usd,
    m.nat_gas_eu_usd
  from hicp h
  left join macro m
    on m.month = h.month and m.geo = h.geo
),

lags as (
  select
    *,
    lag(hicp_index, 1)  over (partition by geo, series_id order by month) as lag_1m,
    lag(hicp_index, 3)  over (partition by geo, series_id order by month) as lag_3m,
    lag(hicp_index, 6)  over (partition by geo, series_id order by month) as lag_6m,
    lag(hicp_index, 12) over (partition by geo, series_id order by month) as lag_12m
  from joined
),

changes as (
  select
    *,
    case when lag_1m  is not null then hicp_index / lag_1m  - 1 end as hicp_mom,
    case when lag_12m is not null then hicp_index / lag_12m - 1 end as hicp_yoy
  from lags
)

select * from changes
