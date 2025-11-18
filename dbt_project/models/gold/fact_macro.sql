{{ config(materialized='table') }}

-- FX: daily → monthly average (SHORTCUT v1)
with fx_daily as (
  select
    {{ first_of_month('ts_date') }} as month,
    coalesce(geo, 'EA20') as geo,
    series_id,
    value as fx_value
  from {{ source('silver','ecb_fx') }}
),
fx_monthly as (
  select
    month,
    geo,
    avg(case when series_id ilike '%USD%' then fx_value end) as fx_usd_eur_avg
  from fx_daily
  where month <= date_trunc('month', {{ month_cutoff() }}::date)
  group by 1,2
),

-- Policy rates: provide both signals
rates_daily as (
  select
    {{ first_of_month('ts_date') }} as month,
    coalesce(geo, 'EA20') as geo,
    ts_date,
    value as policy_rate
  from {{ source('silver','ecb_rates') }}
),
rates_monthly as (
  select month, geo, avg(policy_rate) as policy_rate_monthly_avg
  from rates_daily
  where month <= date_trunc('month', {{ month_cutoff() }}::date)
  group by 1,2
),
rates_eom as (
  select month, geo,
         first_value(policy_rate) over (
           partition by geo, month
           order by ts_date desc
         ) as policy_rate_eom
  from rates_daily
  qualify row_number() over (partition by geo, month order by ts_date desc) = 1
),

-- Energy: Split into Brent Crude and Natural Gas
energy_monthly as (
  select
    {{ first_of_month('ts_date') }} as month,
    coalesce(geo, 'EA20') as geo,
    avg(case when series_id = 'POILBREUSDM' then value end) as brent_crude_usd,
    avg(case when series_id = 'PNGASEUUSDM' then value end) as nat_gas_eu_usd
  from {{ source('silver','energy_prices') }}
  where ts_date <= {{ month_cutoff() }}::date
  group by 1,2
)

select
  t.month,
  coalesce(fx.geo, r1.geo, r2.geo, e.geo, 'EA20') as geo,
  fx.fx_usd_eur_avg,
  r1.policy_rate_monthly_avg,
  r2.policy_rate_eom,
  e.brent_crude_usd,
  e.nat_gas_eu_usd
from {{ ref('dim_time') }} t
left join fx_monthly fx on fx.month = t.month
left join rates_monthly r1 on r1.month = t.month and r1.geo = coalesce(fx.geo, 'EA20')
left join rates_eom     r2 on r2.month = t.month and r2.geo = coalesce(fx.geo, 'EA20')
left join energy_monthly e on e.month = t.month and e.geo = coalesce(fx.geo, 'EA20')
where t.month <= date_trunc('month', {{ month_cutoff() }}::date)


