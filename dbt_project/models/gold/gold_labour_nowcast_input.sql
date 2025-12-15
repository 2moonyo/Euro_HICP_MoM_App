{{ config(materialized='table') }}

with features as (
  select * 
  from {{ ref('fact_labour_features') }}
),

headline as (
  select *
  from features
  where series_id = 'CP00'   -- headline HICP
)

select
  month,
  geo,

  -- TARGET: HICP MoM
  case 
    when lag(hicp_index, 1) over (partition by geo order by month) is not null
    then hicp_index / lag(hicp_index, 1) over (partition by geo order by month) - 1
  end as hicp_mom,

  -- UNEMPLOYMENT
  unemployment_rate,
  unemp_lag_1m,
  unemp_lag_3m,
  unemp_lag_7m,
  unemp_lag_10m,
  unemp_lag_12m,
  unemp_mom,
  unemp_yoy,

  -- LABOUR COST INDEX
  labour_cost_index,
  lci_lag_1m,
  lci_lag_3m,
  lci_lag_7m,
  lci_lag_10m,
  lci_lag_12m,
  lci_mom,
  lci_yoy,

  -- HOURS WORKED
  hours_worked_index,
  hw_lag_1m,
  hw_lag_3m,
  hw_lag_7m,
  hw_lag_10m,
  hw_lag_12m,

  -- JOB VACANCY RATE
  job_vacancy_rate,
  jv_lag_1m,
  jv_lag_3m,
  jv_lag_7m,
  jv_lag_10m,
  jv_lag_12m

from headline
order by month, geo;
