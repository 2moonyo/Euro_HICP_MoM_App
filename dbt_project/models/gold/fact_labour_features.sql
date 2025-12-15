{{ config(materialized='table') }}

with hicp as ( 
  select * from {{ ref('fact_hicp') }} 
),
lab as ( 
  select * from {{ ref('fact_labour') }} 
),

joined as (
  select
    h.month,
    h.geo,
    h.series_id,
    h.hicp_index,

    lab.unemployment_rate,
    lab.labour_cost_index,
    lab.hours_worked_index,
    lab.job_vacancy_rate
  from hicp h
  left join lab
    on lab.month = h.month
   and lab.geo   = h.geo
),

lags as (
  select
    *,
    -- UNEMPLOYMENT (monthly series)
    lag(unemployment_rate, 1)   over (partition by geo order by month) as unemp_lag_1m,
    lag(unemployment_rate, 3)   over (partition by geo order by month) as unemp_lag_3m,
    lag(unemployment_rate, 7)   over (partition by geo order by month) as unemp_lag_7m,
    lag(unemployment_rate, 10)  over (partition by geo order by month) as unemp_lag_10m,
    lag(unemployment_rate, 12)  over (partition by geo order by month) as unemp_lag_12m,

    -- LCI (quarterly series expanded to monthly)
    lag(labour_cost_index, 1)   over (partition by geo order by month) as lci_lag_1m,
    lag(labour_cost_index, 3)   over (partition by geo order by month) as lci_lag_3m,
    lag(labour_cost_index, 7)   over (partition by geo order by month) as lci_lag_7m,
    lag(labour_cost_index, 10)  over (partition by geo order by month) as lci_lag_10m,
    lag(labour_cost_index, 12)  over (partition by geo order by month) as lci_lag_12m,

    -- HOURS WORKED (quarterly expanded to monthly)
    lag(hours_worked_index, 1)  over (partition by geo order by month) as hw_lag_1m,
    lag(hours_worked_index, 3)  over (partition by geo order by month) as hw_lag_3m,
    lag(hours_worked_index, 7)  over (partition by geo order by month) as hw_lag_7m,
    lag(hours_worked_index, 10) over (partition by geo order by month) as hw_lag_10m,
    lag(hours_worked_index, 12) over (partition by geo order by month) as hw_lag_12m,

    -- VACANCIES (quarterly expanded to monthly)
    lag(job_vacancy_rate, 1)    over (partition by geo order by month) as jv_lag_1m,
    lag(job_vacancy_rate, 3)    over (partition by geo order by month) as jv_lag_3m,
    lag(job_vacancy_rate, 7)    over (partition by geo order by month) as jv_lag_7m,
    lag(job_vacancy_rate, 10)   over (partition by geo order by month) as jv_lag_10m,
    lag(job_vacancy_rate, 12)   over (partition by geo order by month) as jv_lag_12m

  from joined
),

changes as (
  select
    *,
    -- UNEMPLOYMENT CHANGE METRICS
    case 
      when unemp_lag_1m is not null 
      then unemployment_rate / unemp_lag_1m - 1 
    end as unemp_mom,

    case 
      when unemp_lag_12m is not null 
      then unemployment_rate / unemp_lag_12m - 1 
    end as unemp_yoy,

    -- LCI CHANGE METRICS (still using 1 and 12 months as “qoq”/“yoy” proxies)
    case 
      when lci_lag_1m is not null 
      then labour_cost_index / lci_lag_1m - 1 
    end as lci_mom,

    case 
      when lci_lag_12m is not null 
      then labour_cost_index / lci_lag_12m - 1 
    end as lci_yoy
  from lags
)

select *
from changes
