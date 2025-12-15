{{ config(materialized='table') }}

with months as (
  select month
  from {{ ref('dim_time') }}
),

-- 1) Get all unique geographies
geos as (
  select distinct coalesce(geo, 'EA20') as geo from {{ source('silver','eurostat_labour_unemployment') }}
  union
  select distinct coalesce(geo, 'EA20') as geo from {{ source('silver','labour_cost_index') }}
  union
  select distinct coalesce(geo, 'EA20') as geo from {{ source('silver','labour_hours_worked') }}
  union
  select distinct coalesce(geo, 'EA20') as geo from {{ source('silver','labour_job_vacancy_rate') }}
),

-- 2) Create a "Spine": Every Month for Every Geography
spine as (
  select
    m.month,
    g.geo
  from months m
  cross join geos g
),

-- 3) Prepare Source Data
unemployment as (
  select
    date_trunc('month', ts_date) as month,
    coalesce(geo, 'EA20') as geo,
    value as unemployment_rate
  from {{ source('silver','eurostat_labour_unemployment') }}
),

lci_q as (
  select
    ts_date as quarter_start,
    coalesce(geo, 'EA20') as geo,
    value as labour_cost_index
  from {{ source('silver','labour_cost_index') }}
),

hw_q as (
  select
    ts_date as quarter_start,
    coalesce(geo, 'EA20') as geo,
    value as hours_worked_index
  from {{ source('silver','labour_hours_worked') }}
),

jv_q as (
  select
    ts_date as quarter_start,
    coalesce(geo, 'EA20') as geo,
    value as job_vacancy_rate
  from {{ source('silver','labour_job_vacancy_rate') }}
),

-- 4) Join everything to the Spine
joined as (
  select
    s.month,
    s.geo,
    u.unemployment_rate,
    l.labour_cost_index,
    h.hours_worked_index,
    j.job_vacancy_rate
  from spine s
  left join unemployment u
    on s.month = u.month
   and s.geo = u.geo
  left join lci_q l
    on s.month >= l.quarter_start
   and s.month <  date_add(l.quarter_start, INTERVAL 3 MONTH)
   and s.geo = l.geo
  left join hw_q h
    on s.month >= h.quarter_start
   and s.month <  date_add(h.quarter_start, INTERVAL 3 MONTH)
   and s.geo = h.geo
  left join jv_q j
    on s.month >= j.quarter_start
   and s.month <  date_add(j.quarter_start, INTERVAL 3 MONTH)
   and s.geo = j.geo
)

select *
from joined
where month <= date_trunc('month', {{ month_cutoff() }}::date)
