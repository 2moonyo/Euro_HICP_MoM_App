{{ config(materialized='table') }}

with base as (
  select
    {{ first_of_month('ts_date') }} as month,
    geo,
    series_id,
    value as hicp_index
  from {{ source('silver','eurostat_hicp') }}
)
select * from base
