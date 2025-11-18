{{ config(materialized='table') }}

with months as (
  select *
  from generate_series(date '2000-01-01', date_trunc('month', {{ month_cutoff() }}::date), interval 1 month) as t(month)
)
select month from months
