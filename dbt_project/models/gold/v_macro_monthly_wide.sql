{{ config(
    materialized = 'view',
    tags = ['dashboard', 'post_ml']
) }}

with macro_raw as (

    select
        month,  
        fx_usd_eur_avg,
        brent_crude_usd,
        nat_gas_eu_usd,
        policy_rate_eom
    from {{ ref('fact_macro') }}

),

monthly as (

    select
        date_trunc('month', month)::date as ref_month,

        -- Monthly averages
        avg(fx_usd_eur_avg)   as fx_usd_eur_avg_m,
        avg(brent_crude_usd) as brent_crude_m,
        avg(nat_gas_eu_usd) as nat_gas_m,

        -- LAST-KNOWN policy rate per month: using max as a proxy
        max(policy_rate_eom)  as policy_rate_eom_m

    from macro_raw
    group by 1

),

with_rolls as (

    select
        ref_month,

        -- FX level and rolls
        fx_usd_eur_avg_m,
        avg(fx_usd_eur_avg_m) over(
            order by ref_month
            rows between 2 preceding and current row
        ) as fx_ma3,
        avg(fx_usd_eur_avg_m) over(
            order by ref_month
            rows between 11 preceding and current row
        ) as fx_ma12,

        -- Brent Crude level and rolls
        brent_crude_m,
        avg(brent_crude_m) over(
            order by ref_month
            rows between 2 preceding and current row
        ) as brent_crude_ma3,
        avg(brent_crude_m) over(
            order by ref_month
            rows between 11 preceding and current row
        ) as brent_crude_ma12,

        -- Natural Gas level and rolls
        nat_gas_m,
        avg(nat_gas_m) over(
            order by ref_month
            rows between 2 preceding and current row
        ) as nat_gas_ma3,
        avg(nat_gas_m) over(
            order by ref_month
            rows between 11 preceding and current row
        ) as nat_gas_ma12,

        -- Policy rate
        policy_rate_eom_m

    from monthly

)

select *
from with_rolls
order by ref_month