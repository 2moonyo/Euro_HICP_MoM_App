{{ config(
    materialized = 'view',
    tags = ['dashboard', 'post_ml']
) }}

with actuals as (

    select
        month,
        hicp_index as index_value,
        'actual'::varchar as source
    from {{ ref('fact_hicp') }}
    where geo = 'EA20'

),

preds as (

    select
        month as month,
        y_nowcast as index_value,
        'nowcast'::varchar as source
    from gold.nowcast_output
    where geo = 'EA20'

),

combined as (

    select * from actuals
    union all
    select * from preds

),

-- Get 12-month-ago actuals for YoY calculation
actuals_for_yoy as (
    select
        month,
        index_value as index_12m_ago
    from actuals
)

select
    c.month,
    c.index_value,
    c.source,

    -- YoY: Compare current index with actual from 12 months ago
    (c.index_value / a.index_12m_ago - 1) as hicp_yoy,

    -- 3-month rolling average of index (partitioned by source)
    avg(c.index_value) over(
        partition by c.source
        order by c.month
        rows between 2 preceding and current row
    ) as hicp_idx_ma3,

    -- 12-month rolling average of index (partitioned by source)
    avg(c.index_value) over(
        partition by c.source
        order by c.month
        rows between 11 preceding and current row
    ) as hicp_idx_ma12

from combined c
left join actuals_for_yoy a
    on a.month = c.month - interval '12 months'
order by c.month, c.source