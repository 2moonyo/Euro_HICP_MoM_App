{% macro first_of_month(col) -%}
date_trunc('month', {{ col }})::date
{%- endmacro %}

-- For live nowcasts, pass a cutoff date (e.g., current_date) to compute month-to-date aggregates.
{% macro month_cutoff() -%}
{{ var('month_cutoff', 'current_date') }}
{%- endmacro %}
