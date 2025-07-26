{{ config(materialized = 'table', file_format='parquet') }}

with base_dates as (
  {{ dbt.date_spine(
      'day',
      "date('2015-01-01')",
      "current_date() + interval '30' day"
    )
  }}
)

select
  cast(date_day as date) as date_day
from base_dates
where date_day >= dateadd(year, -5, current_date())  -- last 5 years
  and date_day <= dateadd(day, 30, current_date())