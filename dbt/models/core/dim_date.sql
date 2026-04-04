with date_spine as (
    {{ generate_date_spine('2026-01-01', '2027-12-31') }}
),

holidays as (
    select
        cast(date as date) as holiday_date,
        holiday_name
    from {{ ref('nl_holidays') }}
)

select
    ds.date_day as date,
    extract(dayofweek from ds.date_day) as day_of_week,
    extract(isoweek from ds.date_day) as week_number,
    extract(month from ds.date_day) as month,
    case
        when extract(month from ds.date_day) in (3, 4, 5) then 'spring'
        when extract(month from ds.date_day) in (6, 7, 8) then 'summer'
        when extract(month from ds.date_day) in (9, 10, 11) then 'autumn'
        else 'winter'
    end as season,
    h.holiday_name is not null as is_holiday,
    extract(dayofweek from ds.date_day) in (1, 7) as is_weekend,
    h.holiday_name
from date_spine ds
left join holidays h
    on ds.date_day = h.holiday_date
