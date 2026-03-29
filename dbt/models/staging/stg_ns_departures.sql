with source as (
    select * from {{ source('raw', 'ns_departures') }}
    {% if var('is_test_run', false) %}
    where service_date >= date_sub(current_date(), interval 30 day)
    {% endif %}
),

cleaned as (
    select
        cast(station_code as string) as station_code,
        cast(service_date as date) as service_date,
        cast(direction as string) as direction,
        cast(planned_departure_ts as timestamp) as planned_departure_ts,
        cast(actual_departure_ts as timestamp) as actual_departure_ts,
        cast(delay_minutes as float64) as delay_minutes,
        cast(train_category as string) as train_category,
        concat(
            cast(station_code as string), '_',
            cast(planned_departure_ts as string)
        ) as departure_id
    from source
)

select * from cleaned
