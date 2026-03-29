with source as (
    select * from {{ source('raw', 'ns_disruptions') }}
    {% if var('is_test_run', false) %}
    where service_date >= date_sub(current_date(), interval 30 day)
    {% endif %}
),

cleaned as (
    select
        cast(disruption_id as string) as disruption_id,
        cast(service_date as date) as service_date,
        cast(title as string) as title,
        cast(is_active as boolean) as is_active,
        cast(start_timestamp as timestamp) as start_ts,
        cast(end_timestamp as timestamp) as end_ts,
        cast(duration_minutes as float64) as duration_minutes,
        cast(cause as string) as cause,
        affected_station_codes,
        cast(stations_affected_count as int64) as stations_affected_count
    from source
)

select * from cleaned
