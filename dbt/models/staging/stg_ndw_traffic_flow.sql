{% set ndw_relation = adapter.get_relation(
    database=target.database,
    schema=var('raw_dataset', 'raw_nl_transport'),
    identifier='ndw_traffic_flow'
) %}

{% if ndw_relation %}
with source as (
    select * from {{ source('raw', 'ndw_traffic_flow') }}
    {% if var('is_test_run', false) %}
    where service_date >= date_sub(current_date(), interval 30 day)
    {% endif %}
),

cleaned as (
    select
        cast(location_id as string) as location_id,
        cast(measurement_ts as timestamp) as measurement_ts,
        cast(service_date as date) as service_date,
        cast(avg_speed_kmh as float64) as avg_speed_kmh,
        cast(vehicle_count as int64) as vehicle_count
    from source
)

select * from cleaned
{% else %}
-- NDW raw table not loaded yet — return empty result with correct schema
select
    cast(null as string) as location_id,
    cast(null as timestamp) as measurement_ts,
    cast(null as date) as service_date,
    cast(null as float64) as avg_speed_kmh,
    cast(null as int64) as vehicle_count
from unnest([]) as empty
{% endif %}
