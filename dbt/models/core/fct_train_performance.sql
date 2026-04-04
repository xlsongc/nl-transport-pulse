{{
    config(
        materialized='incremental',
        unique_key=['station_code', 'service_date'],
        partition_by={
            "field": "service_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['station_code']
    )
}}

with delays as (
    select * from {{ ref('int_train_delays_daily') }}
),

stations as (
    select distinct station_code, corridor_id, corridor_name
    from {{ ref('dim_stations') }}
)

select
    d.station_code,
    d.service_date,
    s.corridor_id,
    s.corridor_name,
    d.total_departures,
    d.avg_delay_min,
    d.max_delay_min,
    d.pct_on_time,
    d.severe_delay_share,
    d.disruption_count
from delays d
left join stations s
    on d.station_code = s.station_code
{% if is_incremental() %}
where d.service_date > (select max(service_date) from {{ this }})
{% endif %}
