{{
    config(
        materialized='table',
        partition_by={
            "field": "service_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['location_id']
    )
}}

with daily_traffic as (
    select * from {{ ref('int_ndw_traffic_daily') }}
),

locations as (
    select location_id, corridor_id
    from {{ ref('dim_ndw_locations') }}
),

baseline as (
    select
        location_id,
        avg(avg_speed_kmh) as baseline_speed_kmh
    from daily_traffic
    group by location_id
)

select
    dt.location_id,
    dt.service_date,
    l.corridor_id,
    dt.avg_speed_kmh,
    dt.total_vehicle_count,
    dt.congestion_minutes,
    round(
        safe_divide(dt.avg_speed_kmh, b.baseline_speed_kmh) * 100.0,
        2
    ) as speed_vs_baseline_pct
from daily_traffic dt
left join locations l on dt.location_id = l.location_id
left join baseline b on dt.location_id = b.location_id
