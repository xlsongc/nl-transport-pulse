with traffic as (
    select * from {{ ref('stg_ndw_traffic_flow') }}
),

free_flow as (
    select
        location_id,
        percentile_cont(avg_speed_kmh, 0.85) over (partition by location_id) as free_flow_speed_kmh
    from traffic
),

free_flow_distinct as (
    select distinct
        location_id,
        free_flow_speed_kmh
    from free_flow
),

daily_stats as (
    select
        t.location_id,
        t.service_date,
        avg(t.avg_speed_kmh) as avg_speed_kmh,
        sum(t.vehicle_count) as total_vehicle_count,
        countif(t.avg_speed_kmh < ff.free_flow_speed_kmh * 0.5) * 15 as congestion_minutes
    from traffic t
    left join free_flow_distinct ff
        on t.location_id = ff.location_id
    group by t.location_id, t.service_date
)

select
    location_id,
    service_date,
    round(avg_speed_kmh, 2) as avg_speed_kmh,
    total_vehicle_count,
    congestion_minutes
from daily_stats
