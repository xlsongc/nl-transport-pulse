{{
    config(
        materialized='table',
        partition_by={
            "field": "service_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['corridor_id']
    )
}}

{% set road_relation = adapter.get_relation(
    database=target.project,
    schema='core_nl_transport',
    identifier='fct_road_traffic'
) %}

with train as (
    select
        corridor_id,
        corridor_name,
        service_date,
        avg(pct_on_time) as avg_pct_on_time,
        avg(avg_delay_min) as avg_delay_min,
        sum(total_departures) as total_departures,
        sum(disruption_count) as disruption_count,
        avg(severe_delay_share) as avg_severe_delay_share
    from {{ ref('fct_train_performance') }}
    group by corridor_id, corridor_name, service_date
),

{% if road_relation is not none %}
road as (
    select
        corridor_id,
        service_date,
        avg(avg_speed_kmh) as avg_road_speed_kmh,
        sum(total_vehicle_count) as total_vehicle_count,
        sum(congestion_minutes) as total_congestion_minutes,
        avg(speed_vs_baseline_pct) as avg_speed_vs_baseline_pct
    from {{ ref('fct_road_traffic') }}
    group by corridor_id, service_date
),
{% endif %}

dates as (
    select * from {{ ref('dim_date') }}
)

select
    t.corridor_id,
    t.corridor_name,
    t.service_date,
    d.day_of_week,
    d.week_number,
    d.month,
    d.season,
    d.is_holiday,
    d.is_weekend,
    round(t.avg_pct_on_time, 2) as pct_on_time,
    round(t.avg_delay_min, 2) as avg_delay_min,
    t.total_departures,
    t.disruption_count,
    round(t.avg_severe_delay_share, 2) as severe_delay_share,
    {% if road_relation is not none %}
    round(r.avg_road_speed_kmh, 2) as avg_road_speed_kmh,
    r.total_vehicle_count,
    r.total_congestion_minutes,
    round(r.avg_speed_vs_baseline_pct, 2) as road_speed_vs_baseline_pct
    {% else %}
    cast(null as float64) as avg_road_speed_kmh,
    cast(null as int64) as total_vehicle_count,
    cast(null as int64) as total_congestion_minutes,
    cast(null as float64) as road_speed_vs_baseline_pct
    {% endif %}
from train t
{% if road_relation is not none %}
left join road r
    on t.corridor_id = r.corridor_id
    and t.service_date = r.service_date
{% endif %}
left join dates d
    on t.service_date = d.date
