-- depends_on: {{ ref('stg_knmi_weather') }}
-- depends_on: {{ ref('knmi_corridor_mapping') }}
-- depends_on: {{ ref('fct_road_traffic') }}
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

{% set weather_relation = adapter.get_relation(
    database=target.project,
    schema='raw_nl_transport',
    identifier='knmi_weather'
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

{% if weather_relation is not none %}
weather as (
    -- Average weather across KNMI stations mapped to each corridor
    select
        cm.corridor_id,
        w.service_date,
        round(avg(w.avg_temp_c), 1) as avg_temp_c,
        round(avg(w.min_temp_c), 1) as min_temp_c,
        round(avg(w.max_temp_c), 1) as max_temp_c,
        round(avg(w.precipitation_mm), 1) as precipitation_mm,
        round(avg(w.precipitation_duration_h), 1) as precipitation_duration_h,
        round(avg(w.avg_wind_speed_kmh), 1) as avg_wind_speed_kmh,
        round(max(w.max_wind_gust_kmh), 1) as max_wind_gust_kmh,
        max(case when w.is_stormy then 1 else 0 end) = 1 as is_stormy,
        max(case when w.is_heavy_rain then 1 else 0 end) = 1 as is_heavy_rain
    from {{ ref('stg_knmi_weather') }} w
    inner join {{ ref('knmi_corridor_mapping') }} cm
        on w.knmi_station_code = cast(cm.knmi_station_code as string)
    group by cm.corridor_id, w.service_date
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
    round(r.avg_speed_vs_baseline_pct, 2) as road_speed_vs_baseline_pct,
    {% else %}
    cast(null as float64) as avg_road_speed_kmh,
    cast(null as int64) as total_vehicle_count,
    cast(null as int64) as total_congestion_minutes,
    cast(null as float64) as road_speed_vs_baseline_pct,
    {% endif %}
    {% if weather_relation is not none %}
    wx.avg_temp_c,
    wx.min_temp_c,
    wx.max_temp_c,
    wx.precipitation_mm,
    wx.precipitation_duration_h,
    wx.avg_wind_speed_kmh,
    wx.max_wind_gust_kmh,
    wx.is_stormy,
    wx.is_heavy_rain
    {% else %}
    cast(null as float64) as avg_temp_c,
    cast(null as float64) as min_temp_c,
    cast(null as float64) as max_temp_c,
    cast(null as float64) as precipitation_mm,
    cast(null as float64) as precipitation_duration_h,
    cast(null as float64) as avg_wind_speed_kmh,
    cast(null as float64) as max_wind_gust_kmh,
    cast(null as boolean) as is_stormy,
    cast(null as boolean) as is_heavy_rain
    {% endif %}
from train t
{% if road_relation is not none %}
left join road r
    on t.corridor_id = r.corridor_id
    and t.service_date = r.service_date
{% endif %}
{% if weather_relation is not none %}
left join weather wx
    on t.corridor_id = wx.corridor_id
    and t.service_date = wx.service_date
{% endif %}
left join dates d
    on t.service_date = d.date
where t.corridor_id is not null
