with departures as (
    select * from {{ ref('int_departures_combined') }}
),

disruptions as (
    select * from {{ ref('stg_ns_disruptions') }}
),

departure_stats as (
    select
        station_code,
        service_date,
        count(*) as total_departures,
        avg(delay_minutes) as avg_delay_min,
        max(delay_minutes) as max_delay_min,
        countif(delay_minutes <= 1.0) / count(*) * 100.0 as pct_on_time,
        countif(delay_minutes > 15.0) / count(*) * 100.0 as severe_delay_share
    from departures
    group by station_code, service_date
),

disruption_counts as (
    select
        station_code,
        service_date,
        count(distinct disruption_id) as disruption_count
    from disruptions,
    unnest(affected_station_codes) as station_code
    group by station_code, service_date
)

select
    ds.station_code,
    ds.service_date,
    ds.total_departures,
    round(ds.avg_delay_min, 2) as avg_delay_min,
    round(ds.max_delay_min, 2) as max_delay_min,
    round(ds.pct_on_time, 2) as pct_on_time,
    round(ds.severe_delay_share, 2) as severe_delay_share,
    coalesce(dc.disruption_count, 0) as disruption_count
from departure_stats ds
left join disruption_counts dc
    on ds.station_code = dc.station_code
    and ds.service_date = dc.service_date
