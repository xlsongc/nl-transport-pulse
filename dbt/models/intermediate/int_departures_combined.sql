-- depends_on: {{ ref('stg_rdt_services') }}

{{
    config(
        materialized='table'
    )
}}

{#
    Union live NS API departures with historical RDT archive data.
    Priority: RDT archive is more complete (full-day coverage vs single snapshot).
    For overlapping dates, prefer RDT data and deduplicate.
#}

{% set rdt_relation = adapter.get_relation(
    database=target.database,
    schema='raw_nl_transport',
    identifier='rdt_services'
) %}

-- Live API departures (always available)
with api_departures as (
    select
        departure_id,
        station_code,
        service_date,
        direction,
        planned_departure_ts,
        actual_departure_ts,
        delay_minutes,
        is_cancelled,
        train_number,
        train_category,
        operator_name,
        data_source
    from {{ ref('stg_ns_departures') }}
),

{% if rdt_relation is not none %}
-- Historical archive departures (when available)
rdt_departures as (
    select
        departure_id,
        station_code,
        service_date,
        cast(null as string) as direction,
        departure_ts as planned_departure_ts,
        timestamp_add(departure_ts, interval cast(coalesce(departure_delay_seconds, 0) as int64) second) as actual_departure_ts,
        departure_delay_minutes as delay_minutes,
        coalesce(completely_cancelled, departure_cancelled) as is_cancelled,
        train_number,
        service_type as train_category,
        company as operator_name,
        data_source
    from {{ ref('stg_rdt_services') }}
    where departure_ts is not null
),

-- Combine: prefer RDT for dates that have both sources
combined as (
    select * from rdt_departures
    union all
    select * from api_departures
    where service_date not in (select distinct service_date from rdt_departures)
)

select * from combined

{% else %}
-- RDT data not loaded yet — use API data only
combined as (
    select * from api_departures
)

select * from combined

{% endif %}
