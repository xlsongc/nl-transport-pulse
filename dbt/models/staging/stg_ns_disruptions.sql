with source as (
    select * from {{ source('raw', 'ns_disruptions') }}
    {% if var('is_test_run', false) %}
    where _service_date >= cast(date_sub(current_date(), interval 30 day) as string)
    {% endif %}
),

-- Extract station codes from publicationSections JSON (two-level unnest)
station_codes_extracted as (
    select
        id,
        _service_date,
        array(
            select distinct cast(json_extract_scalar(station, '$.stationCode') as string)
            from unnest(json_extract_array(publicationSections, '$')) as section_json,
                 unnest(json_extract_array(section_json, '$.section.stations')) as station
            where json_extract_scalar(station, '$.stationCode') is not null
        ) as affected_station_codes
    from source
),

-- Extract first cause from timespans
causes_extracted as (
    select
        id,
        _service_date,
        (
            select cast(json_extract_scalar(ts, '$.cause.label') as string)
            from unnest(json_extract_array(timespans, '$')) as ts
            where json_extract_scalar(ts, '$.cause.label') is not null
            limit 1
        ) as cause
    from source
),

cleaned as (
    select
        -- Core fields
        cast(s.id as string) as disruption_id,
        cast(s._service_date as date) as service_date,
        cast(s._source as string) as data_source,
        cast(s.type as string) as disruption_type,
        cast(s.title as string) as title,
        cast(s.isActive as boolean) as is_active,
        cast(s.local as boolean) as is_local,

        -- Timestamps
        timestamp(s.start) as start_ts,
        timestamp(s.`end`) as end_ts,
        timestamp(s.registrationTime) as registration_ts,

        -- Computed
        timestamp_diff(timestamp(s.`end`), timestamp(s.start), MINUTE) as duration_minutes,

        -- Impact
        cast(json_extract_scalar(s.impact, '$.value') as int64) as impact_score,

        -- From subqueries
        c.cause,
        sc.affected_station_codes,
        array_length(sc.affected_station_codes) as stations_affected_count

    from source s
    left join station_codes_extracted sc
        on s.id = sc.id and s._service_date = sc._service_date
    left join causes_extracted c
        on s.id = c.id and s._service_date = c._service_date
),

-- Deduplicate: multiple polls per day capture the same disruption.
-- Keep the latest snapshot (most up-to-date end_ts/is_active) per disruption.
deduped as (
    select *,
        row_number() over (
            partition by disruption_id, service_date
            order by registration_ts desc nulls last
        ) as _rn
    from cleaned
)

select * except(_rn) from deduped where _rn = 1
