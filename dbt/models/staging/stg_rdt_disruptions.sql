with source as (
    select * from {{ source('raw', 'rdt_disruptions') }}
),

cleaned as (
    select
        cast(rdt_id as string) as disruption_id,
        cast(ns_lines as string) as ns_lines,
        cast(rdt_lines as string) as rdt_lines,
        cast(rdt_station_names as string) as station_names,
        cast(rdt_station_codes as string) as station_codes_raw,
        cast(cause_nl as string) as cause_nl,
        cast(cause_en as string) as cause,
        cast(statistical_cause_en as string) as statistical_cause,
        cast(cause_group as string) as cause_group,
        cast(start_time as timestamp) as start_ts,
        cast(end_time as timestamp) as end_ts,
        cast(duration_minutes as int64) as duration_minutes,

        -- Extract service_date from start_time
        date(start_time) as service_date,

        -- Split station codes into array
        split(trim(rdt_station_codes), ',') as affected_station_codes,
        array_length(split(trim(rdt_station_codes), ',')) as stations_affected_count,

        -- Metadata
        cast(_source as string) as data_source

    from source
    where rdt_id is not null
)

select * from cleaned
