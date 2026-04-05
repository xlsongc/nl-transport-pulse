with source as (
    select * from {{ source('raw', 'rdt_services') }}
),

cleaned as (
    select
        -- Service-level
        cast(service_rdt_id as string) as service_rdt_id,
        cast(service_date as date) as service_date,
        cast(service_type as string) as service_type,
        cast(company as string) as company,
        cast(train_number as string) as train_number,
        cast(completely_cancelled as boolean) as completely_cancelled,
        cast(partly_cancelled as boolean) as partly_cancelled,
        cast(max_delay as int64) as max_delay,

        -- Stop-level
        cast(stop_rdt_id as string) as stop_rdt_id,
        cast(station_code as string) as station_code,
        cast(station_name as string) as station_name,
        cast(arrival_time as timestamp) as arrival_ts,
        cast(arrival_delay as int64) as arrival_delay_seconds,
        cast(arrival_cancelled as boolean) as arrival_cancelled,
        cast(departure_time as timestamp) as departure_ts,
        cast(departure_delay as int64) as departure_delay_seconds,
        cast(departure_cancelled as boolean) as departure_cancelled,
        cast(platform_change as boolean) as platform_change,
        cast(planned_platform as string) as planned_platform,
        cast(actual_platform as string) as actual_platform,

        -- Computed
        round(cast(departure_delay as float64) / 60, 1) as departure_delay_minutes,
        round(cast(arrival_delay as float64) / 60, 1) as arrival_delay_minutes,

        -- Metadata
        cast(_source as string) as data_source,

        -- Surrogate key
        cast(stop_rdt_id as string) as departure_id

    from source
    where station_code is not null
      and station_code != ''
)

select * from cleaned
