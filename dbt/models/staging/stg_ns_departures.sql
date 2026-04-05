with source as (
    select * from {{ source('raw', 'ns_departures') }}
    {% if var('is_test_run', false) %}
    where _service_date >= cast(date_sub(current_date(), interval 30 day) as string)
    {% endif %}
),

cleaned as (
    select
        -- Metadata injected at ingestion
        cast(_station_code as string) as station_code,
        cast(_service_date as date) as service_date,
        cast(_source as string) as data_source,

        -- Core departure fields from raw API
        cast(direction as string) as direction,
        cast(name as string) as train_name,
        timestamp(plannedDateTime) as planned_departure_ts,
        timestamp(actualDateTime) as actual_departure_ts,
        cast(trainCategory as string) as train_category,
        cast(cancelled as boolean) as is_cancelled,
        cast(departureStatus as string) as departure_status,

        -- Track info
        cast(plannedTrack as string) as planned_track,
        cast(actualTrack as string) as actual_track,

        -- Product details (stored as JSON string in raw)
        cast(json_extract_scalar(product, '$.number') as string) as train_number,
        cast(json_extract_scalar(product, '$.operatorName') as string) as operator_name,
        cast(json_extract_scalar(product, '$.longCategoryName') as string) as category_long_name,

        -- Computed fields
        timestamp_diff(
            timestamp(actualDateTime),
            timestamp(plannedDateTime),
            MINUTE
        ) as delay_minutes,

        -- Surrogate key
        concat(
            cast(_station_code as string), '_',
            cast(_service_date as string), '_',
            cast(plannedDateTime as string), '_',
            cast(direction as string)
        ) as departure_id

    from source
    where cancelled is null or cast(cancelled as boolean) = false
)

select * from cleaned
