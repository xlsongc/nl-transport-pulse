with source as (
    select * from {{ source('raw', 'knmi_weather') }}
),

cleaned as (
    select
        cast(station_code as string) as knmi_station_code,
        cast(station_region as string) as station_region,
        cast(service_date as date) as service_date,

        -- Temperature (already converted to °C in ingestion)
        cast(avg_temp_c as float64) as avg_temp_c,
        cast(min_temp_c as float64) as min_temp_c,
        cast(max_temp_c as float64) as max_temp_c,

        -- Precipitation
        cast(precipitation_mm as float64) as precipitation_mm,
        cast(precipitation_duration_h as float64) as precipitation_duration_h,

        -- Wind
        cast(avg_wind_speed_ms as float64) as avg_wind_speed_ms,
        cast(max_wind_gust_ms as float64) as max_wind_gust_ms,
        cast(wind_direction_deg as int64) as wind_direction_deg,

        -- Derived: wind in km/h (more intuitive for analysis)
        round(cast(avg_wind_speed_ms as float64) * 3.6, 1) as avg_wind_speed_kmh,
        round(cast(max_wind_gust_ms as float64) * 3.6, 1) as max_wind_gust_kmh,

        -- Weather severity flags
        cast(avg_wind_speed_ms as float64) > 20.8 as is_stormy,        -- >75 km/h (Beaufort 9+)
        cast(precipitation_mm as float64) > 10.0 as is_heavy_rain,     -- >10mm/day
        cast(max_temp_c as float64) > 35.0 as is_extreme_heat,         -- >35°C
        cast(min_temp_c as float64) < -5.0 as is_freezing,             -- <-5°C

        cast(_source as string) as data_source

    from source
    where station_code is not null
)

select * from cleaned
