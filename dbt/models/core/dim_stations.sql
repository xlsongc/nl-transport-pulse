with corridor_mapping as (
    select * from {{ ref('station_corridor_mapping') }}
),

stations as (
    select distinct
        station_code,
        station_name,
        corridor_id,
        corridor_name,
        cast(lat as float64) as lat,
        cast(lon as float64) as lon
    from corridor_mapping
)

select
    station_code,
    station_name,
    corridor_id,
    corridor_name,
    lat,
    lon
from stations
