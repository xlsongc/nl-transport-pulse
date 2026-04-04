with ndw_mapping as (
    select * from {{ ref('station_ndw_mapping') }}
),

corridor_mapping as (
    select distinct station_code, corridor_id
    from {{ ref('station_corridor_mapping') }}
)

select
    n.ndw_location_id as location_id,
    n.road_name,
    cast(n.ndw_lat as float64) as lat,
    cast(n.ndw_lon as float64) as lon,
    n.station_code as nearest_station_code,
    c.corridor_id,
    cast(n.distance_km as float64) as distance_km
from ndw_mapping n
left join corridor_mapping c
    on n.station_code = c.station_code
