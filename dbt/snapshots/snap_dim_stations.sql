{% snapshot snap_dim_stations %}

{{
    config(
        target_schema='core_nl_transport',
        unique_key="station_code || '-' || cast(corridor_id as string)",
        strategy='check',
        check_cols=['station_name', 'corridor_id', 'corridor_name', 'lat', 'lon'],
    )
}}

select * from {{ ref('dim_stations') }}

{% endsnapshot %}
