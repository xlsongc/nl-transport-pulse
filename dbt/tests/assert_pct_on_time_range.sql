-- Ensure pct_on_time values are always between 0 and 100
select
    station_code,
    service_date,
    pct_on_time
from {{ ref('fct_train_performance') }}
where pct_on_time < 0 or pct_on_time > 100
