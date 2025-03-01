with valid_trips as (
    select *
    from {{ ref('fact_trips') }}
    where fare_amount > 0
      and trip_distance > 0
      and payment_type_description in ('Cash', 'Credit card')
),
monthly_fare_percentiles as (
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        percentile_cont(fare_amount, 0.97) over (partition by service_type, extract(year from pickup_datetime), extract(month from pickup_datetime)) as fare_p97,
        percentile_cont(fare_amount, 0.95) over (partition by service_type, extract(year from pickup_datetime), extract(month from pickup_datetime)) as fare_p95,
        percentile_cont(fare_amount, 0.90) over (partition by service_type, extract(year from pickup_datetime), extract(month from pickup_datetime)) as fare_p90
    from valid_trips
)

select distinct
    service_type,
    year,
    month,
    fare_p97,
    fare_p95,
    fare_p90
from monthly_fare_percentiles
where year = 2020 and month = 4