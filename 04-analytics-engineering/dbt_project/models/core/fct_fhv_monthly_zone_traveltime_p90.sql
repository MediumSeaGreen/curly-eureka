WITH trip_durations AS (
    SELECT
        pickup_locationid,
        dropoff_locationid,
        pickup_zone,
        dropoff_zone,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM
        {{ ref('dim_fhv_trips') }}
),
p90_trip_durations AS (
    SELECT
        year,
        month,
        pickup_zone,
        dropoff_zone,
        PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY year, month, pickup_locationid, dropoff_locationid) AS p90_trip_duration
    FROM
        trip_durations
),
ranked AS (
  SELECT
    year,
    month,
    pickup_zone,
    dropoff_zone,
    p90_trip_duration,
    DENSE_RANK() OVER (PARTITION BY pickup_zone ORDER BY p90_trip_duration DESC) AS rnk
  FROM p90_trip_durations
  WHERE pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
    AND year = 2019
    AND month = 11
)
SELECT distinct
  year,
  month,
  pickup_zone,
  dropoff_zone,
  p90_trip_duration
FROM ranked
WHERE rnk = 2

