select
    ride_id,
    status,
    distance_km,
    fare_amount,
    total_amount
from {{ ref('fact_ride') }}
where status = 'completed'
  and (
      distance_km is null or distance_km <= 0
      or fare_amount is null or fare_amount <= 0
      or total_amount is null or total_amount <= 0
  )
