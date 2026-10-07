select
    ride_id,
    status,
    cancelled_by,
    cancel_reason
from {{ ref('fact_ride') }}
where status = 'cancelled'
  and (
      cancelled_by is null or trim(cancelled_by) = ''
      or cancel_reason is null or trim(cancel_reason) = ''
  )
