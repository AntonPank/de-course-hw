{{ config(
    materialized='incremental',
    unique_key='ride_id',
    incremental_strategy='delete+insert'
) }}

with affected_rides as (

    select distinct ride_id
    from {{ ref('events') }}

    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}

),

all_events as (

    select e.*
    from {{ ref('events') }} as e
    inner join affected_rides as a
        on e.ride_id = a.ride_id

),

rides_aggregated as (

    select
        ride_id,

        -- ride_requested
        max(payload #>> '{rider,id}')
            filter (where event_type = 'ride_requested') as rider_id,

        max(payload #>> '{rider,platform}')
            filter (where event_type = 'ride_requested') as rider_platform,

        max(payload #>> '{rider,app_version}')
            filter (where event_type = 'ride_requested') as app_version,

        max(payload #>> '{requested_vehicle}')
            filter (where event_type = 'ride_requested') as requested_vehicle,

        max((payload #>> '{surge_estimate}')::numeric)
            filter (where event_type = 'ride_requested') as surge_estimate,

        max((payload #>> '{pickup,zone_id}')::int)
            filter (where event_type = 'ride_requested') as requested_pickup_zone_id,

        max((payload #>> '{dropoff,zone_id}')::int)
            filter (where event_type = 'ride_requested') as requested_dropoff_zone_id,

        -- driver lifecycle
        max(payload #>> '{driver,id}')
            filter (where event_type = 'ride_accepted') as accepted_driver_id,

        max(payload #>> '{driver,id}')
            filter (where event_type = 'ride_started') as started_driver_id,

        max(payload #>> '{driver,id}')
            filter (where event_type = 'ride_completed') as completed_driver_id,

        -- actual zones
        max((payload #>> '{pickup,zone_id}')::int)
            filter (where event_type = 'ride_started') as actual_pickup_zone_id,

        max((payload #>> '{dropoff,zone_id}')::int)
            filter (where event_type = 'ride_completed') as actual_dropoff_zone_id,

        -- timestamps
        max(occurred_at)
            filter (where event_type = 'ride_requested') as requested_at,

        max(occurred_at)
            filter (where event_type = 'ride_accepted') as accepted_at,

        max(occurred_at)
            filter (where event_type = 'ride_started') as started_at,

        max(occurred_at)
            filter (where event_type = 'ride_completed') as completed_at,

        max(occurred_at)
            filter (where event_type = 'ride_cancelled') as cancelled_at,

        max(occurred_at)
            filter (where event_type = 'payment_captured') as paid_at,

        -- completion
        max((payload #>> '{distance_km}')::numeric(8, 2))
            filter (where event_type = 'ride_completed') as distance_km,

        max((payload #>> '{fare,amount}')::numeric(10, 2))
            filter (where event_type = 'ride_completed') as fare_amount,

        max((payload #>> '{fare,surge_multiplier}')::numeric(4, 2))
            filter (where event_type = 'ride_completed') as surge_multiplier,

        max((payload #>> '{fare,tolls}')::numeric(10, 2))
            filter (where event_type = 'ride_completed') as tolls_amount,

        max((payload #>> '{fare,tip}')::numeric(10, 2))
            filter (where event_type = 'ride_completed') as tip_amount,

        max((payload #>> '{fare,total}')::numeric(10, 2))
            filter (where event_type = 'ride_completed') as total_amount,

        max(payload #>> '{fare,currency}')
            filter (where event_type = 'ride_completed') as currency,

        -- cancellation
        max(payload #>> '{cancelled_by}')
            filter (where event_type = 'ride_cancelled') as cancelled_by,

        max(payload #>> '{reason}')
            filter (where event_type = 'ride_cancelled') as cancel_reason,

        max(payload #>> '{stage}')
            filter (where event_type = 'ride_cancelled') as cancel_stage,

        -- payment
        max(payload #>> '{payment,method}')
            filter (where event_type = 'payment_captured') as payment_method,

        max(payload #>> '{payment,psp_reference}')
            filter (where event_type = 'payment_captured') as psp_reference,

        max((payload #>> '{payment,amount}')::numeric(10, 2))
            filter (where event_type = 'payment_captured') as payment_amount,

        max(_ingested_at) as _ingested_at

    from all_events
    group by ride_id

),

final as (

    select
        ride_id,
        rider_id,
        rider_platform,
        app_version,
        requested_vehicle,

        coalesce(
            completed_driver_id,
            started_driver_id,
            accepted_driver_id
        ) as driver_id,

        -- status
        case
            when completed_at is not null then 'completed'
            when cancelled_at is not null then 'cancelled'
            when started_at is not null then 'in_progress'
            when accepted_at is not null then 'accepted'
            else 'requested'
        end as status,

        requested_at,
        accepted_at,
        started_at,
        completed_at,
        cancelled_at,
        paid_at,

        coalesce(actual_pickup_zone_id, requested_pickup_zone_id) as pickup_zone_id,
        coalesce(actual_dropoff_zone_id, requested_dropoff_zone_id) as dropoff_zone_id,

        -- durations
        extract(epoch from (accepted_at - requested_at))::int as wait_seconds,

        extract(epoch from (completed_at - started_at))::int as trip_seconds,

        surge_estimate,
        distance_km,
        surge_multiplier,
        fare_amount,
        tolls_amount,
        tip_amount,
        total_amount,
        currency,

        cancelled_by,
        cancel_reason,
        cancel_stage,

        payment_method,
        psp_reference,
        payment_amount,

        _ingested_at,
        '{{ run_started_at }}'::timestamptz as _loaded_at

    from rides_aggregated

    -- a ride becomes a Silver ride only once ride_requested exists
    where requested_at is not null

)

select * from final
