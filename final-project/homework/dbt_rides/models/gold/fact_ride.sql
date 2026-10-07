{{
    config(
        materialized='incremental',
        unique_key='ride_id',
        incremental_strategy='delete+insert'
    )
}}

with rides as (
    select *
    from {{ ref('rides') }}
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}
),

enriched as (
    select
        r.*,
        dz.zone_key as pickup_zone_key,
        dz_drop.zone_key as dropoff_zone_key,
        dd.driver_key as matched_driver_key
    from rides as r
    left join {{ ref('dim_zone') }} as dz
        on r.pickup_zone_id = dz.zone_key
    left join {{ ref('dim_zone') }} as dz_drop
        on r.dropoff_zone_id = dz_drop.zone_key
    left join {{ ref('dim_driver') }} as dd
        on r.driver_id = dd.driver_key
),

final as (
    select
        ride_id,
        coalesce(pickup_zone_key, -1) as pickup_zone_key,
        coalesce(dropoff_zone_key, -1) as dropoff_zone_key,
        coalesce(matched_driver_key, 'unknown') as driver_key,
        rider_id,
        status,

        requested_at,
        accepted_at,
        started_at,
        completed_at,
        cancelled_at,
        paid_at,

        {{ utc_date('requested_at') }} as requested_date,
        date_trunc('hour', requested_at at time zone 'UTC') at time zone 'UTC' as requested_hour,

        wait_seconds,
        trip_seconds,
        distance_km,
        fare_amount,
        surge_multiplier,
        tolls_amount,
        tip_amount,
        total_amount,
        currency,
        payment_method,
        payment_amount,
        cancelled_by,
        cancel_reason,
        cancel_stage,

        _ingested_at,
        '{{ run_started_at }}'::timestamptz as _loaded_at
    from enriched
)

select * from final
