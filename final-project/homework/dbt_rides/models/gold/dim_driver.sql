{{
    config(
        materialized='incremental',
        unique_key='driver_key',
        incremental_strategy='delete+insert'
    )
}}

with affected_drivers as (
    select distinct
        payload #>> '{driver,id}' as driver_key
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
    {% if is_incremental() %}
      and _ingested_at > {{ high_watermark() }}
    {% endif %}

    union
    select 'unknown' as driver_key
),

all_accepted as (
    select
        e.ride_id,
        e.occurred_at,
        e._ingested_at,
        e.payload #>> '{driver,id}' as driver_key,
        e.payload #>> '{driver,vehicle,type}' as vehicle_type,
        e.payload #>> '{driver,vehicle,medallion}' as medallion,
        (e.payload #>> '{driver,rating}')::numeric as rating
    from {{ ref('events') }} as e
    inner join affected_drivers as a
        on e.payload #>> '{driver,id}' = a.driver_key
    where e.event_type = 'ride_accepted'
),

ranked as (
    select
        *,
        row_number() over (
            partition by driver_key
            order by occurred_at desc
        ) as rn
    from all_accepted
),

driver_aggregated as (
    select
        driver_key,
        max(vehicle_type) filter (where rn = 1) as vehicle_type,
        max(medallion) filter (where rn = 1) as medallion,
        max(rating) filter (where rn = 1) as latest_rating,
        min(occurred_at) as first_seen_at,
        max(occurred_at) as last_seen_at,
        max(_ingested_at) as _ingested_at
    from ranked
    group by driver_key
),

final as (
    select
        driver_key,
        vehicle_type,
        medallion,
        latest_rating,
        first_seen_at,
        last_seen_at,
        _ingested_at,
        '{{ run_started_at }}'::timestamptz as _loaded_at
    from driver_aggregated

    union all

    select
        'unknown' as driver_key,
        'unknown' as vehicle_type,
        'unknown' as medallion,
        null::numeric as latest_rating,
        '1970-01-01'::timestamptz as first_seen_at,
        '1970-01-01'::timestamptz as last_seen_at,
        '1970-01-01'::timestamptz as _ingested_at,
        '{{ run_started_at }}'::timestamptz as _loaded_at
)

select * from final
