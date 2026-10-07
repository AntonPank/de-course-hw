{{
    config(
        materialized='incremental',
        unique_key='requested_hour',
        incremental_strategy='delete+insert'
    )
}}

with affected_hours as (
    select distinct
        requested_hour
    from {{ ref('fact_ride') }}
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}
),

source_rides as (
    select r.*
    from {{ ref('fact_ride') }} as r
    {% if is_incremental() %}
    inner join affected_hours as a
        on r.requested_hour = a.requested_hour
    {% endif %}
),

aggregated as (
    select
        requested_hour,
        pickup_zone_key,
        count(*) as rides_requested,
        count(*) filter (where status = 'completed') as rides_completed,
        count(*) filter (where status = 'cancelled') as rides_cancelled,
        coalesce(sum(total_amount) filter (where status = 'completed'), 0)::numeric(12, 2) as gross_revenue,
        coalesce(sum(tip_amount) filter (where status = 'completed'), 0)::numeric(12, 2) as tips,
        avg(wait_seconds) as avg_wait_seconds,
        max(_ingested_at) as _ingested_at,
        '{{ run_started_at }}'::timestamptz as _loaded_at
    from source_rides
    group by
        requested_hour,
        pickup_zone_key
)

select * from aggregated
