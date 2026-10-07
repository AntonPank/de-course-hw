{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert'
) }}

with source_rows as (

    select *
    from {{ source('bronze', 'raw_events') }}

    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}

),

filtered as (

    select *
    from source_rows
    where source is distinct from 'loadtest'
      and occurred_at is not null
      and ride_id is not null

),

ranked as (

    select
        *,
        row_number() over (
            partition by event_id
            order by _ingested_at, _source_file
        ) as rn
    from filtered

),

final as (

    select
        event_id,
        event_type,
        ride_id,
        source,
        occurred_at,
        {{ utc_date('occurred_at') }} as occurred_date,
        payload::jsonb as payload,
        _source_file,
        _ingested_at,
        '{{ run_started_at }}'::timestamptz as _loaded_at
    from ranked
    where rn = 1

)

select * from final
