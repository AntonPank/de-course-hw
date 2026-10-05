{{ config(materialized='incremental', incremental_strategy='append') }}

with source as (
    select *
    from {{ source('bronze', 'raw_events') }}
    {% if is_incremental()%}
    where _ingested_at > (
        select max(_ingested_at)
        from {{ this }}
    )
    {% endif %}
),

filtered as (
    select
        id as event_id,
        type as event_type,
        actor.login as actor_login,
        repo.name as repo_name,
        split(repo.name, '/')[0] as repo_owner,
        to_timestamp(created_at) as created_at,
        payload,
        _ingested_at,
        _source_file
    from source
    where type in (
        'PushEvent',
        'PullRequestEvent',
        'IssuesEvent',
        'IssueCommentEvent',
        'WatchEvent',
        'ForkEvent'
    )
        and public = true
        and id is not null
        and repo.name is not null
        and created_at is not null
),

ranked as (
    select *,
        row_number() over (
            partition by event_id
            order by _ingested_at, _source_file
        ) as rn
    from filtered
)

select
    event_id,
    event_type,
    actor_login,
    repo_name,
    repo_owner,
    created_at,
    payload,
    _ingested_at,
    _source_file
from ranked
where rn = 1
