with parsed as (
    select
        event_id,
        event_type,
        repo_name,
        created_at as event_at,
        from_json(
            payload,
            '{{ var("issue_schema") }}'
        ) as p
    from {{ ref('events') }}
    where event_type in ('IssuesEvent', 'IssueCommentEvent')
),

ranked as (
    select
        repo_name,
        p.issue.number as issue_number,
        p.issue.title as title,
        p.issue.user.login as author_login,
        p.issue.state as state,
        to_timestamp(p.issue.created_at) as opened_at,
        to_timestamp(p.issue.closed_at) as closed_at,
        p.issue.comments as comments,
        transform(p.issue.labels, x -> x.name) as label_names,

        sum(
            case
                when event_type = 'IssueCommentEvent' then 1
                else 0
            end
        ) over (
            partition by repo_name, p.issue.number
        ) as comment_events_seen,

        event_at as last_event_at,

        row_number() over (
            partition by repo_name, p.issue.number
            order by event_at desc, event_id desc
        ) as rn
    from parsed
),

latest as (
    select *
    from ranked
    where rn = 1
)

select
    repo_name,
    issue_number,
    title,
    author_login,
    state,
    opened_at,
    closed_at,
    comments,
    label_names,
    comment_events_seen,
    last_event_at,
    case
        when closed_at is null then null
        else cast(
            (
                unix_timestamp(closed_at)
                - unix_timestamp(opened_at)
            ) / 3600.0
            as double
        )
    end as hours_to_close
from latest
