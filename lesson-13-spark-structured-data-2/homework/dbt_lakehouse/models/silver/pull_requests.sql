with parsed as (
    select
        event_id,
        repo_name,
        created_at as event_at,
        from_json(
            payload,
            '{{ var("pr_schema") }}'
        ) as p
    from {{ ref('events') }}
    where event_type = 'PullRequestEvent'
),

ranked as (
    select
        repo_name,
        p.number as pr_number,
        p.pull_request.title as title,
        p.pull_request.user.login as author_login,
        p.pull_request.state as state,
        p.pull_request.merged as is_merged,
        p.pull_request.draft as is_draft,
        to_timestamp(p.pull_request.created_at) as opened_at,
        to_timestamp(p.pull_request.closed_at) as closed_at,
        to_timestamp(p.pull_request.merged_at) as merged_at,
        p.pull_request.additions as additions,
        p.pull_request.deletions as deletions,
        p.pull_request.changed_files as changed_files,
        p.pull_request.commits as commits_count,
        p.pull_request.comments as comments,
        p.pull_request.review_comments as review_comments,
        p.pull_request.author_association as author_association,
        transform(p.pull_request.labels, x -> x.name) as label_names,
        p.action as last_action,
        event_at as last_event_at,
        row_number() over (
            partition by repo_name, p.number
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
    pr_number,
    title,
    author_login,
    state,
    is_merged,
    is_draft,
    opened_at,
    closed_at,
    merged_at,
    additions,
    deletions,
    changed_files,
    commits_count,
    comments,
    review_comments,
    author_association,
    label_names,
    last_action,
    last_event_at,
    additions + deletions as churn,
    cast(
        (
            unix_timestamp(coalesce(closed_at, last_event_at))
            - unix_timestamp(opened_at)
        ) / 3600.0
        as double
    ) as hours_open
from latest
