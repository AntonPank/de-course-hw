with parsed as (
    select
        event_id,
        repo_name,
        actor_login,
        created_at,
        from_json(
            payload,
            '{{ var("push_schema") }}'
        ) as p
        from {{ ref('events') }}
        where event_type = 'PushEvent'
),

exploded as (
    select
        event_id,
        repo_name,
        actor_login,
        created_at,
        p.ref as ref,
        explode(p.commits) as commit
    from parsed
),

ranked as (
    select
        commit.sha as commit_sha,
        repo_name,
        actor_login as pushed_by,
        regexp_replace(ref, '^refs/heads/', '') as branch,
        commit.author.name as author_name,
        commit.author.email as author_email,
        commit.message as message,
        commit.`distinct` as is_distinct,
        created_at as pushed_at,
        commit.message like 'Merge %' as is_merge_commit,
        split(commit.message, '/n')[0] as message_subject,
        length(commit.message) as message_length,
        row_number() over (
            partition by commit.sha
            order by created_at, event_id
        ) as rn
    from exploded
)

select
    commit_sha,
    repo_name,
    pushed_by,
    branch,
    author_name,
    author_email,
    message,
    is_distinct,
    pushed_at,
    is_merge_commit,
    message_subject,
    message_length
from ranked
where rn = 1
