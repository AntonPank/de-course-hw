with daily as (
    select
        sum(commits) as commit_count
    from {{ ref('fact_repo_activity_daily') }}
),

facts as (
    select
        count(*) as commit_count
    from {{ ref('fact_commit') }}
)

select
    daily.commit_count as daily_commit_count,
    facts.commit_count as fact_commit_count
from daily
cross join facts
where daily.commit_count <> facts.commit_count