with commits_daily as (
    select
        repo_name,
        to_date(pushed_at) as day,
        count(*) as commits,
        count(distinct author_email) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from {{ ref('commits') }}
    group by repo_name, to_date(pushed_at)
),

pr_days as (
    select
        repo_name,
        to_date(opened_at) as day
    from {{ ref('pull_requests') }}

    union

    select
        repo_name,
        to_date(merged_at) as day
    from {{ ref('pull_requests') }}
    where merged_at is not null
),

prs_daily as (
    select
        d.repo_name,
        d.day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        sum(
            case
                when to_date(p.opened_at) = d.day then 1
                else 0
            end
        ) as prs_opened,
        sum(
            case
                when to_date(p.merged_at) = d.day then 1
                else 0
            end
        ) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from pr_days d
    join {{ ref('pull_requests') }} p
        on d.repo_name = p.repo_name
        and (
            to_date(p.opened_at) = d.day
            or to_date(p.merged_at) = d.day
        )
    group by d.repo_name, d.day
),

issue_days as (
    select
        repo_name,
        to_date(opened_at) as day
    from {{ ref('issues') }}

    union

    select
        repo_name,
        to_date(closed_at) as day
    from {{ ref('issues') }}
    where closed_at is not null
),

issues_daily as (
    select
        d.repo_name,
        d.day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        sum(
            case
                when to_date(i.opened_at) = d.day then 1
                else 0
            end
        ) as issues_opened,
        sum(
            case
                when to_date(i.closed_at) = d.day then 1
                else 0
            end
        ) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from issue_days d
    join {{ ref('issues') }} i
        on d.repo_name = i.repo_name
        and (
            to_date(i.opened_at) = d.day
            or to_date(i.closed_at) = d.day
        )
    group by d.repo_name, d.day
),

events_daily as (
    select
        repo_name,
        to_date(created_at) as day,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        sum(
            case
                when event_type = 'WatchEvent' then 1
                else 0
            end
        ) as stars,
        sum(
            case
                when event_type = 'ForkEvent' then 1
                else 0
            end
        ) as forks
    from {{ ref('events') }}
    where event_type in ('WatchEvent', 'ForkEvent')
    group by repo_name, to_date(created_at)
),

combined as (
    select * from commits_daily

    union all

    select * from prs_daily

    union all

    select * from issues_daily

    union all

    select * from events_daily
),

daily as (
    select
        repo_name,
        day,
        sum(commits) as commits,
        sum(distinct_committers) as distinct_committers,
        sum(prs_opened) as prs_opened,
        sum(prs_merged) as prs_merged,
        sum(issues_opened) as issues_opened,
        sum(issues_closed) as issues_closed,
        sum(stars) as stars,
        sum(forks) as forks
    from combined
    group by repo_name, day
),

with_keys as (
    select
        md5(repo_name) as repo_id,
        cast(date_format(day, 'yyyyMMdd') as int) as date_id,
        commits,
        distinct_committers,
        prs_opened,
        prs_merged,
        issues_opened,
        issues_closed,
        stars,
        forks
    from daily
)

select
    md5(
        concat_ws('|', repo_id, cast(date_id as string))
    ) as activity_id,
    repo_id,
    date_id,
    commits,
    distinct_committers,
    prs_opened,
    prs_merged,
    issues_opened,
    issues_closed,
    stars,
    forks
from with_keys