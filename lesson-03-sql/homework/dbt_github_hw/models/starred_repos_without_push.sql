SELECT DISTINCT
    repo_name
FROM {{ ref('stg_events') }} AS w
WHERE
    w.event_type = 'WatchEvent'
    AND NOT EXISTS (
        SELECT 1
        FROM {{ ref('stg_events') }} AS p
        WHERE
            p.event_type = 'PushEvent'
            AND w.repo_name = p.repo_name
    )
