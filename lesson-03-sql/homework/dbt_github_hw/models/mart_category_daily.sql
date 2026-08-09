SELECT
    e.event_date,
    c.is_weekend,
    ec.category,

    COUNT(*) AS events,
    COUNT(DISTINCT repo_name) AS distinct_repos,
    COUNT(DISTINCT actor_login) AS distinct_actors

FROM {{ref('stg_events')}} AS e

JOIN {{ref('event_categories')}} AS ec
    ON e.event_type = ec.event_type

JOIN {{ref('calendar')}} AS c 
    ON e.event_date = c.day

GROUP BY
    e.event_date,
    c.is_weekend,
    ec.category