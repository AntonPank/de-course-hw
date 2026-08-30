WITH repo_event_counts AS (
    SELECT
        event_type,
        repo_name,
        COUNT(*) AS event_count
    FROM {{ ref('stg_events') }}
    GROUP BY
        event_type,
        repo_name
)

SELECT
    event_type,
    repo_name,
    event_count,
    ROW_NUMBER() OVER (
        PARTITION BY event_type
        ORDER BY event_count DESC, repo_name
    ) AS type_rank
FROM repo_event_counts
QUALIFY type_rank <= 5
ORDER BY
    event_type,
    type_rank