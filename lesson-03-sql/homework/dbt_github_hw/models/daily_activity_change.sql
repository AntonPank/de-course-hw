WITH daily_counts AS (
    SELECT
        event_date,
        COUNT(*) AS events
    FROM {{ ref('stg_events') }}
    GROUP BY
        event_date
),

daily_with_previous AS (
    SELECT
        event_date,
        events,
        LAG(events) OVER (
            ORDER BY event_date
        ) AS prev_day_events
    FROM daily_counts
)

SELECT
    event_date,
    events,
    prev_day_events,
    events - prev_day_events AS delta_events
FROM daily_with_previous
ORDER BY
    event_date
