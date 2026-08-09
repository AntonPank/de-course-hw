WITH daily_counts AS (

    SELECT
        event_date,
        COUNT(*) AS events
    FROM {{ref('stg_events')}}
    GROUP BY
        event_date

)


SELECT
    event_date,
    events,

    LAG(events) OVER (

        ORDER BY
            event_date

    ) AS prev_day_events,

    events - 
    LAG(events) OVER (

        ORDER BY
            event_date

    ) AS delta_events


FROM daily_counts


ORDER BY
    event_date