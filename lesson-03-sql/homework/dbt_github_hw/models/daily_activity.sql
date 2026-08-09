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
    
    SUM(events) OVER (

        ORDER BY 
            event_date

        ROWS BETWEEN
            UNBOUNDED PRECEDING
            AND 
            CURRENT ROW
    ) AS running_events

FROM daily_counts

ORDER BY
    event_date