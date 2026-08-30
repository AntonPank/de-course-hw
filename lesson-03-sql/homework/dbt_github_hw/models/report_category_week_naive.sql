SELECT
    c.iso_week,
    cat.category,
    COUNT(*) AS events
FROM {{ ref('stg_events') }} e
JOIN {{ ref('calendar') }} c
    ON strftime(e.event_date, '%Y-%m-%d') = strftime(c.day, '%Y-%m-%d')
JOIN {{ ref('event_categories') }} cat
    ON e.event_type = cat.event_type
WHERE c.iso_week = 2
GROUP BY
    c.iso_week,
    cat.category
ORDER BY
    cat.category
