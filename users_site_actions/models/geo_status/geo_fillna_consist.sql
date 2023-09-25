    SELECT * EXCEPT (country, currency, lang),
        anyLast(country) OVER w AS country,
        anyLast(currency) OVER w AS currency,
        anyLast(lang) OVER w AS lang
    FROM {{ ref("geo_add_last_status") }}
    WINDOW w AS (PARTITION BY track_id ORDER BY time_sort ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

