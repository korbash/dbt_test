SELECT * EXCEPT (last_country, last_currency, last_lang)
FROM (
    SELECT *,
        anyLast(country) OVER w AS last_country,
        anyLast(currency) OVER w AS last_currency,
        anyLast(lang) OVER w AS last_lang
    FROM {{ ref('geo_fillna_consist') }}
    WINDOW w AS (PARTITION BY track_id ORDER BY time_sort ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)
    )
WHERE country != last_country OR (isNull(last_country) AND isNotNull(country))
    OR currency != last_currency OR (isNull(last_currency) AND isNotNull(currency))
    OR lang != last_lang OR (isNull(last_lang) AND isNotNull(lang))