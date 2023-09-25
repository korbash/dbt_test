SELECT * EXCEPT (last_country, last_currency, last_lang)
FROM (
    SELECT *,
        any(country) OVER w AS last_country,
        any(currency) OVER w AS last_currency,
        any(lang) OVER w AS last_lang
    FROM {{ ref('geo_fillna_consist') }}
    WINDOW w AS (PARTITION BY track_id ORDER BY time_sort ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)
    )
WHERE country != last_country OR (isNull(last_country) AND isNotNull(last_country))
    OR currency != last_currency OR (isNull(last_currency) AND isNotNull(last_currency))
    OR lang != last_lang OR (isNull(last_lang) AND isNotNull(last_lang))