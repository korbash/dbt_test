SELECT track_id, count() AS n FROM {{ ref('track_geo_status') }}
WHERE time_start == toDate('1980-01-01')
GROUP BY track_id
HAVING n > 1