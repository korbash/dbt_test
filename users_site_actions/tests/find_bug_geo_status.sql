SELECT track_id, count() AS n FROM {{ ref('geo_add_time_end') }}
WHERE time_end == toDate('2050-01-01')
GROUP BY track_id
HAVING n > 1