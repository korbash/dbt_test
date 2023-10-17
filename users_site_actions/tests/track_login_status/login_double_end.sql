SELECT track_id, count() AS n FROM {{ ref('track_login_status') }}
WHERE time_end == toDate('2050-01-01')
GROUP BY track_id
HAVING n > 1