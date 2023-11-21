SELECT user_id, count() AS n FROM {{ ref('user_ads_status') }}
WHERE time_start == toDate('1980-01-01')
GROUP BY user_id
HAVING n > 1