SELECT user_id, count() AS n 
FROM {{ ref('user_pet_status') }}
WHERE time_end == toDate('2050-01-01')
GROUP BY user_id
HAVING n > 1