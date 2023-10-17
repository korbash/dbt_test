SELECT track_id, min(time_start) AS t_min,  max(time_end) AS t_max FROM {{ ref('track_ads_status') }}
GROUP BY track_id
HAVING NOT (t_min == '1980-01-01' AND t_max == '2050-01-01')