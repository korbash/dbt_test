SELECT COUNT(DISTINCT last_action) AS dif_date FROM (
SELECT max(toDate(time_start)) AS last_action
FROM {{ ref('track_ads_status') }}

UNION ALL

SELECT max(toDate(time_start)) AS last_action
FROM {{ ref('track_geo_status') }}

UNION ALL

SELECT max(toDate(time_start)) AS last_action
FROM {{ ref('track_login_status') }}
)
HAVING dif_date > 1
