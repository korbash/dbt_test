SELECT * , count() AS n2 FROM (
SELECT count() AS n FROM {{ ref('actions_get_sample') }}

UNION ALL

SELECT count() AS n FROM {{ ref('join_with_track_info') }}

UNION ALL

SELECT count() AS n FROM {{ ref('join_with_login_status') }}

UNION ALL

SELECT count() AS n FROM {{ ref('join_with_ads_status') }}

UNION ALL

SELECT count() AS n FROM {{ ref('join_with_geo_status') }}

UNION ALL

SELECT count() AS n FROM {{ ref('join_with_user_info') }}
)
GROUP BY n
HAVING n2 != 6