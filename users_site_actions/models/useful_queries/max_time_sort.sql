
SELECT MAX(time_start) AS t_max, 'track_user_status' AS name 
FROM {{ ref('track_login_status') }}

UNION ALL

SELECT MAX(time_start) AS t_max, 'track_ads_status' AS name 
FROM {{ ref('track_ads_status') }}

UNION ALL

SELECT MAX(time_start) AS t_max, 'track_geo_status' AS name 
FROM {{ ref('track_geo_status') }}

UNION ALL

SELECT MAX(time_sort) AS t_max, 'pre_users_actions' AS name 
FROM {{ source('src_actions', 'src_actions') }}

UNION ALL

SELECT MAX(time_sort) AS t_max, 'users_actions' AS name 
FROM {{ ref('users_actions') }}

UNION ALL

SELECT MAX(time_sort) AS t_max, 'actions_get_sample' AS name 
FROM {{ ref('actions_get_sample') }}
