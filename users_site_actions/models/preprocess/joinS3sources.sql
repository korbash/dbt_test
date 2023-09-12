SELECT _id, track_id, datetime, event, payload, 's30' AS source FROM {{ source('src_actions', 's3actions0') }}

UNION ALL 

SELECT _id, track_id, datetime, event, payload, 's31' AS source FROM {{ source('src_actions', 's3actions1') }}