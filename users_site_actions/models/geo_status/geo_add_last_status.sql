(SELECT id, track_id,
    'init' AS event, 'pass' AS type,
    Null AS country, Null AS currency,
    Null AS lang,
    time_start AS time_sort

FROM {{ ref('geo_uniq_track_sample') }} LEFT ANTI JOIN {{ ref('geo_actual_info_sample') }} USING track_id
)

UNION ALL

SELECT * FROM {{ ref('geo_actual_info_sample') }}

UNION ALL

SELECT * FROM {{ ref('geo_get_sample') }}