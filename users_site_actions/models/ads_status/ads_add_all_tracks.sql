SELECT toString(generateUUIDv4()) AS id,
    track_id,
    'init' as event,
    toDateTime('1980-01-01') AS time_start,
    Null as promo0,
    NUll as promo_time0,
    Null as ref0,
    NUll as ref_time0,
    Null as utm_source0,
    Null as utm_medium0,
    Null as utm_campain0,
    NUll as utm_time0

FROM {{ ref('ads_uniq_tracks') }}

UNION ALL

SELECT * FROM {{ ref('ads_exstract_info') }}
