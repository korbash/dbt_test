SELECT id, track_id, event, time_sort as time_start,
    if(event == 'apply_promo', method, Null) as promo0,
    if(event == 'apply_promo', time_start, Null) as promo_time0,
    if(event == 'apply_ref', method, Null) as ref0,
    if(event == 'apply_ref', time_start, Null) as ref_time0,
    if(event == 'apply_utm', type, Null) as utm_source0,
    if(event == 'apply_utm', method, Null) as utm_medium0,
    if(event == 'apply_utm', merge_id, Null) as utm_campain0,
    if(event == 'apply_utm', time_start, Null) as utm_time0
FROM (
    SELECT * FROM {{ ref("apply_promo") }}

    UNION ALL 

    SELECT * FROM {{ ref("apply_ref") }}

    UNION ALL 

    SELECT * FROM {{ ref("apply_utm") }}  
)