{{config(
    materialized='incremental',
    engine='MergeTree()',
    order_by=['time_upload']
    )}}

{%- if is_incremental() -%}
    select generateUUIDv4() as id, track_id, event, created_at as time_upload, payload
    from {{ source("src_actions", "actions_new") }}
    where time_upload > (select max(time_upload) from {{ this }})

{%- else -%}
    select
        generateUUIDv4() as id,
        assumenotnull(track_id['$oid']) as track_id,
        assumenotnull(event) as event,
        assumenotnull(todatetime64(datetime['$date'], 3)) as time_upload,
        assumenotnull(payload) as payload
    from (select * from {{ ref("joinS3sources") }} where isnotnull(event))

    union all

    select generateUUIDv4() as id, track_id, event, created_at as time_upload, payload
    from {{ source("src_actions", "actions_new") }}
{%- endif -%}
