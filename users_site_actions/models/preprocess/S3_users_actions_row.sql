SELECT _id['$oid'] AS id,
    track_id['$oid'] AS track_id,
    assumeNotNull(event),
    toDateTime64(datetime['$date'],3) AS time_upload,
    payload FROM {{ ref("joinS3sources") }}
    WHERE isNotNull(event)