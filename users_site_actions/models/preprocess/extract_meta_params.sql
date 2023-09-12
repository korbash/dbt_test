SELECT *,
    simpleJSONExtractString(payload, 'type') as type,
    simpleJSONExtractString(payload, 'method') as method,
    simpleJSONExtractBool(payload, 'success') as success,
    toDateTime64OrNull(replaceOne(replaceOne(simpleJSONExtractString(payload, 'datetime'), 'T', ' '), 'Z', ''), 3, 'UTC') as time_user
FROM {{ source('src_actions', 'actions_row') }}