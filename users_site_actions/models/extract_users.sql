


SELECT _airbyte_emitted_at AS load2ch,
    JSONExtract(_airbyte_data, '_id', 'String') AS user_id,
    JSONExtract(_airbyte_data, 'email', 'String') AS email,
    JSONExtract(_airbyte_data, 'public_username', 'String') AS public_username,
    JSONExtract(_airbyte_data, 'ref_code', 'String') AS reg_ref,
    JSONExtract(_airbyte_data, 'main_country', 'String') AS reg_country,
    JSONExtract(_airbyte_data, 'main_currency', 'String') AS reg_currency,
    JSONExtract(_airbyte_data, 'user_type', 'String') AS reg_type,
    toDateTime64(replaceOne(replaceOne(JSONExtract(_airbyte_data, 'date_register', 'String'), 'T', ' '), 'Z', ''), 3, 'UTC') AS reg_time,
    toDateTime64(replaceOne(replaceOne(JSONExtract(_airbyte_data, 'date_request', 'String'), 'T', ' '), 'Z', ''), 3, 'UTC') AS request_time

FROM {{ source('src_back', 'src_users') }}