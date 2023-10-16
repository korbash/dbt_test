SELECT
    JSONExtract(_airbyte_data, '_id', 'String') AS id,
    JSONExtract(_airbyte_data, 'user_id', 'String') AS user_id,
    JSONExtract(_airbyte_data, 'good_id', 'String') AS good_id,
    JSONExtract(_airbyte_data, 'on_withdrawal', 'Bool') AS on_withdrawal,
    JSONExtract(_airbyte_data, 'on_sale', 'Bool') AS on_sale,
    toDateTime64(replaceOne(replaceOne(JSONExtract(_airbyte_data, 'datetime', 'String'), 'T', ' '), 'Z', ''), 3, 'UTC') AS datetime
FROM _airbyte_raw_user_items
