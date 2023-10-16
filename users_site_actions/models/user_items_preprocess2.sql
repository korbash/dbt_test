SELECT * EXCEPT good_id,
    map(good_id, 1) AS good_id
FROM {{ ref('user_items_preprocess') }}