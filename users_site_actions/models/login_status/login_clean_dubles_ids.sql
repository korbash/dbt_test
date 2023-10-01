SELECT * EXCEPT rang FROM (
    SELECT *,
        row_number() OVER (PARTITION BY id ORDER BY (time_start, type == 'reg')) AS rang
    FROM {{ ref('login_add_last_status') }}
)
WHERE rang == 1