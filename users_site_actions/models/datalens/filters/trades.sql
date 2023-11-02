SELECT * FROM {{ ref('users_actions') }}
WHERE event = 'trade'