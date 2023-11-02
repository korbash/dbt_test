SELECT * FROM {{ ref('users_actions') }}
WHERE event = 'pet2game'