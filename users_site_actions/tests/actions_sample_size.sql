SELECT * FROM (
SELECT count() AS n FROM {{ ref('actions_get_sample') }}

UNION ALL

SELECT count() AS n FROM {{ ref('actions_add_status_info') }}
)
GROUP BY n
HAVING n == 1