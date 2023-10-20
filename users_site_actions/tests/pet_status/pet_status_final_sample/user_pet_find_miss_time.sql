SELECT id 
FROM {{ ref('pet_status_final_sample') }}
WHERE time_start == toDate('1970-01-01') OR time_end == toDate('1970-01-01')