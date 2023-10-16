SELECT * FROM {{ ref('track_geo_status') }}
WHERE time_start == toDate('1970-01-01') OR time_end == toDate('1970-01-01')