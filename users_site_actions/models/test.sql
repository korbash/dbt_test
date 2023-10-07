SELECT count() AS n FROM users
GROUP BY toDate(date_registr)