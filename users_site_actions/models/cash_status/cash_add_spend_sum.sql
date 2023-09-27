
SELECT user_id
      ,sumIf(spend, currency == 'rub') over w as rub 
      ,sumIf(spend, currency == 'eur') over w as eur
      ,sumIf(spend, currency == 'usd') over w as usd
      ,time_sort
FROM {{ ref('cash_filter') }}
WINDOW w AS (PARTITION BY user_id ORDER BY time_sort ASC
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
order by user_id, time_sort