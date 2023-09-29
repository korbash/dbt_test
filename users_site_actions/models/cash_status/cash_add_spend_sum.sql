
SELECT id, user_id

      ,sumIf(sum_spend_per_id, currency == 'rub') over w as balance_rub 
      ,sumIf(sum_spend_per_id, currency == 'eur') over w as balance_eur
      ,sumIf(sum_spend_per_id, currency == 'usd') over w as balance_usd

      ,sumIf(sum_spend_per_id, (currency == 'rub') and (type == 'buy') ) over w as buy_rub 
      ,sumIf(sum_spend_per_id, (currency == 'eur') and (type == 'buy') ) over w as buy_eur
      ,sumIf(sum_spend_per_id, (currency == 'usd') and (type == 'buy') ) over w as buy_usd

      ,sumIf(sum_spend_per_id, (currency == 'rub') and (type == 'sell') ) over w as sell_rub 
      ,sumIf(sum_spend_per_id, (currency == 'eur') and (type == 'sell') ) over w as sell_eur
      ,sumIf(sum_spend_per_id, (currency == 'usd') and (type == 'sell') ) over w as sell_usd

      ,sumIf(sum_spend_per_id, (currency == 'rub') and (type == 'topup') ) over w as topup_rub 
      ,sumIf(sum_spend_per_id, (currency == 'eur') and (type == 'topup') ) over w as topup_eur
      ,sumIf(sum_spend_per_id, (currency == 'usd') and (type == 'topup') ) over w as topup_usd

      ,sumIf(sum_spend_per_id, (currency == 'rub') and (type == 'withdraw') ) over w as withdraw_rub 
      ,sumIf(sum_spend_per_id, (currency == 'eur') and (type == 'withdraw') ) over w as withdraw_eur
      ,sumIf(sum_spend_per_id, (currency == 'usd') and (type == 'withdraw') ) over w as withdraw_usd

      ,time_sort
FROM {{ ref('cash_filter') }}
WINDOW w AS (PARTITION BY user_id ORDER BY time_sort ASC
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
