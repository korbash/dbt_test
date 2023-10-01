

SELECT * EXCEPT time_end
      ,if(time_end == '1970-01-01', date('2050-01-01'), time_end) AS time_end
from (
    select id, user_id
          ,round(balance_rub,2) as balance_rub
          ,round(balance_eur,2) as balance_eur
          ,round(balance_usd,2) as balance_usd

          ,round(buy_rub,2) as buy_rub
          ,round(buy_eur,2) as buy_eur
          ,round(buy_usd,2) as buy_usd

          ,round(sell_rub,2) as sell_rub
          ,round(sell_eur,2) as sell_eur
          ,round(sell_usd,2) as sell_usd

          ,round(topup_rub,2) as topup_rub
          ,round(topup_eur,2) as topup_eur
          ,round(topup_usd,2) as topup_usd

          ,round(withdraw_rub,2) as withdraw_rub
          ,round(withdraw_eur,2) as withdraw_eur
          ,round(withdraw_usd,2) as withdraw_usd
          ,time_sort as time_start
          ,any(time_sort) OVER w AS time_end
    from {{ ref('cash_add_spend_sum') }}
    window w as (PARTITION BY user_id ORDER BY time_sort ASC
                ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)
)
order by time_start, user_id