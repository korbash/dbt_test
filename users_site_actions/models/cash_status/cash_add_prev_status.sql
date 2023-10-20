with cash_add_prev_status as ( 

select    toString(generateUUIDv4()) as id, user_id 
          ,0 as balance_rub
          ,0 as balance_eur
          ,0 as balance_usd

          ,0 as buy_rub
          ,0 as buy_eur
          ,0 as buy_usd

          ,0 as sell_rub
          ,0 as sell_eur
          ,0 as sell_usd

          ,0 as topup_rub
          ,0 as topup_eur
          ,0 as topup_usd

          ,0 as withdraw_rub
          ,0 as withdraw_eur
          ,0 as withdraw_usd
          ,toDateTime('1980-01-01 00:00:00') as _time_start, time_start as time_end
from (

select *, row_number() over(partition by user_id order by time_start) as action_rank
from {{ ref('cash_add_time_end') }}
where user_id not in (select distinct user_id 
                      from user_cash_status 
                      where time_start == toDateTime('1980-01-01 00:00:00')) 
)
where action_rank == 1

union all 

select *
from {{ ref('cash_add_time_end') }}
)





select distinct *
from (
    select * except (_time_start, time_end), _time_start as time_start, time_end
    from cash_add_prev_status
)