{%- set n = 50 -%}
{%- set f_list = [] -%}
{%- for i in range(n) -%}
    {%- set f_list = f_list.append('{}'.format(i)) -%}
{%- endfor -%}

{{ config(
    materialized='table_by_parts',
    filter_list=f_list,
    engine='MergeTree()',
    order_by=['track_id']
) }}
    SELECT
        track_id,
        any(user_id) AS user_id,
        groupArrayIf(100)(method, event == 'apply_promo' and success) AS promocodes,
        groupArrayIf(100)(method, event == 'apply_ref' and success) AS refs,
        groupArrayIf(100)(type, event == 'apply_utm' and success) AS utm_source,
        groupArrayIf(100)(method, event == 'apply_utm' and success) AS utm_campains,
        groupArrayIf(100)(merge_id, event == 'apply_utm' and success) AS utm_medium,
        any(datetime) as first_action,
        any(if(event == 'apply_promo' and success, datetime, null)) as apply_promo,
        any(if(event == 'apply_utm' and success, datetime, null)) as apply_utm,
        any(if(event == 'apply_ref' and success, datetime, null)) as apply_ref,
        any(if(event == 'auth' and type == 'reg', datetime, null)) as try_reg,
        any(if(event == 'auth' and type == 'reg' and success, datetime, null)) as reg,
        any(if(event == 'auth' and type == 'login', datetime, null)) as try_login,
        any(if(event == 'auth' and type == 'login' and success, datetime, null)) as login,
        any(if(event == 'payment' and type == 'topup', datetime, null)) as try_topup,
        any(if(event == 'payment' and type == 'topup' and success, datetime, null)) as topup,
        any(if(event == 'payment' and type == 'withdraw', datetime, null)) as try_withdraw,
        any(if(event == 'payment' and type == 'withdraw' and success, datetime, null)) as withdraw,
        any(if(event == 'trade' and type == 'buy' and success, datetime, null)) as buy,
        any(if(event == 'trade' and type == 'sell' and success, datetime, null)) as sell,
        any(if(event == 'pet2game' and type == 'topup', datetime, null)) as try_topup_pet,
        any(if(event == 'pet2game' and type == 'topup' and success, datetime, null)) as topup_pet,
        any(if(event == 'pet2game' and type == 'withdraw', datetime, null)) as try_withdraw_pet,
        any(if(event == 'pet2game' and type == 'withdraw' and success, datetime, null)) as withdraw_pet
    FROM (
        SELECT track_id, user_id, event, datetime, type, method, merge_id, success
        FROM {{ ref("users_actions") }}
        WHERE modulo(sipHash64(track_id),{{n}}) == '__filter__'
        ORDER BY datetime
    )
    GROUP BY track_id