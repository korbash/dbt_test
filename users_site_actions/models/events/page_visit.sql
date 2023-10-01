
{{ config(
    materialized='view',
    order_by=['datetime']
) }}

select * 
from {{ source('src_actions', 'src_actions') }}
where (event == 'page_visit')