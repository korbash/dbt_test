{# select distinct on (track_id, method, success) * #}
select *
from {{ source('src_actions', 'src_actions') }}
where event == 'apply_promo'
