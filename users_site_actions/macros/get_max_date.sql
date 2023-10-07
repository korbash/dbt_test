{%- macro get_max_date(tb_name) -%}
    {%- set tb_exist -%}
        EXISTS TABLE {{ tb_name }}
    {%- endset -%}
    {%- set max_date -%}
        SELECT max(toDate(time_start)) AS last_date FROM {{ tb_name }}
    {%- endset -%}

    {%- if execute and run_query(tb_exist).columns[0].values()[0] -%}
        {{ run_query(max_date).columns[0].values()[0] }}
    {%- else -%} {{ '2022-09-01' }}
    {%- endif -%}
{%- endmacro -%}
 

{%- macro get_uniq_track_sample(max_date, days) -%}
    SELECT DISTINCT ON (track_id) toString(generateUUIDv4()) AS id, track_id, toDateTime('1980-01-01') AS time_start
    FROM {{ source('src_actions', 'src_actions') }}
    WHERE toDate(time_sort)
        between toDate('{{ max_date }}') + toIntervalDay(1)
        and toDate('{{ max_date }}') + toIntervalDay({{days}} + 1)
{%- endmacro -%}