
{%- macro pet_get_max_date(tb_name) -%}
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
