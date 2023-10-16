{%- macro get_actual_info(tb_name, sample_ref) -%}
    {%- set tb_exist -%}
        EXISTS TABLE {{ tb_name }}
    {%- endset -%}
    {%- if execute and run_query(tb_exist).columns[0].values()[0] -%}
        select * except (time_end)
        from {{ tb_name }}
        where
            toDate(time_end) == '2050-01-01'
    {%- else -%}
        select * from {{sample_ref}} limit 0
    {%- endif %}
{%- endmacro %}
