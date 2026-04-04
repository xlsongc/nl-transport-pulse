{% macro generate_date_spine(start_date, end_date) %}
    select
        date as date_day
    from unnest(
        generate_date_array(
            cast('{{ start_date }}' as date),
            cast('{{ end_date }}' as date)
        )
    ) as date
{% endmacro %}
