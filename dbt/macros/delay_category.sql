{% macro delay_category(delay_minutes_col) %}
    case
        when {{ delay_minutes_col }} <= 1.0 then 'on_time'
        when {{ delay_minutes_col }} <= 5.0 then 'minor'
        when {{ delay_minutes_col }} <= 15.0 then 'moderate'
        else 'severe'
    end
{% endmacro %}
