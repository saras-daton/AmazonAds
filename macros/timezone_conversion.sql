{% macro timezone_conversion(col_name) %}

    {% if target.type =='snowflake' and timezone_conversion_flag %}
        cast(CONVERT_TIMEZONE('{{var("to_timezone")}}', {{col_name}}::timestamp_ntz) as {{ dbt.type_timestamp() }})
    {% elif target.type =='bigquery' and timezone_conversion_flag %}
        DATETIME(cast({{col_name}} as timestamp), '{{var("to_timezone")}}')
    {% else %}
        col_name
    {% endif %}

{% endmacro %}
