{% macro remove_nulls(column) %}
        COALESCE ({{ column}}, 'Unknown')
{% endmacro %}