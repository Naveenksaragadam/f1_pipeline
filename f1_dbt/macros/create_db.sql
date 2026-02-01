{% macro create_db(db_name) %}
    {% set sql %}
        CREATE DATABASE IF NOT EXISTS {{ db_name }};
    {% endset %}
    
    {% do run_query(sql) %}
    {{ log("Database created or already exists.", info=True) }}
{% endmacro %}