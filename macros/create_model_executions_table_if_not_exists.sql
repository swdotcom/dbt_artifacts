{% macro create_model_executions_table_if_not_exists(database_name, schema_name, table_name) -%}

    {%- if adapter.get_relation(database=database_name, schema=schema_name, identifier=table_name) is none -%}
        {% if database_name %}
        {{ log("Creating table " ~ adapter.quote(database_name ~ "." ~ schema_name ~ "." ~ table_name), info=true) }}
        {% else %}
        {{ log("Creating table " ~ adapter.quote(schema_name ~ "." ~ table_name), info=true) }}
        {% endif %}
        {%- set query -%}
            {{ adapter.dispatch('get_create_model_executions_table_if_not_exists_statement', 'dbt_artifacts')(database_name, schema_name, table_name) }}
        {% endset %}
        {%- call statement(auto_begin=True) -%}
            {{ query }}
        {%- endcall -%}
    {%- endif -%}

{%- endmacro %}

{% macro snowflake__get_create_model_executions_table_if_not_exists_statement(database_name, schema_name, table_name) -%}
    create table {{database_name}}.{{schema_name}}.{{table_name}} (
        command_invocation_id STRING,
        unique_id STRING,
        run_started_at TIMESTAMP_NTZ,
        was_full_refresh BOOLEAN,
        status STRING,
        thread_id STRING,
        execution_time DOUBLE,
        message STRING,
        failures INTEGER,
        compile_started_at TIMESTAMP_NTZ,
        compile_completed_at TIMESTAMP_NTZ,
        query_started_at TIMESTAMP_NTZ,
        query_completed_at TIMESTAMP_NTZ,
        adapter_response VARIANT,
        materialization STRING,
        database STRING,
        schema STRING,
        name STRING,
        compiled_sql STRING
    )
{%- endmacro %}

{% macro default__get_create_model_executions_table_if_not_exists_statement(database_name, schema_name, table_name) -%}
    create table {{database_name}}.{{schema_name}}.{{table_name}} (
        command_invocation_id STRING,
        unique_id STRING,
        run_started_at TIMESTAMP,
        was_full_refresh BOOLEAN,
        status STRING,
        thread_id STRING,
        execution_time INTEGER,
        message STRING,
        failures INTEGER,
        compile_started_at TIMESTAMP,
        compile_completed_at TIMESTAMP,
        query_started_at TIMESTAMP,
        query_completed_at TIMESTAMP,
        adapter_response STRING,
        materialization STRING,
        database STRING,
        schema STRING,
        name STRING,
        compiled_sql STRING
    )
{%- endmacro %}