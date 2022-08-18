{% macro create_tests_table_if_not_exists(database_name, schema_name, table_name) -%}

    {%- if adapter.get_relation(database=database_name, schema=schema_name, identifier=table_name) is none -%}
        {% if database_name %}
        {{ log("Creating table " ~ adapter.quote(database_name ~ "." ~ schema_name ~ "." ~ table_name), info=true) }}
        {% else %}
        {{ log("Creating table " ~ adapter.quote(schema_name ~ "." ~ table_name), info=true) }}
        {% endif %}
        {%- set query -%}
            {{ adapter.dispatch('get_create_tests_table_if_not_exists_statement', 'dbt_artifacts')(database_name, schema_name, table_name) }}
        {% endset %}
        {%- call statement(auto_begin=True) -%}
            {{ query }}
        {%- endcall -%}
    {%- endif -%}

{%- endmacro %}

{% macro snowflake__get_create_tests_table_if_not_exists_statement_test(database_name, schema_name, table_name) -%}
    create table {{database_name}}.{{schema_name}}.{{table_name}} (
        command_invocation_id STRING,
        node_id STRING,
        run_started_at TIMESTAMP_NTZ,
        name STRING,
        column_name STRING,
        depends_on_nodes ARRAY,
        depends_on_macros ARRAY,
        config VARIANT,
        package_name STRING,
        test_path STRING,
        tags ARRAY
    )
{%- endmacro %}

{% macro default__get_create_tests_table_if_not_exists_statement_test(database_name, schema_name, table_name) -%}
    create table {{database_name}}.{{schema_name}}.{{table_name}} (
        command_invocation_id STRING,
        node_id STRING,
        run_started_at TIMESTAMP,
        name STRING,
        column_name STRING,
        depends_on_nodes STRING,
        depends_on_macros STRING,
        config STRING,
        package_name STRING,
        test_path STRING,
        tags STRING
    )
{%- endmacro %}

{% macro snowflake__get_create_tests_table_if_not_exists_statement(database_name, schema_name, table_name) -%}
    create table {{database_name}}.{{schema_name}}.{{table_name}} (
        command_invocation_id STRING,
        run_started_at TIMESTAMP_NTZ,
        test VARIANT
    )
{%- endmacro %}

{% macro default__get_create_tests_table_if_not_exists_statement(database_name, schema_name, table_name) -%}
    create table {{database_name}}.{{schema_name}}.{{table_name}} (
        command_invocation_id STRING,
        run_started_at TIMESTAMP,
        test VARIANT
    )
{%- endmacro %}