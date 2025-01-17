{% macro upload_invocations() -%}
    {% set src_dbt_invocations = source('dbt_artifacts', 'invocations') %}

    {% set invocation_values %}
    select
        {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(1) }},
        {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(2) }},
        {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(3) }},
        {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(4) }},
        {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(5) }},
        {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(6) }},
        {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(7) }},
        {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(8) }},
        {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(9) }},
        {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(10) }},
        {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(11) }},
        nullif({{ adapter.dispatch('column_identifier', 'dbt_artifacts')(12) }}, ''),
        nullif({{ adapter.dispatch('column_identifier', 'dbt_artifacts')(13) }}, ''),
        nullif({{ adapter.dispatch('column_identifier', 'dbt_artifacts')(14) }}, ''),
        nullif({{ adapter.dispatch('column_identifier', 'dbt_artifacts')(15) }}, ''),
        nullif({{ adapter.dispatch('column_identifier', 'dbt_artifacts')(16) }}, ''),
        nullif({{ adapter.dispatch('column_identifier', 'dbt_artifacts')(17) }}, ''),
        {{ adapter.dispatch('parse_json', 'dbt_artifacts')(adapter.dispatch('column_identifier', 'dbt_artifacts')(18)) }},
        {{ adapter.dispatch('parse_json', 'dbt_artifacts')(adapter.dispatch('column_identifier', 'dbt_artifacts')(19)) }},
        {{ adapter.dispatch('parse_json', 'dbt_artifacts')(adapter.dispatch('column_identifier', 'dbt_artifacts')(20)) }},
        nullif({{ adapter.dispatch('column_identifier', 'dbt_artifacts')(21) }}, '')
    from values
    (
        '{{ invocation_id }}', {# command_invocation_id #}
        '{{ dbt_version }}', {# dbt_version #}
        '{{ project_name }}', {# project_name #}
        '{{ run_started_at }}', {# run_started_at #}
        '{{ flags.WHICH }}', {# dbt_command #}
        '{{ flags.FULL_REFRESH }}', {# full_refresh_flag #}
        '{{ target.profile_name }}', {# target_profile_name #}
        '{{ target.name }}', {# target_name #}
        '{{ target.database }}', {# target_database #}
        '{{ target.schema }}', {# target_schema #}
        {{ target.threads }}, {# target_threads #}

        '{{ env_var('DBT_CLOUD_PROJECT_ID', '') }}', {# dbt_cloud_project_id #}
        '{{ env_var('DBT_CLOUD_JOB_ID', '') }}', {# dbt_cloud_job_id #}
        '{{ env_var('DBT_CLOUD_RUN_ID', '') }}', {# dbt_cloud_run_id #}
        '{{ env_var('DBT_CLOUD_RUN_REASON_CATEGORY', '') }}', {# dbt_cloud_run_reason_category #}
        '{{ env_var('DBT_CLOUD_RUN_REASON', '') }}', {# dbt_cloud_run_reason #}

        '{{ var('job_name', '') }}', {# job_name #}

        {% if var('env_vars', none) %}
            {% set env_vars_dict = {} %}
            {% for env_variable in var('env_vars') %}
                {% do env_vars_dict.update({env_variable: env_var(env_variable)}) %}
            {% endfor %}
            '{{ tojson(env_vars_dict) }}', {# env_vars #}
        {% else %}
            null, {# env_vars #}
        {% endif %}

        {% if var('dbt_vars', none) %}
            {% set dbt_vars_dict = {} %}
            {% for dbt_var in var('dbt_vars') %}
                {% do dbt_vars_dict.update({dbt_var: var(dbt_var)}) %}
            {% endfor %}
            '{{ tojson(dbt_vars_dict) }}', {# dbt_vars #}
        {% else %}
            null, {# dbt_vars #}
        {% endif %}

        '{{ tojson(selected_resources) }}', {# selected_resources #}
        '{{ var('run_id', '') }}' {# run_id #}
    )
    {% endset %}

    {{ dbt_artifacts.insert_into_metadata_table(
        database_name=src_dbt_invocations.database,
        schema_name=src_dbt_invocations.schema,
        table_name=src_dbt_invocations.identifier,
        content=invocation_values
        )
    }}
{% endmacro -%}
