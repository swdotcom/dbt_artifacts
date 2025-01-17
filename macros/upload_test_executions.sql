{% macro upload_test_executions(results) -%}
    {% set src_dbt_test_executions = source('dbt_artifacts', 'test_executions') %}
    {% set tests = [] %}
    {% for result in results  %}
        {% if result.node.resource_type == "test" %}
            {% do tests.append(result) %}
        {% endif %}
    {% endfor %}

    {% if tests != [] %}
        {% set test_execution_values %}
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
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(12) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(13) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(14) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(15) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(16) }}
        from values
        {% for test in tests -%}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ test.node.unique_id }}', {# unique_id #}
                '{{ run_started_at }}', {# run_started_at #}

                {% set config_full_refresh = test.node.config.full_refresh %}
                {% if config_full_refresh is none %}
                    {% set config_full_refresh = flags.FULL_REFRESH %}
                {% endif %}
                '{{ config_full_refresh }}', {# was_full_refresh #}

                '{{ test.status }}', {# status #}
                '{{ test.thread_id }}', {# thread_id #}
                {{ test.execution_time }}, {# execution_time #}
                {{ 'null' if test.failures is none else test.failures }}, {# failures #}

                {% if test.timing != [] %}
                    {% for stage in test.timing if stage.name == "compile" %}
                        {% if loop.length == 0 %}
                            null, {# compile_started_at #}
                            null, {# compile_completed_at #}
                        {% else %}
                            '{{ stage.started_at }}', {# compile_started_at #}
                            '{{ stage.completed_at }}', {# compile_completed_at #}
                        {% endif %}
                    {% endfor %}

                    {% for stage in test.timing if stage.name == "execute" %}
                        {% if loop.length == 0 %}
                            null, {# query_started_at #}
                            null, {# query_completed_at #}
                        {% else %}
                            '{{ stage.started_at }}', {# query_started_at #}
                            '{{ stage.completed_at }}', {# query_completed_at #}
                        {% endif %}
                    {% endfor %}
                {% else %}
                    null, {# compile_started_at #}
                    null, {# compile_completed_at #}
                    null, {# query_started_at #}
                    null, {# query_completed_at #}
                {% endif %}

                '{{ test.node.database }}', {# database #}
                '{{ test.node.schema }}', {# schema #}
                '{{ test.node.name }}', {# name #}
                '{{ test.node.compiled_code | replace("'","\\'") }}' {# compiled_sql #}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}

        {{ dbt_artifacts.insert_into_metadata_table(
            database_name=src_dbt_test_executions.database,
            schema_name=src_dbt_test_executions.schema,
            table_name=src_dbt_test_executions.identifier,
            content=test_execution_values
            )
        }}
    {% endif %}
{% endmacro -%}
