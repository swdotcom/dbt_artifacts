{% macro upload_snapshot_executions(results) -%}
    {% set src_dbt_snapshot_executions = source('dbt_artifacts', 'snapshot_executions') %}
    {% set snapshots = [] %}
    {% for result in results  %}
        {% if result.node.resource_type == "snapshot" %}
            {% do snapshots.append(result) %}
        {% endif %}
    {% endfor %}

    {% if snapshots != [] %}
        {% set snapshot_execution_values %}
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
            {{ adapter.dispatch('parse_json', 'dbt_artifacts')(adapter.dispatch('column_identifier', 'dbt_artifacts')(14)) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(15) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(16) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(17) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(18) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(19) }}
        from values
        {% for model in results if model.node.resource_type == "snapshot" -%}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ model.node.unique_id }}', {# unique_id #}
                '{{ run_started_at }}', {# run_started_at #}

                {% set config_full_refresh = model.node.config.full_refresh %}
                {% if config_full_refresh is none %}
                    {% set config_full_refresh = flags.FULL_REFRESH %}
                {% endif %}
                '{{ config_full_refresh }}', {# was_full_refresh #}

                '{{ model.status }}', {# status #}
                '{{ model.thread_id }}', {# thread_id #}
                {{ model.execution_time }}, {# execution_time #}
                '{{ model.message | replace('\\', '\\\\') | replace("'", "\\'") }}', {# message #}
                {{ 'null' if model.failures is none else model.failures }}, {# failures #}

                {% if model.timing != [] %}
                    {% for stage in model.timing if stage.name == "compile" %}
                        {% if loop.length == 0 %}
                            null, {# compile_started_at #}
                            null, {# compile_completed_at #}
                        {% else %}
                            '{{ stage.started_at }}', {# compile_started_at #}
                            '{{ stage.completed_at }}', {# compile_completed_at #}
                        {% endif %}
                    {% endfor %}

                    {% for stage in model.timing if stage.name == "execute" %}
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

                '{{ tojson(model.adapter_response) | replace('\\', '\\\\') | replace("'", "\\'") }}', {# adapter_response #}
                '{{ model.node.config.materialized }}', {# materialization #}
                '{{ model.node.database }}', {# database #}
                '{{ model.node.schema }}', {# schema #}
                '{{ model.node.name }}', {# name #}
                '{{ model.node.compiled_code | replace('\\', '\\\\') | replace("'", "\\'") }}' {# compiled_sql #}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}

        {{ dbt_artifacts.insert_into_metadata_table(
            database_name=src_dbt_snapshot_executions.database,
            schema_name=src_dbt_snapshot_executions.schema,
            table_name=src_dbt_snapshot_executions.identifier,
            content=snapshot_execution_values
            )
        }}
    {% endif %}
{% endmacro -%}
