{% macro upload_sources(graph) -%}
    {% set src_dbt_sources = source('dbt_artifacts', 'sources') %}
    {% set sources = [] %}
    {% for node in graph.sources.values() %}
        {% do sources.append(node) %}
    {% endfor %}

    {% if sources != [] %}
        {% set source_values %}
        select
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(1) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(2) }},
            {{ adapter.dispatch('parse_json', 'dbt_artifacts')(adapter.dispatch('column_identifier', 'dbt_artifacts')(3)) }}
        from values
        {% for source in sources -%}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ run_started_at }}', {# run_started_at #}
                '{{ tojson(source) | replace('\\', '\\\\') | replace("'", "\\'") }}' {# source #}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}

        {{ dbt_artifacts.insert_into_metadata_table(
            database_name=src_dbt_sources.database,
            schema_name=src_dbt_sources.schema,
            table_name=src_dbt_sources.identifier,
            content=source_values
            )
        }}
    {% endif %}
{% endmacro -%}
