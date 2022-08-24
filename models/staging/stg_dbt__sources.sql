{{
    config(
        materialized='incremental',
        unique_key='source_execution_id'
    )
}}

with base as (

    select
        *
    
    from
        {{ source('dbt_artifacts', 'sources') }}

    where
        1 = 1
    
    {% if target.name == 'reddev' %}
        and run_started_at > dateadd('day', -10, current_date)
    
    {% elif is_incremental() %}
        and run_started_at > (select max(run_started_at) from {{ this }})
    
    {% endif %}

),

enhanced as (

    select
        {{ dbt_utils.surrogate_key(['command_invocation_id', 'source:unique_id']) }} as source_execution_id,
        command_invocation_id,
        source:unique_id as node_id,
        run_started_at,
        source:depends_on:nodes as depends_on_nodes,
        source:depends_on:macros as depends_on_macros,
        source:tags as tags,
        source:refs as refs,
        source:loader as loader,
        source:source_name as source_name,
        source:name as name,
        source:package_name as package_name,
        source:path as path,
        source:checksum as checksum,
        source:freshness:warn_after:count as freshness_warn_after_count,
        source:freshness:warn_after:period as freshness_warn_after_period,
        source:freshness:error_after:count as freshness_error_after_count,
        source:freshness:error_after:period as freshness_error_after_period,
        source:freshness:filter as freshness_filter,
        source:config:enabled as is_enabled,
        source:config:full_refresh as is_full_refresh

    from
        base

)

select * from enhanced
