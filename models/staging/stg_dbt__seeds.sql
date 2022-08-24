{{
    config(
        materialized='incremental',
        unique_key='seed_execution_id'
    )
}}

with base as (

    select
        *
    
    from
        {{ source('dbt_artifacts', 'seeds') }}

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
        {{ dbt_utils.surrogate_key(['command_invocation_id', 'seed:unique_id']) }} as seed_execution_id,
        command_invocation_id,
        seed:unique_id as node_id,
        run_started_at,
        seed:config:materialized as materialized,
        seed:config:on_schema_change as on_schema_change,
        seed:config:"post-hook":sql as post_hook,
        seed:depends_on:nodes as depends_on_nodes,
        seed:depends_on:macros as depends_on_macros,
        seed:tags as tags,
        seed:refs as refs,
        seed:sources as sources,
        seed:database as database,
        seed:schema as schema,
        seed:name as name,
        seed:package_name as package_name,
        seed:path as path,
        seed:raw_sql as raw_sql,
        seed:checksum as checksum,
        seed:config:enabled as is_enabled,
        seed:config:full_refresh as is_full_refresh
    
    from
        base

)

select * from enhanced
