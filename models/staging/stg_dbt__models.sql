{{
    config(
        materialized='incremental',
        unique_key='model_execution_id'
    )
}}

with base as (

    select
        *
    
    from
        {{ source('dbt_artifacts', 'models') }}

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
        {{ dbt_utils.surrogate_key(['command_invocation_id', 'model:unique_id']) }} as model_execution_id,
        command_invocation_id,
        model:unique_id as node_id,
        run_started_at,
        model:config:materialized as materialized,
        model:config:on_schema_change as on_schema_change,
        model:config:"post-hook":sql as post_hook,
        model:depends_on:nodes as depends_on_nodes,
        model:depends_on:macros as depends_on_macros,
        model:tags as tags,
        model:refs as refs,
        model:sources as sources,
        model:database as database,
        model:schema as schema,
        model:name as name,
        model:package_name as package_name,
        model:path as path,
        model:raw_sql as raw_sql,
        model:compiled_sql as compiled_sql,
        model:checksum as checksum,
        model:config:enabled as is_enabled,
        model:config:full_refresh as is_full_refresh
    
    from
        base

)

select * from enhanced
