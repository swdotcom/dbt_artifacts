{{
    config(
        materialized='incremental',
        unique_key='snapshot_execution_id'
    )
}}

with base as (

    select
        *
    
    from
        {{ source('dbt_artifacts', 'snapshots') }}

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
        {{ dbt_utils.surrogate_key(['command_invocation_id', 'snapshot:unique_id']) }} as snapshot_execution_id,
        command_invocation_id,
        snapshot:unique_id as node_id,
        run_started_at,
        snapshot:config:materialized as materialized,
        snapshot:config:on_schema_change as on_schema_change,
        snapshot:config:strategy as strategy,
        snapshot:config:check_cols as check_columns,
        snapshot:config:"post-hook":sql as post_hook,
        snapshot:depends_on:nodes as depends_on_nodes,
        snapshot:depends_on:macros as depends_on_macros,
        snapshot:tags as tags,
        snapshot:refs as refs,
        snapshot:sources as sources,
        snapshot:database as database,
        snapshot:schema as schema,
        snapshot:name as name,
        snapshot:package_name as package_name,
        snapshot:path as path,
        snapshot:raw_sql as raw_sql,
        snapshot:compiled_sql as compiled_sql,
        snapshot:checksum as checksum,
        snapshot:config:enabled as is_enabled,
        snapshot:config:full_refresh as is_full_refresh

    from
        base

)

select * from enhanced
