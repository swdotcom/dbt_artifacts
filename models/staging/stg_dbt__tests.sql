{{
    config(
        materialized='incremental',
        unique_key='test_execution_id'
    )
}}

with base as (

    select
        *
    
    from
        {{ source('dbt_artifacts', 'tests') }}

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
        {{ dbt_utils.surrogate_key(['command_invocation_id', 'test:node_id']) }} as test_execution_id,
        command_invocation_id,
        test:unique_id as node_id,
        test:test_metadata:name as test_name,
        test:test_metadata:kwargs:column_name as column_name,
        run_started_at,
        test:config:materialized as materialized,
        test:config:severity as severity,
        test:config:where as where_clause,
        test:config:limit as limit,
        test:config:fail_calc as fail_calculation,
        test:config:warn_if as warn_if,
        test:config:error_if as error_if,
        test:depends_on:nodes as depends_on_nodes,
        test:depends_on:macros as depends_on_macros,
        test:tags as tags,
        test:refs as refs,
        test:sources as sources,
        test:database as database,
        test:schema as schema,
        test:name as name,
        test:package_name as package_name,
        test:path as path,
        test:compiled_sql as compiled_sql,
        test:checksum as checksum,
        test:config:enabled as is_enabled,
        test:config:full_refresh as is_full_refresh

    from
        base

)

select * from enhanced
