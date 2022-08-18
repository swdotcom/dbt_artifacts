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
        {{ ref('stg_dbt__tests') }}

    where
        1 = 1
    
    {% if target.name = 'reddev ' %}
        and run_started_at > dateadd('day', -10, current_date)
    
    {% elif is_incremental %}
        and run_started_at > (select max(run_started_at) from {{ this }})
    
    {% endif %}

),

tests as (

    select
        test_execution_id,
        command_invocation_id,
        node_id,
        run_started_at,
        name,
        column_name,
        depends_on_nodes,
        depends_on_macros,
        config,
        package_name,
        test_path,
        tags

    from
        base

)

select * from tests
