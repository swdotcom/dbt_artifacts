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
        {{ ref('stg_dbt__test_executions') }}

    where
        1 = 1
    
    {% if target.name == 'reddev' %}
        and run_started_at > dateadd('day', -10, current_date)
    
    {% elif is_incremental %}
        and run_started_at > (select max(run_started_at) from {{ this }})
    
    {% endif %}

),

test_executions as (

    select
        test_execution_id,
        command_invocation_id,
        node_id,
        run_started_at,
        was_full_refresh,
        thread_id,
        status,
        compile_started_at,
        compile_completed_at,
        query_started_at,
        query_completed_at,
        execution_time,
        failures,
        compiled_sql

    from
        base

)

select * from test_executions
