with base as (

    select *
    from {{ ref('stg_dbt__test_executions') }}

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
    from base

)

select * from test_executions
