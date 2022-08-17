with base as (

    select *
    from {{ source('dbt_artifacts', 'snapshot_executions') }}

),

enhanced as (

    select
        {{ dbt_utils.surrogate_key(['command_invocation_id', 'node_id']) }} as snapshot_execution_id,
        command_invocation_id,
        node_id,
        query_id,
        run_started_at,
        was_full_refresh,
        split(thread_id, '-')[1]::int as thread_id,
        status,
        compile_started_at,
        compile_completed_at,
        query_started_at
        query_completed_at,
        execution_time,
        rows_affected,
        materialization,
        database,
        schema,
        name
    from base

)

select * from enhanced
