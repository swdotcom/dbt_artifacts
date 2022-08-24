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
        {{ source('dbt_artifacts', 'snapshot_executions') }}

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
        {{ dbt_utils.surrogate_key(['command_invocation_id', 'unique_id']) }} as snapshot_execution_id,
        command_invocation_id,
        unique_id as node_id,
        adapter_response:query_id as query_id,
        split(thread_id, '-')[1]::int as thread_id,
        run_started_at,
        compile_started_at,
        compile_completed_at,
        datediff('millisecond', compile_started_at, compile_completed_at) / 1000 as compile_execution_time,
        query_started_at,
        query_completed_at,
        datediff('millisecond', query_started_at, query_completed_at) / 1000 as query_execution_time,
        execution_time,
        status,
        adapter_response:rows_affected as rows_affected,
        failures,
        materialization,
        database,
        schema,
        name,
        compiled_sql,
        was_full_refresh

    from
        base

)

select * from enhanced
