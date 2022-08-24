{{
    config(
        materialized='table'
    )
}}

with base as (

    select * from {{ ref('stg_dbt__models') }}

),

model_executions as (

    select * from {{ ref('stg_dbt__model_executions') }}

),

latest_models as (
    
    select
        * 
        
    from
        base 
        
    where
        run_started_at = (select max(run_started_at) from base)

),

latest_model_stats as (

    select
        model_executions.node_id
        , max(case when model_executions.was_full_refresh then model_executions.query_completed_at end) as last_full_refresh_run_completed_at
        , max(case when model_executions.was_full_refresh then model_executions.execution_time end) as last_full_refresh_run_execution_time
        , max(case when model_executions.was_full_refresh then model_executions.rows_affected end) as last_full_refresh_run_rows_affected
        , max(model_executions.query_completed_at) as last_run_completed_at
        , max(model_executions.execution_time) as last_run_execution_time
        , max(model_executions.rows_affected) as last_run_rows_affected
    
    from
        model_executions
    inner join
        latest_models 
        on model_executions.node_id = latest_models.node_id
    
    group by 1

    qualify
        row_number() over (
            partition by latest_models.node_id, model_executions.was_full_refresh
            order by model_executions.query_completed_at desc
        ) = 1

),

final as (

    select
        latest_models.*
        , latest_model_stats.last_full_refresh_run_completed_at
        , latest_model_stats.last_full_refresh_run_execution_time
        , latest_model_stats.last_full_refresh_run_rows_affected
        , latest_model_stats.last_run_completed_at
        , latest_model_stats.last_run_execution_time
        , latest_model_stats.last_run_rows_affected
    
    from
        latest_models
    left join
        latest_model_stats
        on latest_models.node_id = latest_model_stats.node_id

)

select * from final
