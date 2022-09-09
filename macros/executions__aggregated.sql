{% macro executions__aggregated(granularity_field='command_invocation_id') %}

{{
    config(
        materialized='incremental',
        unique_key=granularity_field
    )
}}

with invocations as (

    select
        {{ granularity_field }}
    
    from
        {{ ref('stg_dbt__invocations') }}

    where
        1 = 1
    
    {% if target.name == 'reddev' %}
        and run_started_at > dateadd('day', -10, current_date)
    
    {% elif is_incremental() %}
        and run_started_at > (select dateadd('day', -1, max(run_started_at)) from {{ this }})
    
    {% endif %}

),

model_executions as (

    select
        models.{{ granularity_field }}
      , count(distinct models.node_id) as models
      , sum(models.compile_execution_time) as compile_execution_time
      , sum(models.query_execution_time) as query_execution_time
      , sum(models.execution_time) as execution_time
      , min(invocations.run_started_at) as run_started_at
      , max(models.query_completed_at) as last_query_completed_at
      , max(models.run_order) as max_run_order

    from
        {{ ref('stg_dbt__model_executions') }} as models
    inner join
        invocations
        on models.{{ granularity_field }} = invocations.{{ granularity_field }}

    group by 1

),

seed_executions as (

    select
        seeds.{{ granularity_field }}
      , count(distinct seeds.node_id) as seeds
      , sum(seeds.compile_execution_time) as compile_execution_time
      , sum(seeds.query_execution_time) as query_execution_time
      , sum(seeds.execution_time) as execution_time
      , min(invocations.run_started_at) as run_started_at
      , max(seeds.query_completed_at) as last_query_completed_at
      , max(models.run_order) as max_run_order

    from {{ ref('stg_dbt__seed_executions') }} as seeds
    inner join
        invocations
        on seeds.{{ granularity_field }} = invocations.{{ granularity_field }}

    group by 1

),

snapshot_executions as (

    select
        snapshots.{{ granularity_field }}
      , count(distinct snapshots.node_id) as snapshots
      , sum(snapshots.compile_execution_time) as compile_execution_time
      , sum(snapshots.query_execution_time) as query_execution_time
      , sum(snapshots.execution_time) as execution_time
      , min(invocations.run_started_at) as run_started_at
      , max(snapshots.query_completed_at) as last_query_completed_at
      , max(models.run_order) as max_run_order

    from {{ ref('stg_dbt__snapshot_executions') }} as snapshots
    inner join
        invocations
        on snapshots.{{ granularity_field }} = invocations.{{ granularity_field }}

    group by 1

),

test_executions as (

    select
        tests.{{ granularity_field }}
      , count(distinct tests.node_id) as tests
      , sum(tests.compile_execution_time) as compile_execution_time
      , sum(tests.query_execution_time) as query_execution_time
      , sum(tests.execution_time) as execution_time
      , min(invocations.run_started_at) as run_started_at
      , max(tests.query_completed_at) as last_query_completed_at
      , max(models.run_order) as max_run_order

    from {{ ref('stg_dbt__seed_executions') }} as tests
    inner join
        invocations
        on tests.{{ granularity_field }} = invocations.{{ granularity_field }}

    group by 1

),

last_query_union as (

    select {{ granularity_field }}, run_started_at, last_query_completed_at, run_order from model_executions
    union all
    select {{ granularity_field }}, run_started_at, last_query_completed_at, run_order from test_executions
    union all
    select {{ granularity_field }}, run_started_at, last_query_completed_at, run_order from snapshot_executions
    union all
    select {{ granularity_field }}, run_started_at, last_query_completed_at, run_order from model_executions

),

start_end as (

    select
        {{ granularity_field }}
      , min(run_started_at) as run_started_at
      , max(last_query_completed_at) as run_ended_at
      , max(run_order) as invocations

    from last_query_union

    group by 1

),

final as (

    select
        invocations.{{ granularity_field }}
      , start_end.run_started_at
      , start_end.run_ended_at
      , model_executions.models
      , test_executions.tests
      , snapshot_executions.snapshots
      , seed_executions.seeds
      , model_executions.compile_execution_time as compile_execution_time_models
      , model_executions.query_execution_time as query_execution_time_models
      , model_executions.execution_time as execution_time_models
      , test_executions.compile_execution_time as compile_execution_time_tests
      , test_executions.query_execution_time as query_execution_time_tests
      , test_executions.execution_time as execution_time_tests
      , snapshot_executions.compile_execution_time as compile_execution_time_snapshots
      , snapshot_executions.query_execution_time as query_execution_time_snapshots
      , snapshot_executions.execution_time as execution_time_snapshots
      , seed_executions.compile_execution_time as compile_execution_time_seeds
      , seed_executions.query_execution_time as query_execution_time_seeds
      , seed_executions.execution_time as execution_time_seeds
      , zeroifnull(model_executions.compile_execution_time) +
        zeroifnull(test_executions.compile_execution_time) +
        zeroifnull(snapshot_executions.compile_execution_time) +
        zeroifnull(seed_executions.compile_execution_time) as compile_execution_time
      , zeroifnull(model_executions.query_execution_time) +
        zeroifnull(test_executions.query_execution_time) +
        zeroifnull(snapshot_executions.query_execution_time) +
        zeroifnull(seed_executions.query_execution_time) as query_execution_time
      , zeroifnull(model_executions.execution_time) +
        zeroifnull(test_executions.execution_time) +
        zeroifnull(snapshot_executions.execution_time) +
        zeroifnull(seed_executions.execution_time) as execution_time

    from invocations
    left join start_end
        on invocations.{{ granularity_field }} = start_end.{{ granularity_field }}
    left join model_executions
        on invocations.{{ granularity_field }} = model_executions.{{ granularity_field }}
    left join test_executions
        on invocations.{{ granularity_field }} = test_executions.{{ granularity_field }}
    left join snapshot_executions
        on invocations.{{ granularity_field }} = snapshot_executions.{{ granularity_field }}
    left join seed_executions
        on invocations.{{ granularity_field }} = seed_executions.{{ granularity_field }}        

)

select * from final

{% endmacro %}