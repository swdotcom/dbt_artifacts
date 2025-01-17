{% macro executions__aggregated(granularity_field='command_invocation_id') %}

{{
    config(
        materialized='incremental',
        unique_key=granularity_field
    )
}}

with invocations as (

    select
        *
    
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

base as (

    select distinct {{ granularity_field }} from invocations

),

max_run_order as (

    select
        {{ granularity_field }}
      , max(run_order) as invocations
    
    from invocations
    
    group by 1

),

model_executions as (

    select
        invocations.{{ granularity_field }}
      , count(distinct models.node_id) as models
      , sum(case when models.status = 'success' then 1 end) as successes
      , sum(case when models.status = 'error' then 1 end) as errors
      , sum(case when models.status = 'skipped' then 1 end) as skips
      , sum(models.compile_execution_time) as compile_execution_time
      , sum(models.query_execution_time) as query_execution_time
      , sum(models.execution_time) as execution_time
      , max(models.query_completed_at) as last_query_completed_at


    from
        {{ ref('stg_dbt__model_executions') }} as models
    inner join
        invocations
        on models.command_invocation_id = invocations.command_invocation_id

    group by 1

),

seed_executions as (

    select
        invocations.{{ granularity_field }}
      , count(distinct seeds.node_id) as seeds
      , sum(seeds.compile_execution_time) as compile_execution_time
      , sum(seeds.query_execution_time) as query_execution_time
      , sum(seeds.execution_time) as execution_time
      , max(seeds.query_completed_at) as last_query_completed_at
      , array_agg(distinct seeds.status) within group (order by seeds.status) as status_array

    from {{ ref('stg_dbt__seed_executions') }} as seeds
    inner join
        invocations
        on seeds.command_invocation_id = invocations.command_invocation_id

    group by 1

),

snapshot_executions as (

    select
        invocations.{{ granularity_field }}
      , count(distinct snapshots.node_id) as snapshots
      , sum(snapshots.compile_execution_time) as compile_execution_time
      , sum(snapshots.query_execution_time) as query_execution_time
      , sum(snapshots.execution_time) as execution_time
      , max(snapshots.query_completed_at) as last_query_completed_at
      , array_agg(distinct snapshots.status) within group (order by snapshots.status) as status_array

    from {{ ref('stg_dbt__snapshot_executions') }} as snapshots
    inner join
        invocations
        on snapshots.command_invocation_id = invocations.command_invocation_id

    group by 1

),

test_executions as (

    select
        invocations.{{ granularity_field }}
      , count(distinct tests.node_id) as tests
      , sum(case when tests.status = 'pass' then 1 end) as passes
      , sum(case when tests.status = 'fail' then 1 end) as fails
      , sum(case when tests.status = 'warn' then 1 end) as skips
      , sum(case when tests.status = 'error' then 1 end) as errors
      , sum(tests.compile_execution_time) as compile_execution_time
      , sum(tests.query_execution_time) as query_execution_time
      , sum(tests.execution_time) as execution_time
      , max(tests.query_completed_at) as last_query_completed_at
      , array_agg(distinct tests.status) within group (order by tests.status) as status_array

    from {{ ref('stg_dbt__test_executions') }} as tests
    inner join
        invocations
        on tests.command_invocation_id = invocations.command_invocation_id

    group by 1

),

last_query_union as (

    select {{ granularity_field }}, last_query_completed_at from model_executions
    union all
    select {{ granularity_field }}, last_query_completed_at from test_executions
    union all
    select {{ granularity_field }}, last_query_completed_at from snapshot_executions
    union all
    select {{ granularity_field }}, last_query_completed_at from model_executions

),

run_end as (

    select
        {{ granularity_field }}
      , max(last_query_completed_at) as run_ended_at

    from last_query_union

    group by 1

),

run_start as (

    select
        {{ granularity_field }}
      , min(run_started_at) as run_started_at

    from invocations

    group by 1

),

final as (

    select
        base.{{ granularity_field }}
      , run_start.run_started_at
      , run_end.run_ended_at
      , max_run_order.invocations
      , ifnull(model_executions.models, 0) as models
      , ifnull(model_executions.successes, 0) as model_successes
      , ifnull(model_executions.errors, 0) as model_errors
      , ifnull(model_executions.skips, 0) as model_skips
      , ifnull(test_executions.tests, 0) as tests
      , ifnull(test_executions.passes, 0) as test_passes
      , ifnull(test_executions.fails, 0) as test_fails
      , ifnull(test_executions.skips, 0) as test_skips
      , ifnull(test_executions.errors, 0) as test_errors
      , ifnull(snapshot_executions.snapshots, 0) as snapshots
      , ifnull(seed_executions.seeds, 0) as seeds
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
      , iff(model_errors = 0
            and test_fails = 0
            and test_errors = 0,
            True,
            False)::boolean as is_successful

    from base
    left join run_start
        on base.{{ granularity_field }} = run_start.{{ granularity_field }}
    left join run_end
        on base.{{ granularity_field }} = run_end.{{ granularity_field }}
    left join max_run_order
        on base.{{ granularity_field }} = max_run_order.{{ granularity_field }}
    left join model_executions
        on base.{{ granularity_field }} = model_executions.{{ granularity_field }}
    left join test_executions
        on base.{{ granularity_field }} = test_executions.{{ granularity_field }}
    left join snapshot_executions
        on base.{{ granularity_field }} = snapshot_executions.{{ granularity_field }}
    left join seed_executions
        on base.{{ granularity_field }} = seed_executions.{{ granularity_field }}        

)

select * from final

{% endmacro %}