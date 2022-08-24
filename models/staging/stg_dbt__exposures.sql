{{
    config(
        materialized='incremental',
        unique_key='exposure_execution_id'
    )
}}

with base as (

    select
        *
    
    from
        {{ source('dbt_artifacts', 'exposures') }}

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
        {{ dbt_utils.surrogate_key(['command_invocation_id', 'exposure::unique_id']) }} as exposure_execution_id,
        command_invocation_id,
        exposure:unique_id as node_id,
        run_started_at

    from
        base

)

select * from enhanced
