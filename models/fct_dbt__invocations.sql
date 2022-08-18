{{
    config(
        materialized='incremental',
        unique_key='command_invocation_id'
    )
}}

with base as (

    select
        *
    
    from
        {{ ref('stg_dbt__invocations') }}

    where
        1 = 1
    
    {% if target.name == 'reddev' %}
        and run_started_at > dateadd('day', -10, current_date)
    
    {% elif is_incremental() %}
        and run_started_at > (select max(run_started_at) from {{ this }})
    
    {% endif %}

),

invocations as (

    select
        command_invocation_id,
        dbt_version,
        project_name,
        run_started_at,
        dbt_command,
        full_refresh_flag,
        target_profile_name,
        target_name,
        target_database,
        target_schema,
        target_threads,
        dbt_cloud_project_id,
        dbt_cloud_job_id,
        dbt_cloud_run_id,
        dbt_cloud_run_reason_category,
        dbt_cloud_run_reason,
        env_vars,
        dbt_vars

    from
        base

)

select * from invocations
