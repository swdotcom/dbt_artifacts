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
        {{ source('dbt_artifacts', 'invocations') }}

    where
        1 = 1
    
    {% if target.name == 'reddev' %}
        and run_started_at > dateadd('day', -10, current_date)
    
    {% elif is_incremental() %}
        and run_started_at > (select dateadd('day', -1, max(run_started_at)) from {{ this }})
    
    {% endif %}

),

enhanced as (

    select
        command_invocation_id,
        coalesce(
            dbt_cloud_job_id,
            job_name,
            target_database || '.' || target_schema
        ) as job_id,
        {{ dbt_utils.surrogate_key(['job_id']) }} as job_sk,
        coalesce(
            dbt_cloud_run_id,
            run_id,
            command_invocation_id
        ) as run_id,
        {{ dbt_utils.surrogate_key(['job_run_id']) }} as run_sk
        dbt_version,
        project_name,
        run_started_at,
        dbt_command,
        full_refresh_flag as has_full_refresh_flag,
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
        job_name as core_job_id,
        run_id as core_run_id,
        env_vars,
        dbt_vars,
        selected_resources,
        row_number() over (partition by run_sk, order by base.run_started_at asc) as run_order

    from
        base

)

select * from enhanced
