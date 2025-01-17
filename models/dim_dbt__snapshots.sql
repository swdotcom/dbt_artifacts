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
        {{ ref('stg_dbt__snapshots') }}

    where
        1 = 1
    
    {% if target.name == 'reddev' %}
        and run_started_at > dateadd('day', -10, current_date)
    
    {% elif is_incremental() %}
        and run_started_at > (select dateadd('day', -1, max(run_started_at)) from {{ this }})
    
    {% endif %}

),

snapshots as (

    select
        snapshot_execution_id
      , command_invocation_id
      , node_id
      , run_started_at
      , materialized
      , on_schema_change
      , strategy
      , check_columns
      , post_hook
      , depends_on_nodes
      , depends_on_macros
      , tags
      , refs
      , sources
      , database
      , schema
      , name
      , package_name
      , path
      , raw_sql
      , compiled_sql
      , checksum
      , is_enabled
      , is_full_refresh

    from
        base

)

select * from snapshots
