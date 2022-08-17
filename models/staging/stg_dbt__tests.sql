with base as (

    select *
    from {{ source('dbt_artifacts', 'tests') }}

),

enhanced as (

    select
        {{ dbt_utils.surrogate_key(['command_invocation_id', 'node_id']) }} as test_execution_id,
        command_invocation_id,
        node_id,
        run_started_at,
        name,
        column_name,
        depends_on_nodes,
        depends_on_macros,
        config,
        package_name,
        test_path,
        tags
    from base

)

select * from enhanced
