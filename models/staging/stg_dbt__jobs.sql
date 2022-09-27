{{
    config(
        materialized='table'
    )
}}

with data_table as (
    
    select
        *
        
    from (
        values
        ('36a20604aa73222006b3a8092f3fe20d', 'Production Incremental Github', 'prod')
      , ('6307af420519a370927308124f56b35d', 'Production - Jira mart (30 min)', 'prod')
      , ('f493b86ef5a1f0cee9dd64ba5abd12c1', 'dbt Observability', 'prod')
      , ('be45c3fccfbf42ae415affb2a2d95fc6', 'GitHub Org Signup CI Test', 'local')
      , ('6dc53468a6a6c55d3a3fae809727bb06', 'Continuous Integration (main)', 'ci')
      , ('e9277b4c7ff99a69b9cc2a7083543612', 'Manual into dw.dw', 'prod')
      , ('df934f579f2cfbd5eadc33af86b60a6c', 'Production tests - codetime.external marts.github (6am)', 'prod')
      , ('630571ee8e61fb9efaa9786a9de27353', 'Production - weekly', 'prod')
      , ('f365c0e4bc0642c916d918d58d764f01', 'Production (5 minutes)', 'prod')
      , ('91edf9918caf23ade612be8a563a676b', 'Continuous Integration (dev)', 'ci')
      , ('79efa8ab2a570d70cd5a4091a8343ba7', 'Manual into dw.dw_staging', 'stage')
      , ('41187a739fda97dda002fc1ec8bac447', 'Production tests - non marts (12 hours)', 'prod')
      , ('4579897252fd6d1afe1ff6b4313db6ce', 'Analytics (4 hours)', 'prod')
      , ('3dd640b2748772a55c764c54dc8d0a27', 'Production - GitHub mart (30 mins)', 'prod')
      , ('7464a0f3bcc0fd653107b9764ba172ea', 'Production - users mart (30 mins)', 'prod')
      , ('01b2a470d875b93b22f9029b8ae3eca6', 'Backfill GitHub metrics', 'prod')
      , ('8ec6cd52b9ba5eda90beb3ff84688078', 'GitHub Org Signup', 'prod')
      , ('dc4c1bb00aaf01e8a147e3ffa0c93be3', 'GitHub Org Signup CI', 'ci')
      , ('e5c6c01d58e904d89d1929d2193da101', 'Whitney local dev', 'local')
      , ('edd4b90229084f9b5b42d84b91c69c6b', 'Org Signup local test', 'local')
      , ('b5d4ed6284cf404f7ad152f36da76e6b', 'Michael local dev', 'local')
      , ('d98420da8d2b76e41ceb98085569e395', 'Staging - external (3 hours)', 'stage')
      , ('9414f98871ac648972b33820552aec0e', 'Staging - internal (8 hours)', 'stage')
      , ('8072a178dbdc6167b19235baefba2abc', 'Deploy dev changes', 'stage')
      , ('4d0c33af7839dff8084a2f5a3fd6eea8', 'Clone dw.dw into dw.dw_staging and deploy dev changes', 'stage')
    ) as _table (job_sk, name, environment)

)

select * from data_table
