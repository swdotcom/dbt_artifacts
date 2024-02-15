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
    ('106932','8072a178dbdc6167b19235baefba2abc','Deploy dev changes','stage')
    , ('110124','36a20604aa73222006b3a8092f3fe20d','Production Incremental Github','prod')
    , ('121267','6307af420519a370927308124f56b35d','Production - Jira mart (30 min)','prod')
    , ('123360','f493b86ef5a1f0cee9dd64ba5abd12c1','dbt Observability','prod')
    -- , ('124178','be45c3fccfbf42ae415affb2a2d95fc6','GitHub Org Signup CI Test','local')
    , ('133129','4d0c33af7839dff8084a2f5a3fd6eea8','Clone dw.dw into dw.dw_staging and deploy dev changes','stage')
    -- , ('145603','c9750b65810da92397b226a5df24d87a','','')
    , ('15668','6dc53468a6a6c55d3a3fae809727bb06','Continuous Integration, main','ci')
    , ('22292','e9277b4c7ff99a69b9cc2a7083543612','Manual into dw.dw','prod')
    , ('247519','73eb0496bf33cbb8f948a857608cac67','Production - Devops Weekly','prod')
    , ('26165','df934f579f2cfbd5eadc33af86b60a6c','Production tests - codetime.external marts.github , (6am)','prod')
    , ('30243','630571ee8e61fb9efaa9786a9de27353','Production - weekly','prod')
    -- , ('332381','cc5182e7691d3296ef6438d211156344','','')
    , ('35283','f365c0e4bc0642c916d918d58d764f01','Production - 5 minute','prod')
    , ('389748','1c9c1e34f3866be23c9b975fe58058cb','Production - BitBucket (30 mins)','prod')
    , ('390979','de85abcfd1b7513615385173fe997466','dbt testing','staging')
    , ('392215','c2573aedd489943e7ec8df372ce2dc55','Manual into dw.dw_weekly','prod')
    , ('395281','bfba58a71cf043b25744f28637affe6b','Staging - Bitbucket','stage')
    , ('39769','d98420da8d2b76e41ceb98085569e395','Staging - Devops Job','stage')
    , ('40110','9414f98871ac648972b33820552aec0e','Staging - 4 hours','stage')
    , ('40502','91edf9918caf23ade612be8a563a676b','Continuous Integration , (dev)','ci')
    , ('53323','79efa8ab2a570d70cd5a4091a8343ba7','Manual into dw.dw_staging','stage')
    , ('53850','41187a739fda97dda002fc1ec8bac447','Production tests - non marts (12 hours)','prod')
    , ('56344','4579897252fd6d1afe1ff6b4313db6ce','Analytics (4 hours)','prod')
    , ('58883','3dd640b2748772a55c764c54dc8d0a27','Production - Devops','prod')
    , ('58884','7464a0f3bcc0fd653107b9764ba172ea','Production - users mart','prod')
    , ('58913','b026f93439176b320722ea7d2591a6f4','Backfill user metrics','prod')
    , ('58917','01b2a470d875b93b22f9029b8ae3eca6','Backfill GitHub metrics','prod')
    , ('org_signup','fb239d3c926ab484089fdbf9a2ecf025','GitHub Org Signup','prod')
    , ('org_signup_ci','3b9808cf16bafc8266344bb56a3672b7','GitHub Org Signup CI','ci')
    ) as _table (job_id, job_sk, name, environment_type)

)

select * from data_table
