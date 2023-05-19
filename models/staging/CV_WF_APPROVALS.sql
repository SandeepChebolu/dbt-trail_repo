with
    projection_1 as (
        select
            client,
            wiid,
            item_guid,
            resolver_descr,
            resolver_name,
            ctime,
            step,
            status,
            etime,
            sysid
        from {{ source('sample_poc', 't_tab_wf_approvals') }}
    )
select *
from projection_1