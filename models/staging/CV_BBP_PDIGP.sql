with
    projection_1 as (
        select sysid, client, active_item, guid

        from {{ source('sample_poc', 't_tab_bbp_pdigp') }}

    )
select *
from projection_1
