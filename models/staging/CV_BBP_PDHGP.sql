with
    projection_1 as (
        select sysid, client, active_header, guid

        from {{ source('sample_poc', 't_tab_bbp_pdhgp') }}

    )
select *
from projection_1
