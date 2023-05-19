with
    projection_1 as (
        select sysid, client, parent, number_int, guid, object_type

        from {{ source('sample_poc', 't_tab_crmd_orderadm_i') }}

    )
select *
from projection_1