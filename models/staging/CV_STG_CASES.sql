with
    projection_1 as (
        select
            case_key,
            act_run_id,
            sysid,
            mandt,
            ebeln,
            ebelp,
            srm_sysid,
            srm_mandt,
            guid_sch,
            guid_sci,
            erp,
            guid_poh,
            guid_poi
        from {{ source('sample_poc', 't_tab_cases') }}

    )
select *
from projection_1
