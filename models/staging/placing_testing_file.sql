with
    projection_1 as (
        select
            sysid,
            mandt,
            ebeln,
            ebelp,
            zterm_po,
            zbd1t_po,
            zbd2t_po,
            zbd3t_po,
            zbd1p_po,
            zbd2p_po,
            zterm_md_po,
            zbd1t_md_po,
            zbd2t_md_po,
            zbd3t_md_po,
            zbd1p_md_po,
            zbd2p_md_po,
            awkey,
            fixed_day_md
        from {{ source('SAMPLE_POC', 'T_CV_HQ_VIEW_PO') }}
    )
select *
from projection_1
