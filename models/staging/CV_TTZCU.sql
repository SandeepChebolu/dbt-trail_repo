with
    projection_1 as (
        select sysid, mandt, tzonesys

        from {{ source('sample_poc', 't_tab_ttzcu') }}

    )
select *
from projection_1
