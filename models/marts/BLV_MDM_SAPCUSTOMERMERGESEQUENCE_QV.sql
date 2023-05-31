/*
Agg_KNVP: Distinct of Loser (KUNN2) And Winner (KUNNR)
*/ with prj_knvp as (
        select mandt,
               kunnr,
               parvw,
               kunn2,
               left(cast(rtrim(ltrim(kunnr)) as NVARCHAR), 10) as kunnr_c,
               left(cast(rtrim(ltrim(kunn2)) as NVARCHAR), 10) as kunn2_c,
               left(cast(rtrim(ltrim(parvw)) as NVARCHAR), 2) as parvw_c
          from {{ ref('DLV_STAGING_KNVP') }}
         where (parvw_c = 'ZL')
       ),
       agg_knvp as (
        select mandt,
               kunn2_c,
               kunnr_c
          from prj_knvp
         group by mandt,
                  kunn2_c,
                  kunnr_c
       ),
       agg_winncnt4loser as (
        select mandt,
               kunn2_c,
               count(kunnr_c) as kunnr_c_count
          from agg_knvp
         group by mandt,
                  kunn2_c
       ),
       jnr_ as (
        select agg_knvp.mandt as mandt,
               agg_knvp.kunn2_c as kunn2_c,
               agg_knvp.kunnr_c as kunnr_c,
               agg_winncnt4loser.kunnr_c_count as kunnr_c_count
          from agg_knvp
         inner join agg_winncnt4loser
            on agg_knvp.mandt = agg_winncnt4loser.mandt
           and agg_knvp.kunn2_c = agg_winncnt4loser.kunn2_c
       ),
       rnk_ as (
        select mandt,
               kunn2_c,
               kunnr_c,
               kunnr_c_count,
               row_number() over (partition by kunn2_c order by kunnr_c asc) as rnk
          from jnr_ qualify rnk <= 100
       ) select mandt as client,
       kunn2_c as losercustomernumber,
       kunnr_c as winnercustomernumber,
       kunnr_c_count as winnercustomercount,
       rnk as winnercustomersequence
  from rnk_