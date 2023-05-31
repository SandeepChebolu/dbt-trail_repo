/*
Aggregation_2: This node wil pass customer count = 1 for these customers we have to implement the specific logic ahead
*/ /*
Aggregation_3: If the customer count > 1 then put the same customer number for SGN and NGN

*/ /*
Join_1: Join the attributes to the custome number which have count= 1.
*/ /*
Union_1: For customer count > 1 we fill customer number in CALC_ columns and for count = 1 we'll send them as is (i.e. the way they are getting calculated from bottom).
*/ /*
Join_2: In case if all the CALC column are null or blank then put customer number else put the same value which they have
*/ with a_createmapcolumn as (
        select client, 
               customernumber, 
               salesgroupingnumber, 
               nationalgroupingnumber, 
               globalgroupingnumber, 
               cast(iff((len(salesgroupingnumber) > 0) and (len(nationalgroupingnumber) > 0) and (len(globalgroupingnumber) > 0), 111,iff((len(salesgroupingnumber) = 0) and (len(nationalgroupingnumber) > 0) and (len(globalgroupingnumber) > 0), 011,iff((len(salesgroupingnumber) > 0) and (len(nationalgroupingnumber) > 0) and (len(globalgroupingnumber) = 0), 110,iff((len(salesgroupingnumber) = 0) and (len(nationalgroupingnumber) > 0) and (len(globalgroupingnumber) = 0), 010, 000)))) as INTEGER) as calc_mapcolumn, 
               left(cast(iff(calc_mapcolumn IN (111, 110),salesgroupingnumber,iff(calc_mapcolumn IN (011, 010),nationalgroupingnumber,customernumber)) as NVARCHAR), 10) as calc_salesgroupingnumber, 
               left(cast(iff(calc_mapcolumn = 000,customernumber,nationalgroupingnumber) as NVARCHAR), 10) as calc_nationalgroupingnumber, 
               left(cast(iff(calc_mapcolumn IN (111, 011),globalgroupingnumber,iff(calc_mapcolumn IN (110, 010, 000),'',customernumber)) as NVARCHAR), 10) as calc_globalgroupingnumber 
          from {{ ref('BLV_MDM_SAPCUSTOMERSALESDATA_QV') }} 
         group by client, 
                  customernumber, 
                  salesgroupingnumber, 
                  nationalgroupingnumber, 
                  globalgroupingnumber, 
                  calc_mapcolumn, 
                  calc_salesgroupingnumber, 
                  calc_nationalgroupingnumber, 
                  calc_globalgroupingnumber
       ), 
       aggregation_2 as (
        select client, 
               customernumber, 
               count(customernumber) as agg_customercount 
          from a_createmapcolumn 
         group by client, 
                  customernumber 
        having agg_customercount = 1
       ), 
       aggregation_3 as (
        select client, 
               customernumber, 
               count(customernumber) as agg_customercount, 
               cast(to_number(agg_customercount) as INTEGER) as calc_availablehierarchycount 
          from a_createmapcolumn 
         group by client, 
                  customernumber 
        having calc_availablehierarchycount > 1
       ), 
       join_1 as (
        select a_createmapcolumn.client as client, 
               a_createmapcolumn.customernumber as customernumber, 
               a_createmapcolumn.salesgroupingnumber as csd_sourcesalesgroupingnumber, 
               a_createmapcolumn.nationalgroupingnumber as csd_sourcenationalgroupingnumber, 
               a_createmapcolumn.globalgroupingnumber as csd_sourceglobalgroupingnumber, 
               a_createmapcolumn.calc_salesgroupingnumber as calc_salesgroupingnumber, 
               a_createmapcolumn.calc_nationalgroupingnumber as calc_nationalgroupingnumber, 
               a_createmapcolumn.calc_globalgroupingnumber as calc_globalgroupingnumber 
          from a_createmapcolumn 
         inner join aggregation_2 
            on a_createmapcolumn.client = aggregation_2.client 
           and a_createmapcolumn.customernumber = aggregation_2.customernumber
       ), 
       union_1 as ((select client, customernumber, csd_sourcesalesgroupingnumber, csd_sourcenationalgroupingnumber, csd_sourceglobalgroupingnumber, calc_salesgroupingnumber, calc_nationalgroupingnumber, calc_globalgroupingnumber, '1' as calc_availablehierarchycount from join_1) union all (select client, customernumber, '' as csd_sourcesalesgroupingnumber, '' as csd_sourcenationalgroupingnumber, '' as csd_sourceglobalgroupingnumber, customernumber as calc_salesgroupingnumber, customernumber as calc_nationalgroupingnumber, '' as calc_globalgroupingnumber, calc_availablehierarchycount from aggregation_3)), 
       p_custname as (
        select client as mandt, 
               customernumber as kunnr, 
               customername as name 
          from {{ ref('BLV_MDM_SAPCUSTOMER_QV') }}
       ), 
       join_2 as (
        select union_1.csd_sourcesalesgroupingnumber as sourcesalesgroupingnumber, 
               union_1.csd_sourcenationalgroupingnumber as sourcenationalgroupingnumber, 
               union_1.csd_sourceglobalgroupingnumber as sourceglobalgroupingnumber, 
               union_1.calc_salesgroupingnumber as calc_salesgroupingnumber_hide, 
               union_1.calc_nationalgroupingnumber as calc_nationalgroupingnumber_hide, 
               union_1.calc_globalgroupingnumber as calc_globalgroupingnumber_hide, 
               union_1.calc_availablehierarchycount as calc_availablehierarchycount_hide, 
               p_custname.mandt as client, 
               p_custname.kunnr as customernumber, 
               p_custname.name as customername, 
               left(cast(iff((union_1.calc_globalgroupingnumber_hide = '' or (union_1.calc_globalgroupingnumber_hide is null)) and (union_1.calc_nationalgroupingnumber_hide = '' or (union_1.calc_nationalgroupingnumber_hide is null)) and (union_1.calc_salesgroupingnumber_hide = '' or (union_1.calc_salesgroupingnumber_hide is null)),union_1.customernumber,union_1.calc_salesgroupingnumber_hide) as NVARCHAR), 10) as salesgroupingnumber, 
               left(cast(iff((union_1.calc_globalgroupingnumber_hide = '' or (union_1.calc_globalgroupingnumber_hide is null)) and (union_1.calc_nationalgroupingnumber_hide = '' or (union_1.calc_nationalgroupingnumber_hide is null)) and (union_1.calc_salesgroupingnumber_hide = '' or (union_1.calc_salesgroupingnumber_hide is null)),union_1.customernumber,union_1.calc_nationalgroupingnumber_hide) as NVARCHAR), 10) as nationalgroupingnumber, 
               left(cast(iff((union_1.calc_globalgroupingnumber_hide = '' or (union_1.calc_globalgroupingnumber_hide is null)) and (union_1.calc_nationalgroupingnumber_hide = '' or (union_1.calc_nationalgroupingnumber_hide is null)) and (union_1.calc_salesgroupingnumber_hide = '' or (union_1.calc_salesgroupingnumber_hide is null)),'',union_1.calc_globalgroupingnumber_hide) as NVARCHAR), 10) as globalgroupingnumber,
               cast(iff((union_1.calc_availablehierarchycount_hide is null),0,union_1.calc_availablehierarchycount_hide) as INTEGER) as availablehierarchycount
          from union_1
         right outer join p_custname
            on union_1.client = p_custname.mandt
           and union_1.customernumber = p_custname.kunnr
       ),
       join_3 as (
        select join_2.client as client,
               join_2.customernumber as customernumber,
               join_2.sourceglobalgroupingnumber as sourceglobalgroupingnumber,
               join_2.sourcenationalgroupingnumber as sourcenationalgroupingnumber,
               join_2.sourcesalesgroupingnumber as sourcesalesgroupingnumber,
               join_2.salesgroupingnumber as salesgroupingnumber,
               join_2.nationalgroupingnumber as nationalgroupingnumber,
               join_2.globalgroupingnumber as globalgroupingnumber,
               join_2.availablehierarchycount as availablehierarchycount,
               join_2.customername as customername,
               p_custname.name as sourcesalesgroupingname
          from join_2
          left outer join p_custname
            on join_2.client = p_custname.mandt
           and join_2.sourcesalesgroupingnumber = p_custname.kunnr
       ),
       join_4 as (
        select join_3.client as client,
               join_3.customernumber as customernumber,
               join_3.sourcesalesgroupingnumber as sourcesalesgroupingnumber,
               join_3.sourcenationalgroupingnumber as sourcenationalgroupingnumber,
               join_3.sourceglobalgroupingnumber as sourceglobalgroupingnumber,
               join_3.salesgroupingnumber as salesgroupingnumber,
               join_3.nationalgroupingnumber as nationalgroupingnumber,
               join_3.globalgroupingnumber as globalgroupingnumber,
               join_3.availablehierarchycount as availablehierarchycount,
               join_3.customername as customername,
               join_3.sourcesalesgroupingname as sourcesalesgroupingname,
               p_custname.name as sourcenationalgroupingname
          from join_3
          left outer join p_custname
            on join_3.client = p_custname.mandt
           and join_3.sourcenationalgroupingnumber = p_custname.kunnr
       ),
       join_5 as (
        select join_4.client as client,
               join_4.customernumber as customernumber,
               join_4.sourceglobalgroupingnumber as sourceglobalgroupingnumber,
               join_4.sourcenationalgroupingnumber as sourcenationalgroupingnumber,
               join_4.sourcesalesgroupingnumber as sourcesalesgroupingnumber,
               join_4.salesgroupingnumber as salesgroupingnumber,
               join_4.nationalgroupingnumber as nationalgroupingnumber,
               join_4.globalgroupingnumber as globalgroupingnumber,
               join_4.availablehierarchycount as availablehierarchycount,
               join_4.customername as customername,
               join_4.sourcesalesgroupingname as sourcesalesgroupingname,
               join_4.sourcenationalgroupingname as sourcenationalgroupingname,
               p_custname.name as sourceglobalgroupingname
          from join_4
          left outer join p_custname
            on join_4.client = p_custname.mandt
           and join_4.sourceglobalgroupingnumber = p_custname.kunnr
       ),
       join_6 as (
        select join_5.client as client,
               join_5.customernumber as customernumber,
               join_5.sourcesalesgroupingnumber as sourcesalesgroupingnumber,
               join_5.sourcenationalgroupingnumber as sourcenationalgroupingnumber,
               join_5.sourceglobalgroupingnumber as sourceglobalgroupingnumber,
               join_5.salesgroupingnumber as salesgroupingnumber,
               join_5.nationalgroupingnumber as nationalgroupingnumber,
               join_5.globalgroupingnumber as globalgroupingnumber,
               join_5.availablehierarchycount as availablehierarchycount,
               join_5.customername as customername,
               join_5.sourcesalesgroupingname as sourcesalesgroupingname,
               join_5.sourcenationalgroupingname as sourcenationalgroupingname,
               join_5.sourceglobalgroupingname as sourceglobalgroupingname,
               p_custname.name as salesgroupingname
          from join_5
          left outer join p_custname
            on join_5.client = p_custname.mandt
           and join_5.salesgroupingnumber = p_custname.kunnr
       ),
       join_7 as (
        select join_6.client as client,
               join_6.customernumber as customernumber,
               join_6.sourceglobalgroupingnumber as sourceglobalgroupingnumber,
               join_6.sourcenationalgroupingnumber as sourcenationalgroupingnumber,
               join_6.salesgroupingnumber as salesgroupingnumber,
               join_6.nationalgroupingnumber as nationalgroupingnumber,
               join_6.globalgroupingnumber as globalgroupingnumber,
               join_6.availablehierarchycount as availablehierarchycount,
               join_6.sourcesalesgroupingnumber as sourcesalesgroupingnumber,
               join_6.customername as customername,
               join_6.sourcesalesgroupingname as sourcesalesgroupingname,
               join_6.sourcenationalgroupingname as sourcenationalgroupingname,
               join_6.sourceglobalgroupingname as sourceglobalgroupingname,
               join_6.salesgroupingname as salesgroupingname,
               p_custname.name as nationalgroupingname
          from join_6
          left outer join p_custname
            on join_6.client = p_custname.mandt
           and join_6.nationalgroupingnumber = p_custname.kunnr
       ),
       join_8 as (
        select join_7.client as client,
               join_7.customernumber as customernumber,
               join_7.sourcesalesgroupingnumber as sourcesalesgroupingnumber,
               join_7.sourcenationalgroupingnumber as sourcenationalgroupingnumber,
               join_7.sourceglobalgroupingnumber as sourceglobalgroupingnumber,
               join_7.salesgroupingnumber as salesgroupingnumber,
               join_7.nationalgroupingnumber as nationalgroupingnumber,
               join_7.globalgroupingnumber as globalgroupingnumber,
               join_7.availablehierarchycount as availablehierarchycount,
               join_7.customername as customername,
               join_7.sourcesalesgroupingname as sourcesalesgroupingname,
               join_7.sourcenationalgroupingname as sourcenationalgroupingname,
               join_7.sourceglobalgroupingname as sourceglobalgroupingname,
               join_7.salesgroupingname as salesgroupingname,
               join_7.nationalgroupingname as nationalgroupingname,
               p_custname.name as globalgroupingname
          from join_7
          left outer join p_custname
            on join_7.client = p_custname.mandt
           and join_7.globalgroupingnumber = p_custname.kunnr
       ) select client,
       customernumber,
       customername,
       sourcesalesgroupingnumber,
       sourcesalesgroupingname,
       sourcenationalgroupingnumber,
       sourcenationalgroupingname,
       sourceglobalgroupingnumber,
       sourceglobalgroupingname,
       salesgroupingnumber,
       salesgroupingname,
       nationalgroupingnumber,
       nationalgroupingname,
       globalgroupingnumber,
       globalgroupingname,
       availablehierarchycount
  from join_8