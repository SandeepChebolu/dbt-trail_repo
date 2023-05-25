  with cv_wf_approvals_source as (
        select client,
               wiid,
               item_guid,
               resolver_descr,
               resolver_name,
               ctime,
               step,
               status,
               etime,
               sysid
          from {{ ref('CV_WF_APPROVALS') }}
       ),
       wf_appr as (
        select sysid,
               client,
               status,
               item_guid,
               wiid,
               step,
               resolver_descr,
               ctime,
               etime,
               resolver_name,
               cast(case when status = 'APPROVED' then case when step = '2A' then '2A - Approval by Procurement' when step = '2B' then '2B - Approval by Expert' end when status = 'REJECTED' then case when step = '2A' then '2A - Rejection by Procurement' when step = '2B' then '2B - Rejection by Expert' end end as nvarchar(60)) as activity_en,
               cast(etime as TIMESTAMP) as eventtime,
               left(cast('M' as NVARCHAR), 1) as user_type,
               cast(case when status = 'APPROVED' then case when step = '2A' then 10 when step = '2B' then 20 else 0 end when status = 'REJECTED' then case when step = '2A' then 15 when step = '2B' then 25 else 0 end else 0 end as INTEGER) as sorting
          from cv_wf_approvals_source
         where status IN ('APPROVED', 'REJECTED')
           and resolver_name <> 'ADHOC'
           and step IN ('2A', '2B', '3A', '3B', '3C', '3D', '3E', '4', '4B')
       ),
       cases as (
        select case_key,
               act_run_id,
               sysid as sysid_c,
               mandt as mandt_c,
               ebeln,
               ebelp,
               srm_sysid,
               srm_mandt,
               guid_sch,
               guid_sci,
               erp,
               guid_poh,
               guid_poi,
               left(cast(srm_sysid as NVARCHAR), 3) as sysid,
               left(cast(srm_mandt as NVARCHAR), 3) as client
          from {{ ref('CV_STG_CASES') }}
         where erp = ''
       ),
       sc_appr2 as (
        select cases.case_key as case_key,
               cases.act_run_id as act_run_id,
               cases.sysid_c as sysid_c,
               cases.mandt_c as mandt_c,
               cases.srm_sysid as sysid_srm,
               cases.srm_mandt as mandt_srm,
               cases.guid_sci as guid_sci,
               wf_appr.activity_en as activity_en,
               wf_appr.eventtime as eventtime,
               wf_appr.user_type as user_type,
               wf_appr.sorting as sorting,
               left(cast('I' as NVARCHAR), 1) as level
          from cases
         inner join wf_appr
            on cases.srm_sysid = wf_appr.sysid
           and cases.srm_mandt = wf_appr.client
           and cases.guid_sci = wf_appr.item_guid
       ),
       adhoc as (
        select sysid,
               client,
               status,
               item_guid,
               wiid,
               step,
               resolver_descr,
               ctime,
               etime,
               resolver_name,
               cast(case when status = 'APPROVED' then 'Adhoc SC Approval' when status = 'REJECTED' then 'Adhoc SC Rejection' end as nvarchar(60)) as activity_en,
               cast(etime as TIMESTAMP) as eventtime,
               left(cast('M' as NVARCHAR), 1) as user_type,
               cast(case when status = 'APPROVED' then 30 when status = 'REJECTED' then 35 else 0 end as INTEGER) as sorting
          from cv_wf_approvals_source
         where status IN ('APPROVED', 'REJECTED')
           and resolver_name = 'ADHOC'
       ),
       adhoc_sc as (
        select cases.case_key as case_key,
               cases.act_run_id as act_run_id,
               cases.sysid_c as sysid_c,
               cases.mandt_c as mandt_c,
               cases.srm_sysid as sysid_srm,
               cases.srm_mandt as mandt_srm,
               cases.guid_sch as guid_sch,
               adhoc.activity_en as activity_en,
               adhoc.eventtime as eventtime,
               adhoc.user_type as user_type,
               adhoc.sorting as sorting,
               left(cast('I' as NVARCHAR), 1) as level
          from cases
         inner join adhoc
            on cases.srm_sysid = adhoc.sysid
           and cases.srm_mandt = adhoc.client
           and cases.guid_sch = adhoc.item_guid
       ),
       all_versions as (
        select cases.sysid as sysid,
               cases.client as client,
               cases.case_key as case_key,
               cases.act_run_id as act_run_id,
               cases.ebeln as ebeln,
               cases.ebelp as ebelp,
               cases.guid_poh as guid_poh,
               cases.guid_poi as guid_poi,
               cases.sysid_c as sysid_c,
               cases.mandt_c as mandt_c,
               cv_bbp_pdigp.guid as guid,
               cast(guid as varbinary(16)) as guid_pdigp
          from cases
          left outer join {{ ref('CV_BBP_PDIGP') }} as cv_bbp_pdigp
            on cases.sysid = cv_bbp_pdigp.sysid
           and cases.client = cv_bbp_pdigp.client
           and cases.guid_poi = cv_bbp_pdigp.active_item
       ),
       join_4 as (
        select all_versions.sysid as sysid,
               all_versions.client as client,
               all_versions.case_key as case_key,
               all_versions.act_run_id as act_run_id,
               all_versions.ebeln as ebeln,
               all_versions.ebelp as ebelp,
               all_versions.guid_poh as guid_poh,
               all_versions.guid_pdigp as guid_pdigp,
               all_versions.sysid_c as sysid_c,
               all_versions.mandt_c as mandt_c,
               cv_bbp_pdhgp.guid as guid
          from all_versions
          left outer join {{ ref('CV_BBP_PDHGP') }} as cv_bbp_pdhgp
            on all_versions.guid_poh = cv_bbp_pdhgp.active_header
           and all_versions.sysid = cv_bbp_pdhgp.sysid
           and all_versions.client = cv_bbp_pdhgp.client
       ),
       projection_1 as (
        select sysid,
               client,
               parent,
               number_int,
               guid,
               object_type,
               left(cast(right(number_int,5) as NVARCHAR), 5) as po_item,
               cast(guid as varbinary(16)) as guid_crmdi
          from {{ ref('CV_CRMD_ORDERADM_I') }}
       ),
       join_3 as (
        select join_4.sysid as sysid,
               join_4.client as client,
               join_4.case_key as case_key,
               join_4.act_run_id as act_run_id,
               join_4.guid_poh as guid_poh,
               join_4.guid_pdigp as guid_pdigp,
               join_4.sysid_c as sysid_c,
               join_4.mandt_c as mandt_c,
               projection_1.guid_crmdi as guid_crmdi,
               cast(coalesce(guid_crmdi, guid_pdigp) as varbinary(16)) as guid_poi
          from join_4
          left outer join projection_1
            on join_4.guid = projection_1.parent
           and join_4.ebelp = projection_1.po_item
           and join_4.sysid = projection_1.sysid
           and join_4.client = projection_1.client
       ),
       all_versions_top as ((select sysid, client, case_key, act_run_id, guid_poh, guid_poi, sysid_c, mandt_c from join_3) union all (select sysid, client, case_key, act_run_id, guid_poh, guid_poi, sysid_c, mandt_c from cases)),
       po_appr as (
        select wf_appr.wiid as wiid,
               wf_appr.step as step,
               wf_appr.status as status,
               wf_appr.etime as etime,
               wf_appr.resolver_name as resolver_name,
               wf_appr.item_guid as item_guid,
               wf_appr.sysid as sysid_srm,
               wf_appr.client as mandt_srm,
               all_versions_top.case_key as case_key,
               all_versions_top.act_run_id as act_run_id,
               all_versions_top.sysid_c as sysid_c,
               all_versions_top.mandt_c as mandt_c,
               cast(case when status = 'APPROVED' then case when step = '2B' then '2B - Approval by Expert' when step = '3C' then '3C - Proc. App. approves PO' when step = '3D' then '3D - Expert App aproves PO' when step = '3E' then '3E - Requester approves PO' when step = '4' then '4 - Mngmnt App. approves PO' when step = '4B' then '4B - Procurement Release' else step end when status = 'REJECTED' then case when step = '2B' then '2B - Expert rejects PO' when step = '3C' then '3C - Procurement rejects PO' when step = '3D' then '3D - Expert rejects PO' when step = '3E' then '3E - Requester rejects PO' when step = '4' then '4 - Management rejects PO' when step = '4B' then '4B - Procurement rejects PO' else step end end as nvarchar(60)) as activity_en,
               cast(case when status = 'APPROVED' then case when step = '2B' then 20 when step = '3C' then 145 when step = '3D' then 155 when step = '3E' then 165 when step = '4' then 175 when step = '4B' then 185 else 0 end when status = 'REJECTED' then case when step = '2B' then 25 when step = '3C' then 150 when step = '3D' then 160 when step = '3E' then 170 when step = '4' then 180 when step = '4B' then 190 else 0 end else 0 end as INTEGER) as sorting,
               left(cast('M' as NVARCHAR), 1) as user_type,
               cast(to_timestamp(etime) as TIMESTAMP) as eventtime,
               left(cast('I' as NVARCHAR), 1) as level
          from wf_appr
         inner join all_versions_top
            on wf_appr.item_guid = all_versions_top.guid_poi
           and wf_appr.sysid = all_versions_top.sysid
           and wf_appr.client = all_versions_top.client
       ),
       adhoc_po as (
        select all_versions_top.case_key as case_key,
               all_versions_top.act_run_id as act_run_id,
               all_versions_top.sysid_c as sysid_c,
               all_versions_top.mandt_c as mandt_c,
               all_versions_top.guid_poh as guid_poh,
               all_versions_top.sysid as sysid_srm,
               all_versions_top.client as mandt_srm,
               adhoc.eventtime as eventtime,
               adhoc.user_type as user_type,
               adhoc.status as status,
               cast(case when status = 'APPROVED' then 'Adhoc PO Approval' when status = 'REJECTED' then 'Adhoc PO Rejection' end as nvarchar(60)) as activity_en,
               cast(case when status = 'APPROVED' then 195 when status = 'REJECTED' then 200 else 0 end as INTEGER) as sorting,
               left(cast('I' as NVARCHAR), 1) as level
          from all_versions_top
         inner join adhoc
            on all_versions_top.guid_poh = adhoc.item_guid
           and all_versions_top.sysid = adhoc.sysid
           and all_versions_top.client = adhoc.client
       ),
       union_1 as ((select case_key, act_run_id, activity_en, eventtime, user_type, sorting, sysid_c, mandt_c, level, sysid_srm, mandt_srm from sc_appr2) union all (select case_key, act_run_id, activity_en, eventtime, user_type, sorting, sysid_c, mandt_c, level, sysid_srm, mandt_srm from adhoc_sc) union all (select case_key, act_run_id, activity_en, eventtime, user_type, sorting, sysid_c, mandt_c, level, sysid_srm, mandt_srm from po_appr) union all (select case_key, act_run_id, activity_en, eventtime, user_type, sorting, sysid_c, mandt_c, level, sysid_srm, mandt_srm from adhoc_po)),
       ttzcu as (
        select sysid,
               mandt,
               tzonesys
          from {{ ref('CV_TTZCU') }}
       ),
       timezone as (
        select union_1.case_key as case_key,
               union_1.act_run_id as act_run_id,
               union_1.sysid_c as sysid,
               union_1.mandt_c as mandt,
               union_1.activity_en as activity_en,
               union_1.eventtime as eventtime_1,
               union_1.user_type as user_type,
               union_1.sorting as sorting,
               union_1.level as level,
               union_1.sysid_srm as sysid_srm,
               union_1.mandt_srm as mandt_srm,
               ttzcu.tzonesys as tzonesys,
               cast(convert_timezone(coalesce(tzonesys, 'CET'),'UTC', eventtime_1) as TIMESTAMP) as eventtime
          from union_1
          left outer join ttzcu
            on union_1.sysid_c = ttzcu.sysid
           and union_1.mandt_c = ttzcu.mandt
       ) 
  select case_key,
       act_run_id,
       sysid,
       mandt,
       activity_en,
       eventtime,
       user_type,
       sorting,
       level,
       sysid_srm,
       mandt_srm
  from timezone