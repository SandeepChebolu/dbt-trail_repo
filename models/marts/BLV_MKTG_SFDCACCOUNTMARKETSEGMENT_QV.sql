with projection_marketsegment_tf as (
        select id,
               isdeleted,
               name,
               currencyisocode,
               createddate,
               createdbyid,
               lastmodifieddate,
               lastmodifiedbyid,
               systemmodstamp,
               lastactivitydate,
               account__c,
               market_segment__c,
               market_sub_segment__c,
               percent_allocation__c,
               "sequence",
               precedence
          from {{ ref('BLV_MKTG_SFDCACCOUNTMARKETSEGMENT_TF') }}
       ),
       proj_user as (
        select meta_iud_flg,
               id,
               name
          from {{ ref('DLV_STAGING_USER') }}
         where (meta_iud_flg != 'D')
       ),
       join_createdby as (
        select projection_marketsegment_tf.id as id,
               projection_marketsegment_tf.isdeleted as isdeleted,
               projection_marketsegment_tf.name as name,
               projection_marketsegment_tf.currencyisocode as currencyisocode,
               projection_marketsegment_tf.createddate as createddate,
               projection_marketsegment_tf.createdbyid as createdbyid,
               projection_marketsegment_tf.lastmodifieddate as lastmodifieddate,
               projection_marketsegment_tf.lastmodifiedbyid as lastmodifiedbyid,
               projection_marketsegment_tf.systemmodstamp as systemmodstamp,
               projection_marketsegment_tf.lastactivitydate as lastactivitydate,
               projection_marketsegment_tf.account__c as account__c,
               projection_marketsegment_tf.market_segment__c as market_segment__c,
               projection_marketsegment_tf.market_sub_segment__c as market_sub_segment__c,
               projection_marketsegment_tf.percent_allocation__c as percent_allocation__c,
               projection_marketsegment_tf."sequence" as "sequence",
               projection_marketsegment_tf.precedence as precedence,
               proj_user.name as createdbyname
          from projection_marketsegment_tf
          left outer join proj_user
            on projection_marketsegment_tf.createdbyid = proj_user.id
       ),
       join_lastmodifiedby as (
        select join_createdby.id as id,
               join_createdby.isdeleted as isdeleted,
               join_createdby.name as name,
               join_createdby.currencyisocode as currencyisocode,
               join_createdby.createddate as createddate,
               join_createdby.createdbyid as createdbyid,
               join_createdby.lastmodifieddate as lastmodifieddate,
               join_createdby.lastmodifiedbyid as lastmodifiedbyid,
               join_createdby.systemmodstamp as systemmodstamp,
               join_createdby.lastactivitydate as lastactivitydate,
               join_createdby.account__c as account__c,
               join_createdby.market_segment__c as market_segment__c,
               join_createdby.market_sub_segment__c as market_sub_segment__c,
               join_createdby.percent_allocation__c as percent_allocation__c,
               join_createdby."sequence" as "sequence",
               join_createdby.precedence as precedence,
               join_createdby.createdbyname as createdbyname,
               proj_user.name as lastmodifiedbyname
          from join_createdby
          left outer join proj_user
            on join_createdby.lastmodifiedbyid = proj_user.id
       ),
       projection_3 as (
        select meta_iud_flg,
               id,
               name
          from {{ ref('DLV_STAGING_ACCOUNT') }}
         where (meta_iud_flg != 'D')
       ),
       join_account as (
        select join_lastmodifiedby.id as id,
               join_lastmodifiedby.isdeleted as isdeleted,
               join_lastmodifiedby.name as name,
               join_lastmodifiedby.currencyisocode as currencyisocode,
               join_lastmodifiedby.createddate as createddate,
               join_lastmodifiedby.createdbyid as createdbyid,
               join_lastmodifiedby.lastmodifieddate as lastmodifieddate,
               join_lastmodifiedby.lastmodifiedbyid as lastmodifiedbyid,
               join_lastmodifiedby.systemmodstamp as systemmodstamp,
               join_lastmodifiedby.lastactivitydate as lastactivitydate,
               join_lastmodifiedby.account__c as account__c,
               join_lastmodifiedby.market_segment__c as market_segment__c,
               join_lastmodifiedby.market_sub_segment__c as market_sub_segment__c,
               join_lastmodifiedby.percent_allocation__c as percent_allocation__c,
               join_lastmodifiedby."sequence" as "sequence",
               join_lastmodifiedby.precedence as precedence,
               join_lastmodifiedby.createdbyname as createdbyname,
               join_lastmodifiedby.lastmodifiedbyname as lastmodifiedbyname,
               projection_3.name as accountname
          from join_lastmodifiedby
         inner join projection_3
            on join_lastmodifiedby.account__c = projection_3.id
       ),
       aggr_mktsegcount as (
        select account__c,
               market_segment__c,
               count(market_segment__c) as market_segment__c_count
          from join_account
         group by account__c,
                  market_segment__c
       ),
       join_mktsegcnt as (
        select join_account.id as id,
               join_account.isdeleted as isdeleted,
               join_account.name as name,
               join_account.currencyisocode as currencyisocode,
               join_account.createddate as createddate,
               join_account.createdbyid as createdbyid,
               join_account.lastmodifieddate as lastmodifieddate,
               join_account.lastmodifiedbyid as lastmodifiedbyid,
               join_account.systemmodstamp as systemmodstamp,
               join_account.lastactivitydate as lastactivitydate,
               join_account.account__c as account__c,
               join_account.market_segment__c as market_segment__c,
               join_account.market_sub_segment__c as market_sub_segment__c,
               join_account.percent_allocation__c as percent_allocation__c,
               join_account."sequence" as "sequence",
               join_account.precedence as precedence,
               join_account.createdbyname as createdbyname,
               join_account.lastmodifiedbyname as lastmodifiedbyname,
               join_account.accountname as accountname,
               aggr_mktsegcount.market_segment__c_count as market_segment__c_count
          from join_account
          left outer join aggr_mktsegcount
            on join_account.account__c = aggr_mktsegcount.account__c
       ) select id as accountmarketsegmentid,
       name as accountmarketsegmentname,
       isdeleted as deleted,
       currencyisocode as currencyisocode,
       createddate as createddate,
       createdbyid as createdbyid,
       createdbyname as createdbyname,
       lastmodifieddate as lastmodifieddate,
       lastmodifiedbyid as lastmodifiedbyid,
       lastmodifiedbyname as lastmodifiedbyname,
       systemmodstamp as systemmodstamp,
       lastactivitydate as lastactivitydate,
       account__c as accountid,
       accountname as accountname,
       market_segment__c as marketsegment,
       market_sub_segment__c as marketsubsegment,
       percent_allocation__c as percentallocation,
       precedence as precedence,
       "sequence" as marketsegmentsequence,
       market_segment__c_count as marketsegmentcount
  from join_mktsegcnt
