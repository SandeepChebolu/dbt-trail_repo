with dd07t_source as (
        select domname,
               ddtext,
               domvalue_l,
               ddlanguage
          from {{ ref('DLV_STAGING_DD07T') }}
       ),
       projection_1 as (
        select mandt,
               kunnr,
               vkorg,
               vtweg,
               spart,
               ernam,
               erdat,
               loevm,
               versg,
               aufsd,
               kalks,
               bzirk,
               konda,
               pltyp,
               awahr,
               inco1,
               inco2,
               lifsd,
               autlf,
               antlf,
               kztlf,
               kzazu,
               lprio,
               vsbed,
               faksd,
               mrnkz,
               perrl,
               waers,
               ktgrd,
               zterm,
               vwerk,
               kvgr1,
               kvgr2,
               kvgr3,
               kvgr4,
               kvgr5,
               bokre,
               boidt,
               kurst,
               prfre,
               kkber,
               cassd,
               rdoff,
               uebto,
               untto,
               uebtk,
               pvksm,
               podkz,
               podtg,
               blind,
               _bev1_emlgpfand as _bev1_emlgpfand,
               _bev1_emlgforts as _bev1_emlgforts,
               fsh_kvgr6,
               fsh_kvgr7,
               fsh_kvgr8,
               fsh_kvgr9,
               fsh_kvgr10,
               fsh_grreg,
               fsh_sc_cid,
               fsh_vas_detc,
               fsh_vas_cg,
               fsh_grsgy,
               fsh_ss,
               fsh_msocdc,
               fsh_msopid,
               zzpar_id,
               fsh_resgy,
               zzggn,
               zzngn,
               zzsgn,
               zzkvgr6,
               zzkvgr7
          from {{ ref('DLV_STAGING_KNVV') }}
       ),
       projection_2 as (
        select domname,
               ddlanguage,
               domvalue_l,
               ddtext
          from {{ ref('DLV_STAGING_DD07T') }}
         where domname = 'ZFSH_KVGR_6'
           and ddlanguage = 'E'
       ),
       projection_3 as (
        select domname,
               ddlanguage,
               domvalue_l,
               ddtext
          from {{ ref('DLV_STAGING_DD07T') }}
         where domname = 'ZFSH_KVGR7'
           and ddlanguage = 'E'
       ),
       join_1 as (
        select projection_1.mandt as client,
               projection_1.kunnr as customernumber,
               projection_1.vkorg as salesorganization,
               projection_1.vtweg as distributionchannel,
               projection_1.spart as division,
               projection_1.ernam as nameofpersonwhocreatedtheobject,
               projection_1.erdat as dateonwhichrecordwascreated,
               projection_1.loevm as deletionflagforcustomersaleslevel,
               projection_1.versg as customerstatisticsgroup,
               projection_1.aufsd as customerorderblocksalesarea,
               projection_1.kalks as pricingprocedureassignedtothiscustomer,
               projection_1.bzirk as salesdistrict,
               projection_1.konda as pricegroupcustomer,
               projection_1.pltyp as pricelisttype,
               projection_1.awahr as orderprobabilityoftheitem,
               projection_1.inco1 as incotermspart1,
               projection_1.inco2 as incotermspart2,
               projection_1.lifsd as customerdeliveryblocksalesarea,
               projection_1.autlf as completeddeliverydefinedforeachsalesorder,
               projection_1.antlf as maximumnumberofpartialdeliveriesallowedperitem,
               projection_1.kztlf as partialdeliveryatitemlevel,
               projection_1.kzazu as ordercombinationindicator,
               projection_1.lprio as deliverypriority,
               projection_1.vsbed as shippingconditions,
               projection_1.faksd as billingblockforcustomersalesanddistribution,
               projection_1.mrnkz as manualinvoicemaintenance,
               projection_1.perrl as invoicelistschedulecalendaridentification,
               projection_1.waers as currency,
               projection_1.ktgrd as accountassignmentgroupforcustomer,
               projection_1.zterm as termsofpaymentkey,
               projection_1.vwerk as deliveringplantownorexternal,
               projection_1.kvgr1 as customergroup1,
               projection_1.kvgr2 as customergroup2,
               projection_1.kvgr3 as customergroup3,
               projection_1.kvgr4 as customergroup4,
               projection_1.kvgr5 as customergroup5,
               projection_1.bokre as indicatorcustomerisrebaterelevant,
               projection_1.boidt as startofvalidityperfortherebateindexforthecustomer,
               projection_1.kurst as exchangeratetype,
               projection_1.prfre as relevantforpricedeterminationid,
               projection_1.kkber as creditcontrolarea,
               projection_1.cassd as salesblockforcustomersalesarea,
               projection_1.rdoff as switchoffrounding,
               projection_1.uebto as overdeliverytolerancelimit,
               projection_1.uebtk as unlimitedoverdeliveryallowed,
               projection_1.untto as underdeliverytolerancelimit,
               projection_1.pvksm as customerprocedureforproductproposal,
               projection_1.podkz as relevantforpodprocessing,
               projection_1.podtg as timeframeforconfirmationofpod,
               projection_1.blind as indexstructureactiveforsubsequentsettlementinab,
               projection_1._bev1_emlgpfand as depositonempties,
               projection_1._bev1_emlgforts as indicatorforemptiesupdate,
               projection_1.fsh_kvgr6 as customergroup6,
               projection_1.fsh_kvgr7 as customergroup7,
               projection_1.fsh_kvgr8 as customergroup8,
               projection_1.fsh_kvgr9 as customergroup9,
               projection_1.fsh_kvgr10 as customergroup10,
               projection_1.fsh_grreg as groupingrule,
               projection_1.fsh_resgy as releasestrategy,
               projection_1.fsh_sc_cid as customervendorid,
               projection_1.fsh_vas_detc as vasdeterminationmode,
               projection_1.fsh_vas_cg as valueaddedservicescustomergroup,
               projection_1.fsh_grsgy as arungroupingstrategy,
               projection_1.fsh_ss as orderschedulingstrategy,
               projection_1.fsh_msocdc as distributioncenterofcustomer,
               projection_1.fsh_msopid as identificationofthecustomerinthepartnersystem,
               projection_1.zzpar_id as customernumbr,
               projection_1.zzggn as globalgroupingnumber,
               projection_1.zzngn as nationalgroupingnumber,
               projection_1.zzsgn as salesgroupingnumber,
               projection_1.zzkvgr6 as hospitaldepartment,
               projection_1.zzkvgr7 as cdclevel,
               projection_2.ddtext as hospitaldepartmenttext
          from projection_1
          left outer join projection_2
            on projection_1.zzkvgr6 = projection_2.domvalue_l
       ),
       join_2 as (
        select join_1.cdclevel as cdclevel,
               join_1.hospitaldepartment as hospitaldepartment,
               join_1.salesgroupingnumber as salesgroupingnumber,
               join_1.nationalgroupingnumber as nationalgroupingnumber,
               join_1.globalgroupingnumber as globalgroupingnumber,
               join_1.customernumbr as customernumbr,
               join_1.identificationofthecustomerinthepartnersystem as identificationofthecustomerinthepartnersystem,
               join_1.distributioncenterofcustomer as distributioncenterofcustomer,
               join_1.orderschedulingstrategy as orderschedulingstrategy,
               join_1.arungroupingstrategy as arungroupingstrategy,
               join_1.valueaddedservicescustomergroup as valueaddedservicescustomergroup,
               join_1.vasdeterminationmode as vasdeterminationmode,
               join_1.customervendorid as customervendorid,
               join_1.releasestrategy as releasestrategy,
               join_1.groupingrule as groupingrule,
               join_1.customergroup10 as customergroup10,
               join_1.customergroup9 as customergroup9,
               join_1.customergroup8 as customergroup8,
               join_1.customergroup7 as customergroup7,
               join_1.customergroup6 as customergroup6,
               join_1.indicatorforemptiesupdate as indicatorforemptiesupdate,
               join_1.depositonempties as depositonempties,
               join_1.indexstructureactiveforsubsequentsettlementinab as indexstructureactiveforsubsequentsettlementinab,
               join_1.timeframeforconfirmationofpod as timeframeforconfirmationofpod,
               join_1.relevantforpodprocessing as relevantforpodprocessing,
               join_1.customerprocedureforproductproposal as customerprocedureforproductproposal,
               join_1.underdeliverytolerancelimit as underdeliverytolerancelimit,
               join_1.unlimitedoverdeliveryallowed as unlimitedoverdeliveryallowed,
               join_1.overdeliverytolerancelimit as overdeliverytolerancelimit,
               join_1.switchoffrounding as switchoffrounding,
               join_1.salesblockforcustomersalesarea as salesblockforcustomersalesarea,
               join_1.creditcontrolarea as creditcontrolarea,
               join_1.relevantforpricedeterminationid as relevantforpricedeterminationid,
               join_1.exchangeratetype as exchangeratetype,
               join_1.startofvalidityperfortherebateindexforthecustomer as startofvalidityperfortherebateindexforthecustomer,
               join_1.indicatorcustomerisrebaterelevant as indicatorcustomerisrebaterelevant,
               join_1.customergroup5 as customergroup5,
               join_1.customergroup4 as customergroup4,
               join_1.customergroup3 as customergroup3,
               join_1.customergroup2 as customergroup2,
               join_1.customergroup1 as customergroup1,
               join_1.deliveringplantownorexternal as deliveringplantownorexternal,
               join_1.termsofpaymentkey as termsofpaymentkey,
               join_1.accountassignmentgroupforcustomer as accountassignmentgroupforcustomer,
               join_1.currency as currency,
               join_1.invoicelistschedulecalendaridentification as invoicelistschedulecalendaridentification,
               join_1.manualinvoicemaintenance as manualinvoicemaintenance,
               join_1.billingblockforcustomersalesanddistribution as billingblockforcustomersalesanddistribution,
               join_1.shippingconditions as shippingconditions,
               join_1.deliverypriority as deliverypriority,
               join_1.ordercombinationindicator as ordercombinationindicator,
               join_1.partialdeliveryatitemlevel as partialdeliveryatitemlevel,
               join_1.maximumnumberofpartialdeliveriesallowedperitem as maximumnumberofpartialdeliveriesallowedperitem,
               join_1.completeddeliverydefinedforeachsalesorder as completeddeliverydefinedforeachsalesorder,
               join_1.customerdeliveryblocksalesarea as customerdeliveryblocksalesarea,
               join_1.incotermspart2 as incotermspart2,
               join_1.incotermspart1 as incotermspart1,
               join_1.orderprobabilityoftheitem as orderprobabilityoftheitem,
               join_1.pricelisttype as pricelisttype,
               join_1.pricegroupcustomer as pricegroupcustomer,
               join_1.salesdistrict as salesdistrict,
               join_1.pricingprocedureassignedtothiscustomer as pricingprocedureassignedtothiscustomer,
               join_1.customerorderblocksalesarea as customerorderblocksalesarea,
               join_1.customerstatisticsgroup as customerstatisticsgroup,
               join_1.deletionflagforcustomersaleslevel as deletionflagforcustomersaleslevel,
               join_1.dateonwhichrecordwascreated as dateonwhichrecordwascreated,
               join_1.nameofpersonwhocreatedtheobject as nameofpersonwhocreatedtheobject,
               join_1.division as division,
               join_1.distributionchannel as distributionchannel,
               join_1.salesorganization as salesorganization,
               join_1.customernumber as customernumber,
               join_1.client as client,
               join_1.hospitaldepartmenttext as hospitaldepartmenttext,
               projection_3.ddtext as cdcleveltext
          from join_1
          left outer join projection_3
            on join_1.cdclevel = projection_3.domvalue_l
       ) select salesgroupingnumber,
       nationalgroupingnumber,
       globalgroupingnumber,
       customernumbr,
       indicatorforemptiesupdate,
       depositonempties,
       indexstructureactiveforsubsequentsettlementinab,
       relevantforpodprocessing,
       customerprocedureforproductproposal,
       underdeliverytolerancelimit,
       switchoffrounding,
       salesblockforcustomersalesarea,
       creditcontrolarea,
       relevantforpricedeterminationid,
       exchangeratetype,
       startofvalidityperfortherebateindexforthecustomer,
       indicatorcustomerisrebaterelevant,
       customergroup5,
       customergroup4,
       customergroup3,
       customergroup2,
       customergroup1,
       deliveringplantownorexternal,
       termsofpaymentkey,
       accountassignmentgroupforcustomer,
       currency,
       invoicelistschedulecalendaridentification,
       manualinvoicemaintenance,
       billingblockforcustomersalesanddistribution,
       shippingconditions,
       deliverypriority,
       ordercombinationindicator,
       partialdeliveryatitemlevel,
       maximumnumberofpartialdeliveriesallowedperitem,
       completeddeliverydefinedforeachsalesorder,
       customerdeliveryblocksalesarea,
       incotermspart2,
       incotermspart1,
       orderprobabilityoftheitem,
       pricelisttype,
       pricegroupcustomer,
       salesdistrict,
       pricingprocedureassignedtothiscustomer,
       customerorderblocksalesarea,
       customerstatisticsgroup,
       deletionflagforcustomersaleslevel,
       dateonwhichrecordwascreated,
       nameofpersonwhocreatedtheobject,
       division,
       distributionchannel,
       salesorganization,
       customernumber,
       client,
       hospitaldepartment,
       hospitaldepartmenttext,
       cdclevel,
       cdcleveltext
  from join_2