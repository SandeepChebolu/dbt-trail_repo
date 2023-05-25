select client,
       customernumber,
       customername,
       title,
       name1,
       name2,
       name3,
       name4,
       sortfield,
       housenumberandstreet,
       city,
       district,
       postalcode,
       pobox,
       poboxpostalcode,
       countycode,
       countycodedesc,
       region,
       regionname,
       countrykey,
       countryname,
       firsttelephonenumber,
       faxnumber,
       address,
       dateonwhichtherecordwascreated,
       nameofpersonwhocreatedtheobject,
       customeraccountgroup,
       customeraccountgroupdesc,
       accountnumberofvendororcreditor,
       citycoordinates,
       centraldeletionflagformasterrecord,
       taxnumber1,
       taxnumber2,
       vatregistrationnumber,
       taxjurisdiction,
       uniformresourcelocator,
       typeofbusiness,
       groupidentifier,
       sfdcaccountid,
       sfdcerpcustomeraccountcount,
       sfdcaccountname,
       sfdcaccounttype,
       sfdcrecordname,
       sfdcrecorddescription,
       sfdcparentid,
       sfdcparentname,
       sfdcindustry,
       sfdcownerid,
       sfdcownername,
       sfdcpartneraccount,
       sfdcaccountsource,
       sfdcexcludefromterritoryassignmentrules,
       sfdccreditblock,
       sfdcchannelpartnertype,
       sfdcorderblock,
       sfdcsalesblock,
       sfdcaddressstatus,
       sfdcresubmissionreason,
       sfdcaccountgroup,
       sfdcaccountstatus,
       sfdcproductfamily,
       sfdcregulatorytype,
       sfdclegacysfdcrecordid,
       sfdcpopulationgenomics,
       sfdctier,
       sfdccustomertype,
       sfdccustomersubtype,
       sfdcclinicaltype,
       sfdcprimarymarketsegment,
       sfdcprimarymarketsegmentpercentallocation,
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
       availablehierarchycount,
       winnercustomernumber,
       winnercustomername,
       winnercustomercount,
       globalregion,
       globalregionname,
       sfdcentityacquisitionid
  from {{ ref('BLV_ILMN_MDM_CUSTOMERMASTER_QV') }}