with Var_Account as 
(
select MS.ID
      ,MS.ISDELETED
	  ,MS.NAME,MS.CURRENCYISOCODE
	  ,MS.CREATEDDATE
	  ,MS.CREATEDBYID
	  ,MS.LASTMODIFIEDDATE
	  ,MS.LASTMODIFIEDBYID
	  ,MS.SYSTEMMODSTAMP
	  ,MS.LASTACTIVITYDATE
	  ,MS.ACCOUNT__C
	  ,MS.MARKET_SEGMENT__C
	  ,MS.MARKET_SUB_SEGMENT__C
	  ,MS.PERCENT_ALLOCATION__C
	  ,ROW_NUMBER() OVER (PARTITION BY ACCOUNT__C ORDER BY MS.PERCENT_ALLOCATION__c DESC , MSP.PRECEDENCE  ASC ,MS.LASTMODIFIEDDATE DESC ) AS "Sequence" 
	  , MSP.PRECEDENCE
from {{ source('REP_ILMN_INFC_SFDC','MARKET_SEGMENT__C') }} MS 
LEFT OUTER JOIN (SELECT * FROM {{ source('REP_ILMN_INFC_SFDC','MRKTSEGPICKLISTORDER') }} WHERE META_IUD_FLG<> 'D') MSP 
  ON  MS.MARKET_SEGMENT__C =  MSP.MARKETSEGMENTNAME
Where MS.META_IUD_FLG<> 'D' Order by 1
)
select * from Var_Account