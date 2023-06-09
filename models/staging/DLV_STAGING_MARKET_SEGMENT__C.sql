WITH source AS (
	SELECT * FROM {{ source('REP_ILMN_INFC_SFDC','MARKET_SEGMENT__C') }}
),
renamed AS (
	SELECT
		META_CRT_DT,
		META_UPD_DT,
		META_IUD_FLG,
		META_SRC_NM,
		ID,
		ISDELETED,
		NAME,
		CURRENCYISOCODE,
		CREATEDDATE,
		CREATEDBYID,
		LASTMODIFIEDDATE,
		LASTMODIFIEDBYID,
		SYSTEMMODSTAMP,
		LASTACTIVITYDATE,
		ACCOUNT__C,
		MARKET_SEGMENT__C,
		MARKET_SUB_SEGMENT__C,
		PERCENT_ALLOCATION__C
	FROM source
)
SELECT * FROM renamed