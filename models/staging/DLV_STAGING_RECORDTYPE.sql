WITH source AS (
	SELECT * FROM {{ source('REP_ILMN_INFC_SFDC','RECORDTYPE') }}
),
renamed AS (
	SELECT
		META_CRT_DT,
		META_UPD_DT,
		META_IUD_FLG,
		META_SRC_NM,
		ID,
		NAME,
		DEVELOPERNAME,
		NAMESPACEPREFIX,
		DESCRIPTION,
		BUSINESSPROCESSID,
		SOBJECTTYPE,
		ISACTIVE,
		CREATEDBYID,
		CREATEDDATE,
		LASTMODIFIEDBYID,
		LASTMODIFIEDDATE,
		SYSTEMMODSTAMP
	FROM source
)
SELECT * FROM renamed