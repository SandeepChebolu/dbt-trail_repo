WITH source AS (
	SELECT * FROM {{ source('SAP_ECC','DD07T') }}
),
renamed AS (
	SELECT
		DOMNAME,
		DDLANGUAGE,
		AS4LOCAL,
		VALPOS,
		AS4VERS,
		DDTEXT,
		DOMVAL_LD,
		DOMVAL_HD,
		DOMVALUE_L,
		META_UPD_DT
	FROM source
)
SELECT * FROM renamed