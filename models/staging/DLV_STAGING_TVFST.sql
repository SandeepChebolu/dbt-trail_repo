WITH source AS (
	SELECT * FROM {{ source('SAP_ECC','TVFST') }}
),
renamed AS (
	SELECT
		MANDT,
		SPRAS,
		FAKSP,
		VTEXT,
		META_UPD_DT
	FROM source
)
SELECT * FROM renamed