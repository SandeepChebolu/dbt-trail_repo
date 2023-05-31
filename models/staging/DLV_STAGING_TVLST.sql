WITH source AS (
	SELECT * FROM {{ source('SAP_ECC','TVLST') }}
),
renamed AS (
	SELECT
		MANDT,
		SPRAS,
		LIFSP,
		VTEXT,
		META_UPD_DT
	FROM source
)
SELECT * FROM renamed