WITH source AS (
	SELECT * FROM {{ source('SAP_ECC','T005H') }}
),
renamed AS (
	SELECT
		MANDT,
		SPRAS,
		LAND1,
		REGIO,
		CITYC,
		BEZEI,
		META_UPD_DT
	FROM source
)
SELECT * FROM renamed