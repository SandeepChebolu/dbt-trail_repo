WITH source AS (
	SELECT * FROM {{ source('SAP_ECC','T005F') }}
),
renamed AS (
	SELECT
		MANDT,
		SPRAS,
		LAND1,
		REGIO,
		COUNC,
		BEZEI,
		META_UPD_DT
	FROM source
)
SELECT * FROM renamed