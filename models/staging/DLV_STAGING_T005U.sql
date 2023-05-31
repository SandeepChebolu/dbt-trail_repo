WITH source AS (
	SELECT * FROM {{ source('SAP_ECC','T005U') }}
),
renamed AS (
	SELECT
		MANDT,
		SPRAS,
		LAND1,
		BLAND,
		BEZEI,
		META_UPD_DT
	FROM source
)
SELECT * FROM renamed