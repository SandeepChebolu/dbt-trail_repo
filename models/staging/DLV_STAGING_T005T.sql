WITH source AS (
	SELECT * FROM {{ source('SAP_ECC','T005T') }}
),
renamed AS (
	SELECT
		MANDT,
		SPRAS,
		LAND1,
		LANDX,
		NATIO,
		LANDX50,
		NATIO50,
		PRQ_SPREGT,
		META_UPD_DT
	FROM source
)
SELECT * FROM renamed