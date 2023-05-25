WITH source AS (
	SELECT * FROM {{ source('SAP_ECC','T077X') }}
),
renamed AS (
	SELECT
		MANDT,
		SPRAS,
		KTOKD,
		TXT30,
		META_UPD_DT
	FROM source
)
SELECT * FROM renamed