WITH source AS (
	SELECT * FROM {{ source('SAP_ECC','TKUKT') }}
),
renamed AS (
	SELECT
		MANDT,
		SPRAS,
		KUKLA,
		VTEXT,
		META_UPD_DT
	FROM source
)
SELECT * FROM renamed