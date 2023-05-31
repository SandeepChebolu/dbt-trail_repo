WITH source AS (
	SELECT * FROM {{ source('SAP_ECC','TBRCT') }}
),
renamed AS (
	SELECT
		MANDT,
		SPRAS,
		BRACO,
		VTEXT,
		META_UPD_DT
	FROM source
)
SELECT * FROM renamed