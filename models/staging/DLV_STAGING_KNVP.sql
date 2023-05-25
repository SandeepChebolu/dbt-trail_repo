WITH source AS (
	SELECT * FROM {{ source('SAP_ECC','KNVP') }}
),
renamed AS (
	SELECT
		MANDT,
		KUNNR,
		VKORG,
		VTWEG,
		SPART,
		PARVW,
		PARZA,
		KUNN2,
		LIFNR,
		PERNR,
		PARNR,
		KNREF,
		DEFPA,
		META_UPD_DT
	FROM source
)
SELECT * FROM renamed