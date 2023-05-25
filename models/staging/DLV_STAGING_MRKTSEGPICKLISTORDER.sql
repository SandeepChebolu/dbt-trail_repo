WITH source AS (
	SELECT * FROM {{ source('REP_ILMN_INFC_SFDC','MRKTSEGPICKLISTORDER') }}
),
renamed AS (
	SELECT
		META_CRT_DT,
		META_UPD_DT,
		META_IUD_FLG,
		META_SRC_NM,
		MARKETSEGMENTNAME,
		PRECEDENCE
	FROM source
)
SELECT * FROM renamed